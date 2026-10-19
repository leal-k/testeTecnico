package app

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/event"
	"github.com/leal-k/testeTecnico/internal/domain/ledger"
	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

var (
	// ErrForbidden: a identidade autenticada não pode executar a operação.
	ErrForbidden = errors.New("app: operation not allowed for this caller")
	// ErrWalletAlreadyExists: já existe carteira para o jogador nessa moeda.
	ErrWalletAlreadyExists = errors.New("app: wallet already exists for this player and currency")
	// ErrTransactionNotFound: a transação não existe ou não pertence ao chamador.
	ErrTransactionNotFound = errors.New("app: transaction not found")
	// ErrInvalidCursor: cursor de paginação malformado ou de outra carteira.
	ErrInvalidCursor = errors.New("app: invalid cursor")
)

const (
	DefaultLedgerLimit = 50
	MaxLedgerLimit     = 200
	cursorPrefix       = "ledger:v1:"
)

// Caller é a identidade autenticada que faz a chamada, já extraída do token.
// Internal identifica o serviço interno; ProviderID, um provedor de jogos.
type Caller struct {
	ProviderID string
	Internal   bool
}

// canSee informa se o chamador pode ver uma transação do provedor informado.
// Provedores só veem as próprias; o serviço interno vê todas.
func (c Caller) canSee(providerID string) bool {
	if c.Internal {
		return true
	}
	return c.ProviderID != "" && c.ProviderID == providerID
}

// LedgerRow é um lançamento com sua posição estável no ledger (seq).
type LedgerRow struct {
	Seq   int64
	Entry ledger.Entry
}

// ReconciliationData é o que o banco devolve, lido em uma única consulta (uma
// única fotografia consistente dos dados).
type ReconciliationData struct {
	Stored     money.Money
	Calculated money.Money
	Entries    int
}

// Queries são as leituras fora de transação de escrita.
type Queries interface {
	GetWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	ListLedger(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]LedgerRow, error)
	GetTransaction(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	GetTransactionByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	Reconciliation(ctx context.Context, walletID uuid.UUID) (ReconciliationData, error)
}

// WalletService abre carteiras e expõe as leituras de carteira e ledger. Todas
// as operações são restritas ao serviço interno.
type WalletService struct {
	tm           TxManager
	q            Queries
	now          func() time.Time
	newID        func() uuid.UUID
	onDivergence func(Reconciliation)
}

func NewWalletService(tm TxManager, q Queries, now func() time.Time, newID func() uuid.UUID) *WalletService {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	if newID == nil {
		newID = func() uuid.UUID {
			id, err := uuid.NewV7()
			if err != nil {
				return uuid.New()
			}
			return id
		}
	}
	return &WalletService{tm: tm, q: q, now: now, newID: newID}
}

// OnDivergence registra uma função chamada sempre que uma reconciliação
// encontra diferença (usada para log e métrica).
func (s *WalletService) OnDivergence(fn func(Reconciliation)) {
	s.onDivergence = fn
}

type OpenWalletCommand struct {
	PlayerID       uuid.UUID
	InitialBalance money.Money
	CorrelationID  string
}

// Open cria a carteira. Com saldo inicial positivo, grava no MESMO commit a
// transação OPENING (já PROCESSED), o lançamento de crédito e os eventos
// WalletBalanceChanged e WagerTransactionProcessed. Com saldo zero, cria só a
// carteira.
func (s *WalletService) Open(ctx context.Context, caller Caller, cmd OpenWalletCommand) (*wallet.Wallet, error) {
	if !caller.Internal {
		return nil, ErrForbidden
	}
	now := s.now()
	w, mv, err := wallet.Open(s.newID(), cmd.PlayerID, cmd.InitialBalance, now)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}

	correlationID := cmd.CorrelationID
	if correlationID == "" {
		correlationID = w.ID().String()
	}
	meta := func() event.Meta {
		return event.Meta{ID: s.newID(), CorrelationID: correlationID, OccurredAt: now}
	}

	err = s.tm.WithinTx(ctx, func(ctx context.Context, st Store) error {
		inserted, err := st.InsertWallet(ctx, w)
		if err != nil {
			return err
		}
		if !inserted {
			return ErrWalletAlreadyExists
		}
		if mv == nil {
			return nil // saldo inicial zero
		}

		opening, err := wager.NewOpening(s.newID(), w.ID(), w.PlayerID(), cmd.InitialBalance, now)
		if err != nil {
			return err
		}
		ok, err := st.InsertTransaction(ctx, opening)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("app: opening transaction already exists for this wallet")
		}

		entry, err := ledger.New(s.newID(), w.ID(), opening.ID(), *mv, now)
		if err != nil {
			return err
		}
		if err := st.InsertLedgerEntry(ctx, entry); err != nil {
			return err
		}

		changed, err := event.WalletBalanceChanged(w.ID(), opening.ID(), *mv, w.Version(), meta())
		if err != nil {
			return err
		}
		processed, err := event.TransactionProcessed(opening.Snapshot(), meta())
		if err != nil {
			return err
		}
		if err := st.AddOutboxEvent(ctx, changed); err != nil {
			return err
		}
		return st.AddOutboxEvent(ctx, processed)
	})
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (s *WalletService) Get(ctx context.Context, caller Caller, id uuid.UUID) (*wallet.Wallet, error) {
	if !caller.Internal {
		return nil, ErrForbidden
	}
	w, err := s.q.GetWallet(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return nil, ErrWalletNotFound
	}
	return w, err
}

// LedgerPage é uma página do ledger. NextCursor vazio indica a última página.
type LedgerPage struct {
	Entries    []ledger.Entry
	NextCursor string
}

// Ledger lista os lançamentos em ordem de gravação (estável), paginando por
// cursor opaco. limit 0 usa o padrão; o máximo é MaxLedgerLimit.
func (s *WalletService) Ledger(ctx context.Context, caller Caller, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	if !caller.Internal {
		return LedgerPage{}, ErrForbidden
	}
	if limit == 0 {
		limit = DefaultLedgerLimit
	}
	if limit < 1 || limit > MaxLedgerLimit {
		return LedgerPage{}, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidCommand, MaxLedgerLimit)
	}
	afterSeq, err := decodeCursor(cursor, walletID)
	if err != nil {
		return LedgerPage{}, err
	}
	if _, err := s.Get(ctx, caller, walletID); err != nil {
		return LedgerPage{}, err
	}

	// pede um a mais para saber se existe próxima página
	rows, err := s.q.ListLedger(ctx, walletID, afterSeq, limit+1)
	if err != nil {
		return LedgerPage{}, err
	}

	page := LedgerPage{}
	if len(rows) > limit {
		rows = rows[:limit]
		page.NextCursor = encodeCursor(walletID, rows[len(rows)-1].Seq)
	}
	page.Entries = make([]ledger.Entry, len(rows))
	for i, r := range rows {
		page.Entries[i] = r.Entry
	}
	return page, nil
}

// Reconciliation compara o saldo armazenado com o saldo reconstruído pelo
// ledger (incluindo a abertura). Não altera nada.
type Reconciliation struct {
	WalletID       uuid.UUID
	Stored         money.Money
	Calculated     money.Money
	Difference     money.Money // armazenado - reconstruído
	Consistent     bool
	CheckedEntries int
}

func (s *WalletService) Reconcile(ctx context.Context, caller Caller, walletID uuid.UUID) (Reconciliation, error) {
	if !caller.Internal {
		return Reconciliation{}, ErrForbidden
	}
	data, err := s.q.Reconciliation(ctx, walletID)
	if errors.Is(err, ErrNotFound) {
		return Reconciliation{}, ErrWalletNotFound
	}
	if err != nil {
		return Reconciliation{}, err
	}
	diff, err := data.Stored.Sub(data.Calculated)
	if err != nil {
		return Reconciliation{}, err
	}

	r := Reconciliation{
		WalletID:       walletID,
		Stored:         data.Stored,
		Calculated:     data.Calculated,
		Difference:     diff,
		Consistent:     diff.IsZero(),
		CheckedEntries: data.Entries,
	}
	if !r.Consistent && s.onDivergence != nil {
		s.onDivergence(r)
	}
	return r, nil
}

// TransactionReader responde às consultas de transação, aplicando o
// isolamento entre provedores.
type TransactionReader struct {
	q Queries
}

func NewTransactionReader(q Queries) *TransactionReader {
	return &TransactionReader{q: q}
}

// ByID devolve a transação. Para um provedor, uma transação de outro provedor
// (ou interna) responde como inexistente, sem revelar que ela existe.
func (r *TransactionReader) ByID(ctx context.Context, caller Caller, id uuid.UUID) (*wager.Transaction, error) {
	tx, err := r.q.GetTransaction(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return nil, ErrTransactionNotFound
	}
	if err != nil {
		return nil, err
	}
	if !caller.canSee(tx.Snapshot().ProviderID) {
		return nil, ErrTransactionNotFound
	}
	return tx, nil
}

// ByExternalID busca por (provedor, externalTransactionId). Um provedor só pode
// consultar o próprio providerId; a recusa acontece antes de qualquer leitura.
func (r *TransactionReader) ByExternalID(ctx context.Context, caller Caller, providerID, externalID string) (*wager.Transaction, error) {
	if !caller.canSee(providerID) {
		return nil, ErrForbidden
	}
	tx, err := r.q.GetTransactionByExternalID(ctx, providerID, externalID)
	if errors.Is(err, ErrNotFound) {
		return nil, ErrTransactionNotFound
	}
	return tx, err
}

// O cursor é opaco para o cliente: base64url de "ledger:v1:<walletId>:<seq>".
// Carregar o walletId impede usar o cursor de uma carteira em outra.

func encodeCursor(walletID uuid.UUID, seq int64) string {
	raw := cursorPrefix + walletID.String() + ":" + strconv.FormatInt(seq, 10)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(cursor string, walletID uuid.UUID) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, ErrInvalidCursor
	}
	rest, ok := strings.CutPrefix(string(raw), cursorPrefix)
	if !ok {
		return 0, ErrInvalidCursor
	}
	id, seqText, ok := strings.Cut(rest, ":")
	if !ok || id != walletID.String() {
		return 0, ErrInvalidCursor
	}
	seq, err := strconv.ParseInt(seqText, 10, 64)
	if err != nil || seq < 1 {
		return 0, ErrInvalidCursor
	}
	return seq, nil
}
