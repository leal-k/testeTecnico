package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/event"
	"github.com/leal-k/testeTecnico/internal/domain/ledger"
	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

var (
	ErrWalletNotFound       = errors.New("app: wallet not found")
	ErrWalletPlayerMismatch = errors.New("app: wallet belongs to another player")
	ErrCurrencyMismatch     = errors.New("app: amount currency differs from the wallet currency")
	ErrInvalidCommand       = errors.New("app: invalid command")
	// Mesma chave de idempotência com conteúdo diferente.
	ErrIdempotencyConflict = errors.New("app: idempotency key reused with a different payload")
	// Mesmo (provedor, externalId) com outra chave de idempotência.
	ErrExternalIDConflict = errors.New("app: external transaction id already used with another idempotency key")
	// Mesmo messageId de entrada com conteúdo diferente do já tratado.
	ErrInboxConflict = errors.New("app: message id reused with a different payload")
)

const maxConcurrentRetries = 3

// Config ajusta a espera por referências que ainda não chegaram.
type Config struct {
	// ReferenceTTL é o tempo máximo que uma transação espera pela referência.
	ReferenceTTL time.Duration
	// BackoffBase e BackoffMax definem o backoff exponencial entre tentativas.
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

func DefaultConfig() Config {
	return Config{
		ReferenceTTL: 5 * time.Minute,
		BackoffBase:  time.Second,
		BackoffMax:   time.Minute,
	}
}

// ProcessCommand é uma operação financeira recebida de um provedor (por HTTP
// ou SQS). O ID do provedor vem do token autenticado, nunca do corpo.
type ProcessCommand struct {
	ProviderID          string
	IdempotencyKey      string
	ExternalID          string
	Kind                wager.Kind
	WalletID            uuid.UUID
	PlayerID            uuid.UUID
	RoundID             string
	GameID              string
	Amount              money.Money
	ReferenceExternalID string
	CorrelationID       string
	CausationID         string
	// Inbox, quando presente (entrada por SQS), é gravada na MESMA transação
	// da operação: a mensagem só conta como tratada se a operação também
	// foi confirmada.
	Inbox *InboxRecord
}

// Result é o resultado persistido da operação.
type Result struct {
	TransactionID uuid.UUID
	Status        wager.Status
	// Balance é o saldo observado quando a operação foi concluída.
	Balance     money.Money
	FailureCode wager.FailureCode
	// Replay indica que a resposta veio de uma execução anterior da mesma
	// operação, sem reaplicar nada.
	Replay bool
	// DuplicateMessage indica que a mesma mensagem de entrada (messageId) já
	// tinha sido tratada antes.
	DuplicateMessage bool
}

// Processor aplica operações financeiras com idempotência persistente.
type Processor struct {
	tm         TxManager
	cfg        Config
	now        func() time.Time
	newID      func() uuid.UUID
	onConflict func()
}

// NewProcessor cria o caso de uso. now e newID podem ser nil (usa o relógio
// UTC e UUID v7); os testes injetam versões determinísticas.
func NewProcessor(tm TxManager, cfg Config, now func() time.Time, newID func() uuid.UUID) *Processor {
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
	return &Processor{tm: tm, cfg: cfg, now: now, newID: newID, onConflict: func() {}}
}

// OnConcurrencyConflict registra uma função chamada sempre que duas
// requisições disputam a mesma operação e esta precisa ser refeita (métrica de
// conflitos de concorrência).
func (p *Processor) OnConcurrencyConflict(fn func()) {
	p.onConflict = fn
}

// Process executa a operação em UMA transação de banco: trava a carteira,
// confere a idempotência, aplica as regras do domínio e grava transação,
// saldo, ledger e eventos da outbox juntos.
//
// Uma recusa de negócio NÃO é erro: a transação é gravada como REJECTED e o
// resultado volta normalmente. Só erros de validação, conflitos de
// idempotência e falhas técnicas retornam error.
func (p *Processor) Process(ctx context.Context, cmd ProcessCommand) (Result, error) {
	for attempt := 0; ; attempt++ {
		var result Result
		err := p.tm.WithinTx(ctx, func(ctx context.Context, st Store) error {
			r, err := p.process(ctx, st, cmd)
			if err != nil {
				return err
			}
			result = r
			return nil
		})
		if errors.Is(err, ErrConcurrentInsert) || errors.Is(err, ErrVersionConflict) {
			p.onConflict()
		}
		if errors.Is(err, ErrConcurrentInsert) && attempt < maxConcurrentRetries {
			continue
		}
		if err != nil {
			return Result{}, err
		}
		return result, nil
	}
}

func (p *Processor) process(ctx context.Context, st Store, cmd ProcessCommand) (Result, error) {
	now := p.now()

	// 0. Inbox (só na entrada por SQS). Fica antes do lock da carteira: com
	// essa ordem fixa, duas entregas da mesma mensagem em instâncias
	// diferentes nunca se travam mutuamente (a segunda espera a primeira no
	// INSERT da inbox e depois enxerga o registro dela).
	duplicate := false
	if cmd.Inbox != nil {
		inserted, storedHash, err := st.RecordInbox(ctx, *cmd.Inbox, now)
		if err != nil {
			return Result{}, err
		}
		if !inserted {
			if storedHash != cmd.Inbox.PayloadHash {
				return Result{}, ErrInboxConflict
			}
			// Mesma mensagem de novo: segue normalmente, e a idempotência
			// abaixo devolve o resultado gravado, sem reaplicar nada.
			duplicate = true
		}
	}

	res, err := p.apply(ctx, st, cmd, now)
	if err != nil {
		return Result{}, err
	}
	res.DuplicateMessage = duplicate
	return res, nil
}

func (p *Processor) apply(ctx context.Context, st Store, cmd ProcessCommand, now time.Time) (Result, error) {
	// 1. Trava a carteira ANTES de qualquer INSERT que a referencie. Assim duas
	// operações da mesma carteira nunca se cruzam (e não há deadlock entre a
	// chave estrangeira e o FOR UPDATE).
	w, err := st.LockWallet(ctx, cmd.WalletID)
	if errors.Is(err, ErrNotFound) {
		return Result{}, ErrWalletNotFound
	}
	if err != nil {
		return Result{}, err
	}
	if w.PlayerID() != cmd.PlayerID {
		return Result{}, ErrWalletPlayerMismatch
	}
	if cmd.Amount.IsInitialized() && cmd.Amount.Currency() != w.Currency() {
		return Result{}, ErrCurrencyMismatch
	}

	tx, err := wager.NewExternal(wager.ExternalParams{
		ID:                  p.newID(),
		Kind:                cmd.Kind,
		ProviderID:          cmd.ProviderID,
		ExternalID:          cmd.ExternalID,
		IdempotencyKey:      cmd.IdempotencyKey,
		WalletID:            cmd.WalletID,
		PlayerID:            cmd.PlayerID,
		RoundID:             cmd.RoundID,
		GameID:              cmd.GameID,
		Amount:              cmd.Amount,
		ReferenceExternalID: cmd.ReferenceExternalID,
	}, now)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}

	// 2. Idempotência persistente: a mesma chave devolve o resultado gravado.
	existing, err := st.FindTransactionByKey(ctx, cmd.ProviderID, cmd.IdempotencyKey)
	switch {
	case err == nil:
		s := existing.Snapshot()
		if s.PayloadHash != tx.Snapshot().PayloadHash {
			return Result{}, ErrIdempotencyConflict
		}
		return resultOf(s, true), nil
	case !errors.Is(err, ErrNotFound):
		return Result{}, err
	}

	// 3. O mesmo externalId com outra chave seria uma segunda aplicação da
	// mesma operação financeira.
	_, err = st.FindTransactionByExternalID(ctx, cmd.ProviderID, cmd.ExternalID)
	switch {
	case err == nil:
		return Result{}, ErrExternalIDConflict
	case !errors.Is(err, ErrNotFound):
		return Result{}, err
	}

	// 4. Regras de negócio.
	ref, err := p.resolve(ctx, st, tx)
	if err != nil {
		return Result{}, err
	}
	expectedVersion := w.Version()
	mv, applyErr := wager.Apply(tx, w, ref, now)

	return p.conclude(ctx, st, outcome{
		tx:              tx,
		w:               w,
		ref:             ref,
		mv:              mv,
		applyErr:        applyErr,
		expectedVersion: expectedVersion,
		correlationID:   cmd.CorrelationID,
		causationID:     cmd.CausationID,
		create:          true,
		now:             now,
	})
}

// RetryPending reavalia uma transação em PENDING_REFERENCE. Se a referência
// já chegou, conclui a operação; se o prazo acabou, rejeita; senão agenda a
// próxima tentativa. É seguro chamar várias vezes e por várias instâncias.
func (p *Processor) RetryPending(ctx context.Context, id uuid.UUID) error {
	return p.tm.WithinTx(ctx, func(ctx context.Context, st Store) error {
		return p.retry(ctx, st, id)
	})
}

func (p *Processor) retry(ctx context.Context, st Store, id uuid.UUID) error {
	now := p.now()

	probe, err := st.GetTransaction(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	// Com o lock da carteira, o estado lido a seguir é definitivo: nenhuma outra
	// operação da mesma carteira roda em paralelo.
	w, err := st.LockWallet(ctx, probe.Snapshot().WalletID)
	if err != nil {
		return err
	}
	tx, err := st.GetTransaction(ctx, id)
	if err != nil {
		return err
	}
	if tx.Status() != wager.StatusPendingReference {
		return nil // outra instância já concluiu
	}

	ref, err := p.resolve(ctx, st, tx)
	if err != nil {
		return err
	}
	expectedVersion := w.Version()
	mv, applyErr := wager.Apply(tx, w, ref, now)

	_, err = p.conclude(ctx, st, outcome{
		tx:              tx,
		w:               w,
		ref:             ref,
		mv:              mv,
		applyErr:        applyErr,
		expectedVersion: expectedVersion,
		correlationID:   tx.ID().String(),
		create:          false,
		now:             now,
	})
	return err
}

// outcome reúne o que é preciso para concluir uma transação depois de Apply.
type outcome struct {
	tx              *wager.Transaction
	w               *wallet.Wallet
	ref             *wager.Reference
	mv              *wallet.Movement
	applyErr        error
	expectedVersion int64
	correlationID   string
	causationID     string
	create          bool
	now             time.Time
}

func (p *Processor) conclude(ctx context.Context, st Store, o outcome) (Result, error) {
	var rej *wager.Rejection
	switch {
	case o.applyErr == nil:
		return p.succeed(ctx, st, o)
	case errors.As(o.applyErr, &rej):
		return p.reject(ctx, st, o, rej.Code)
	case errors.Is(o.applyErr, wager.ErrReferenceNotFound), errors.Is(o.applyErr, wager.ErrReferencePending):
		return p.wait(ctx, st, o)
	default:
		// falha técnica: desfaz tudo e deixa quem chamou decidir (retry)
		return Result{}, o.applyErr
	}
}

func (p *Processor) succeed(ctx context.Context, st Store, o outcome) (Result, error) {
	tx := o.tx
	if o.ref != nil {
		if err := tx.SetReference(o.ref.Tx.ID(), o.now); err != nil {
			return Result{}, err
		}
	}
	if err := tx.Process(o.w.Balance(), o.now); err != nil {
		return Result{}, err
	}
	// A transação é gravada primeiro: o ledger tem chave estrangeira para ela.
	if err := p.saveTransaction(ctx, st, tx, o.create); err != nil {
		return Result{}, err
	}

	if o.mv != nil {
		if err := st.SaveWallet(ctx, o.w, o.expectedVersion); err != nil {
			return Result{}, err
		}
		entry, err := ledger.New(p.newID(), o.w.ID(), tx.ID(), *o.mv, o.now)
		if err != nil {
			return Result{}, err
		}
		if err := st.InsertLedgerEntry(ctx, entry); err != nil {
			return Result{}, err
		}
		changed, err := event.WalletBalanceChanged(o.w.ID(), tx.ID(), *o.mv, o.w.Version(), p.meta(o))
		if err != nil {
			return Result{}, err
		}
		if err := st.AddOutboxEvent(ctx, changed); err != nil {
			return Result{}, err
		}
	}

	processed, err := event.TransactionProcessed(tx.Snapshot(), p.meta(o))
	if err != nil {
		return Result{}, err
	}
	if err := st.AddOutboxEvent(ctx, processed); err != nil {
		return Result{}, err
	}
	return resultOf(tx.Snapshot(), false), nil
}

func (p *Processor) reject(ctx context.Context, st Store, o outcome, code wager.FailureCode) (Result, error) {
	tx := o.tx
	if err := tx.Reject(code, o.w.Balance(), o.now); err != nil {
		return Result{}, err
	}
	if err := p.saveTransaction(ctx, st, tx, o.create); err != nil {
		return Result{}, err
	}
	rejected, err := event.TransactionRejected(tx.Snapshot(), p.meta(o))
	if err != nil {
		return Result{}, err
	}
	if err := st.AddOutboxEvent(ctx, rejected); err != nil {
		return Result{}, err
	}
	return resultOf(tx.Snapshot(), false), nil
}

// wait trata a referência indisponível: agenda nova tentativa com backoff ou,
// vencido o prazo, rejeita a transação.
func (p *Processor) wait(ctx context.Context, st Store, o outcome) (Result, error) {
	tx := o.tx
	s := tx.Snapshot()

	deadline := s.CreatedAt.Add(p.cfg.ReferenceTTL)
	if !o.now.Before(deadline) {
		code := wager.FailureReferenceNotFound
		if !errors.Is(o.applyErr, wager.ErrReferenceNotFound) {
			// a referência existe, mas não terminou a tempo
			code = wager.FailureReferenceNotProcessed
		}
		return p.reject(ctx, st, o, code)
	}

	next := o.now.Add(p.backoff(s.Attempts))
	if next.After(deadline) {
		next = deadline // garante uma última passagem exatamente no prazo
	}
	if err := tx.MarkPendingReference(next, o.now); err != nil {
		return Result{}, err
	}
	if err := p.saveTransaction(ctx, st, tx, o.create); err != nil {
		return Result{}, err
	}
	pending, err := event.TransactionPendingReference(tx.Snapshot(), p.meta(o))
	if err != nil {
		return Result{}, err
	}
	if err := st.AddOutboxEvent(ctx, pending); err != nil {
		return Result{}, err
	}

	res := resultOf(tx.Snapshot(), false)
	res.Balance = o.w.Balance()
	return res, nil
}

func (p *Processor) saveTransaction(ctx context.Context, st Store, tx *wager.Transaction, create bool) error {
	if !create {
		return st.UpdateTransaction(ctx, tx)
	}
	inserted, err := st.InsertTransaction(ctx, tx)
	if err != nil {
		return err
	}
	if !inserted {
		return ErrConcurrentInsert
	}
	return nil
}

func (p *Processor) resolve(ctx context.Context, st Store, tx *wager.Transaction) (*wager.Reference, error) {
	if tx.ReferenceExternalID() == "" {
		return nil, nil
	}
	return st.ResolveReference(ctx, tx.Snapshot().ProviderID, tx.ReferenceExternalID())
}

func (p *Processor) meta(o outcome) event.Meta {
	return event.Meta{
		ID:            p.newID(),
		CorrelationID: o.correlationID,
		CausationID:   o.causationID,
		OccurredAt:    o.now,
	}
}

// backoff devolve a espera antes da tentativa seguinte: dobra a cada tentativa
// a partir de BackoffBase, limitada a BackoffMax.
func (p *Processor) backoff(attempts int) time.Duration {
	d := p.cfg.BackoffBase
	for i := 0; i < attempts && d < p.cfg.BackoffMax; i++ {
		d *= 2
	}
	if d > p.cfg.BackoffMax {
		d = p.cfg.BackoffMax
	}
	return d
}

func resultOf(s wager.Snapshot, replay bool) Result {
	return Result{
		TransactionID: s.ID,
		Status:        s.Status,
		Balance:       s.ResultBalance,
		FailureCode:   s.FailureCode,
		Replay:        replay,
	}
}
