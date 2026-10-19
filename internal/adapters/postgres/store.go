package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/event"
	"github.com/leal-k/testeTecnico/internal/domain/ledger"
	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

// store opera dentro de uma única transação de banco.
type store struct {
	tx pgx.Tx
}

const transactionColumns = `
	id, kind, status, provider_id, external_transaction_id, idempotency_key,
	payload_hash, round_id, game_id, reference_external_transaction_id,
	wallet_id, player_id, currency, amount_minor, reference_transaction_id,
	failure_code, result_balance_minor, attempts, next_attempt_at,
	created_at, updated_at`

// LockWallet lê a carteira e trava a linha (FOR UPDATE) até o fim da
// transação: operações da mesma carteira esperam em fila; as demais seguem.
func (s *store) LockWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	var (
		playerID     uuid.UUID
		currency     string
		balanceMinor int64
		version      int64
		createdAt    time.Time
		updatedAt    time.Time
	)
	err := s.tx.QueryRow(ctx, `
		SELECT player_id, currency, balance_minor, version, created_at, updated_at
		FROM wallets
		WHERE id = $1
		FOR UPDATE`, id).Scan(&playerID, &currency, &balanceMinor, &version, &createdAt, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: locking wallet: %w", err)
	}

	cur, err := money.NewCurrency(currency)
	if err != nil {
		return nil, err
	}
	balance, err := money.FromMinor(balanceMinor, cur)
	if err != nil {
		return nil, err
	}
	return wallet.Rehydrate(id, playerID, balance, version, createdAt.UTC(), updatedAt.UTC())
}

// SaveWallet grava saldo e versão. A cláusula "version = $4" confere que a
// carteira não mudou desde a leitura; o trigger do banco exige version + 1.
func (s *store) SaveWallet(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error {
	tag, err := s.tx.Exec(ctx, `
		UPDATE wallets
		SET balance_minor = $2, version = $3
		WHERE id = $1 AND version = $4`,
		w.ID(), w.Balance().Minor(), w.Version(), expectedVersion)
	if err != nil {
		return fmt.Errorf("postgres: saving wallet: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrVersionConflict
	}
	return nil
}

func (s *store) FindTransactionByKey(ctx context.Context, providerID, idempotencyKey string) (*wager.Transaction, error) {
	return scanTransaction(s.tx.QueryRow(ctx,
		`SELECT `+transactionColumns+` FROM wager_transactions WHERE provider_id = $1 AND idempotency_key = $2`,
		providerID, idempotencyKey))
}

func (s *store) FindTransactionByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return scanTransaction(s.tx.QueryRow(ctx,
		`SELECT `+transactionColumns+` FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`,
		providerID, externalID))
}

func (s *store) GetTransaction(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	return scanTransaction(s.tx.QueryRow(ctx,
		`SELECT `+transactionColumns+` FROM wager_transactions WHERE id = $1`, id))
}

func (s *store) ResolveReference(ctx context.Context, providerID, referenceExternalID string) (*wager.Reference, error) {
	tx, err := s.FindTransactionByExternalID(ctx, providerID, referenceExternalID)
	if errors.Is(err, app.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var reversed bool
	err = s.tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM wager_transactions
			WHERE reference_transaction_id = $1
			  AND kind IN ('REFUND', 'ROLLBACK')
			  AND status = 'PROCESSED'
		)`, tx.ID()).Scan(&reversed)
	if err != nil {
		return nil, fmt.Errorf("postgres: checking reversals: %w", err)
	}
	return &wager.Reference{Tx: tx, AlreadyReversed: reversed}, nil
}

// InsertTransaction grava uma transação nova. ON CONFLICT DO NOTHING (sem
// alvo) cobre qualquer violação de unicidade: chave de idempotência, externalId,
// OPENING duplicado ou reversão duplicada. Se outra transação concorrente ainda
// não confirmou a linha conflitante, o INSERT espera o resultado dela.
func (s *store) InsertTransaction(ctx context.Context, tx *wager.Transaction) (bool, error) {
	t := tx.Snapshot()

	origin := "EXTERNAL"
	if t.Kind == wager.KindOpening {
		origin = "INTERNAL"
	}
	var resultMinor any
	if t.ResultBalance.IsInitialized() {
		resultMinor = t.ResultBalance.Minor()
	}

	tag, err := s.tx.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, kind, status, provider_id, external_transaction_id,
			idempotency_key, payload_hash, round_id, game_id,
			reference_external_transaction_id, wallet_id, player_id, currency,
			amount_minor, reference_transaction_id, failure_code,
			result_balance_minor, attempts, next_attempt_at, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
		)
		ON CONFLICT DO NOTHING`,
		t.ID, origin, string(t.Kind), string(t.Status),
		nullString(t.ProviderID), nullString(t.ExternalID),
		nullString(t.IdempotencyKey), nullString(t.PayloadHash),
		nullString(t.RoundID), nullString(t.GameID),
		nullString(t.ReferenceExternalID), t.WalletID, t.PlayerID,
		t.Amount.Currency().String(), t.Amount.Minor(),
		nullUUID(t.ReferenceID), nullString(string(t.FailureCode)),
		resultMinor, t.Attempts, nullTime(t.NextAttemptAt), t.CreatedAt, t.UpdatedAt)
	if err != nil {
		return false, fmt.Errorf("postgres: inserting transaction: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *store) UpdateTransaction(ctx context.Context, tx *wager.Transaction) error {
	t := tx.Snapshot()

	var resultMinor any
	if t.ResultBalance.IsInitialized() {
		resultMinor = t.ResultBalance.Minor()
	}

	tag, err := s.tx.Exec(ctx, `
		UPDATE wager_transactions
		SET status = $2,
		    reference_transaction_id = $3,
		    failure_code = $4,
		    result_balance_minor = $5,
		    attempts = $6,
		    next_attempt_at = $7
		WHERE id = $1`,
		t.ID, string(t.Status), nullUUID(t.ReferenceID),
		nullString(string(t.FailureCode)), resultMinor, t.Attempts,
		nullTime(t.NextAttemptAt))
	if err != nil {
		return fmt.Errorf("postgres: updating transaction: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrNotFound
	}
	return nil
}

func (s *store) InsertLedgerEntry(ctx context.Context, e ledger.Entry) error {
	_, err := s.tx.Exec(ctx, `
		INSERT INTO wallet_ledger_entries (
			id, wallet_id, transaction_id, direction, amount_minor, currency,
			balance_before_minor, balance_after_minor, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()),
		e.Amount().Minor(), e.Amount().Currency().String(),
		e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), e.CreatedAt())
	if err != nil {
		return fmt.Errorf("postgres: inserting ledger entry: %w", err)
	}
	return nil
}

func (s *store) AddOutboxEvent(ctx context.Context, e event.Event) error {
	_, err := s.tx.Exec(ctx, `
		INSERT INTO outbox_events (
			id, aggregate_type, aggregate_id, event_type, event_version,
			correlation_id, causation_id, payload, occurred_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		e.ID, e.AggregateType, e.AggregateID, string(e.Type), e.Version,
		e.CorrelationID, nullString(e.CausationID), e.Payload, e.OccurredAt)
	if err != nil {
		return fmt.Errorf("postgres: inserting outbox event: %w", err)
	}
	return nil
}

// scanTransaction lê uma linha de wager_transactions (colunas na ordem de
// transactionColumns) e a reidrata no domínio.
func scanTransaction(row pgx.Row) (*wager.Transaction, error) {
	var (
		s                      wager.Snapshot
		kind, status, currency string
		providerID, externalID *string
		idempotencyKey, hash   *string
		roundID, gameID        *string
		referenceExternalID    *string
		failureCode            *string
		amountMinor            int64
		referenceID            *uuid.UUID
		resultMinor            *int64
		nextAttemptAt          *time.Time
	)
	err := row.Scan(
		&s.ID, &kind, &status, &providerID, &externalID, &idempotencyKey,
		&hash, &roundID, &gameID, &referenceExternalID,
		&s.WalletID, &s.PlayerID, &currency, &amountMinor, &referenceID,
		&failureCode, &resultMinor, &s.Attempts, &nextAttemptAt,
		&s.CreatedAt, &s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: scanning transaction: %w", err)
	}

	cur, err := money.NewCurrency(currency)
	if err != nil {
		return nil, err
	}
	s.Amount, err = money.FromMinor(amountMinor, cur)
	if err != nil {
		return nil, err
	}
	if resultMinor != nil {
		s.ResultBalance, err = money.FromMinor(*resultMinor, cur)
		if err != nil {
			return nil, err
		}
	}

	s.Kind = wager.Kind(kind)
	s.Status = wager.Status(status)
	s.ProviderID = deref(providerID)
	s.ExternalID = deref(externalID)
	s.IdempotencyKey = deref(idempotencyKey)
	s.PayloadHash = deref(hash)
	s.RoundID = deref(roundID)
	s.GameID = deref(gameID)
	s.ReferenceExternalID = deref(referenceExternalID)
	s.FailureCode = wager.FailureCode(deref(failureCode))
	if referenceID != nil {
		s.ReferenceID = *referenceID
	}
	if nextAttemptAt != nil {
		s.NextAttemptAt = nextAttemptAt.UTC()
	}
	s.CreatedAt = s.CreatedAt.UTC()
	s.UpdatedAt = s.UpdatedAt.UTC()

	return wager.Rehydrate(s)
}

// Os campos opcionais do domínio usam o valor zero; no banco viram NULL.

func nullString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func nullUUID(v uuid.UUID) any {
	if v == uuid.Nil {
		return nil
	}
	return v
}

func nullTime(v time.Time) any {
	if v.IsZero() {
		return nil
	}
	return v
}

func deref(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
