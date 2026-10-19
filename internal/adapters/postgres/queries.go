package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/ledger"
	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
	"github.com/leal-k/testeTecnico/internal/domain/wallet"
)

var _ app.Queries = (*Queries)(nil)

// Queries implementa as leituras usadas pela API (fora das transações de
// escrita). Cada método é uma única consulta, logo uma fotografia consistente.
type Queries struct {
	pool *pgxpool.Pool
}

func NewQueries(pool *pgxpool.Pool) *Queries {
	return &Queries{pool: pool}
}

func (q *Queries) GetWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	var (
		playerID             uuid.UUID
		currency             string
		balanceMinor         int64
		version              int64
		createdAt, updatedAt time.Time
	)
	err := q.pool.QueryRow(ctx, `
		SELECT player_id, currency, balance_minor, version, created_at, updated_at
		FROM wallets WHERE id = $1`, id).
		Scan(&playerID, &currency, &balanceMinor, &version, &createdAt, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: reading wallet: %w", err)
	}
	balance, err := minorToMoney(balanceMinor, currency)
	if err != nil {
		return nil, err
	}
	return wallet.Rehydrate(id, playerID, balance, version, createdAt.UTC(), updatedAt.UTC())
}

// ListLedger lista os lançamentos com seq maior que afterSeq, em ordem de seq
// (crescente e estável, gerada pelo banco).
func (q *Queries) ListLedger(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]app.LedgerRow, error) {
	rows, err := q.pool.Query(ctx, `
		SELECT seq, id, transaction_id, direction, amount_minor, currency,
		       balance_before_minor, balance_after_minor, created_at
		FROM wallet_ledger_entries
		WHERE wallet_id = $1 AND seq > $2
		ORDER BY seq
		LIMIT $3`, walletID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: listing ledger: %w", err)
	}
	defer rows.Close()

	var out []app.LedgerRow
	for rows.Next() {
		var (
			seq, amount, before, after int64
			id, txID                   uuid.UUID
			direction, currency        string
			createdAt                  time.Time
		)
		if err := rows.Scan(&seq, &id, &txID, &direction, &amount, &currency, &before, &after, &createdAt); err != nil {
			return nil, fmt.Errorf("postgres: scanning ledger entry: %w", err)
		}
		amountM, err := minorToMoney(amount, currency)
		if err != nil {
			return nil, err
		}
		beforeM, err := minorToMoney(before, currency)
		if err != nil {
			return nil, err
		}
		afterM, err := minorToMoney(after, currency)
		if err != nil {
			return nil, err
		}
		entry, err := ledger.Rehydrate(id, walletID, txID, wallet.Direction(direction), amountM, beforeM, afterM, createdAt.UTC())
		if err != nil {
			return nil, err
		}
		out = append(out, app.LedgerRow{Seq: seq, Entry: entry})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: listing ledger: %w", err)
	}
	return out, nil
}

func (q *Queries) GetTransaction(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	return scanTransaction(q.pool.QueryRow(ctx,
		`SELECT `+transactionColumns+` FROM wager_transactions WHERE id = $1`, id))
}

func (q *Queries) GetTransactionByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return scanTransaction(q.pool.QueryRow(ctx,
		`SELECT `+transactionColumns+` FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`,
		providerID, externalID))
}

// Reconciliation lê o saldo armazenado e reconstrói o saldo pelo ledger em UMA
// consulta: o PostgreSQL garante que ela enxerga um único snapshot dos dados,
// então uma operação gravada no meio não produz falsa divergência.
func (q *Queries) Reconciliation(ctx context.Context, walletID uuid.UUID) (app.ReconciliationData, error) {
	var (
		currency          string
		stored, net, rows int64
	)
	err := q.pool.QueryRow(ctx, `
		SELECT w.currency,
		       w.balance_minor,
		       COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount_minor ELSE -l.amount_minor END), 0)::bigint,
		       COUNT(l.id)
		FROM wallets w
		LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
		WHERE w.id = $1
		GROUP BY w.currency, w.balance_minor`, walletID).Scan(&currency, &stored, &net, &rows)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.ReconciliationData{}, app.ErrNotFound
	}
	if err != nil {
		return app.ReconciliationData{}, fmt.Errorf("postgres: reconciling wallet: %w", err)
	}
	storedM, err := minorToMoney(stored, currency)
	if err != nil {
		return app.ReconciliationData{}, err
	}
	calcM, err := minorToMoney(net, currency)
	if err != nil {
		return app.ReconciliationData{}, err
	}
	return app.ReconciliationData{Stored: storedM, Calculated: calcM, Entries: int(rows)}, nil
}

func minorToMoney(minor int64, currency string) (money.Money, error) {
	cur, err := money.NewCurrency(currency)
	if err != nil {
		return money.Money{}, err
	}
	return money.FromMinor(minor, cur)
}
