// Package postgres implementa as portas de persistência do pacote app sobre
// PostgreSQL, usando pgx e SQL explícito.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/leal-k/testeTecnico/internal/app"
)

var (
	_ app.TxManager     = (*DB)(nil)
	_ app.PendingLister = (*DB)(nil)
	_ app.Store         = (*store)(nil)
)

// DB gerencia as transações sobre um pool de conexões.
type DB struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *DB {
	return &DB{pool: pool}
}

// WithinTx executa fn em uma transação READ COMMITTED. Confirma (commit) se fn
// retornar nil e desfaz tudo (rollback) em qualquer erro ou pânico.
func (d *DB) WithinTx(ctx context.Context, fn func(ctx context.Context, s app.Store) error) error {
	tx, err := d.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("postgres: begin transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			// o contexto pode já estar cancelado; o rollback precisa acontecer mesmo assim
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()

	if err := fn(ctx, &store{tx: tx}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit: %w", err)
	}
	committed = true
	return nil
}

// ListDuePending devolve as transações em PENDING_REFERENCE cuja próxima
// tentativa já venceu, as mais antigas primeiro.
func (d *DB) ListDuePending(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT id
		FROM wager_transactions
		WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= $1
		ORDER BY next_attempt_at
		LIMIT $2`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: listing due pending transactions: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("postgres: scanning pending transaction id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: listing due pending transactions: %w", err)
	}
	return ids, nil
}
