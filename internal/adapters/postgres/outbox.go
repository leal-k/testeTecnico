package postgres

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/event"
)

var _ app.OutboxStore = (*DB)(nil)

const maxErrorLen = 500

// ClaimOutbox reserva eventos pendentes em uma única instrução:
//   - SKIP LOCKED: publishers concorrentes pulam as linhas que outro está
//     reservando no mesmo instante, em vez de esperar ou repetir;
//   - lease: a reserva vale até locked_until; se o publisher morrer, ela
//     vence e outro assume o evento (recuperação de trabalho abandonado).
//
// Os prazos usam o relógio do banco (now()), igual para todas as instâncias.
func (d *DB) ClaimOutbox(ctx context.Context, owner string, lease time.Duration, limit int) ([]app.OutboxRecord, error) {
	rows, err := d.pool.Query(ctx, `
		UPDATE outbox_events o
		SET locked_by = $1,
		    locked_until = now() + make_interval(secs => $2)
		WHERE o.id IN (
			SELECT id
			FROM outbox_events
			WHERE published_at IS NULL
			  AND next_attempt_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY created_at, id
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		RETURNING o.id, o.aggregate_type, o.aggregate_id, o.event_type, o.event_version,
		          o.correlation_id, COALESCE(o.causation_id, ''), o.payload, o.occurred_at,
		          o.attempts, o.created_at`,
		owner, lease.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: claiming outbox events: %w", err)
	}
	defer rows.Close()

	type claimed struct {
		rec       app.OutboxRecord
		createdAt time.Time
	}
	var out []claimed
	for rows.Next() {
		var (
			e         event.Event
			eventType string
			attempts  int
			createdAt time.Time
		)
		if err := rows.Scan(&e.ID, &e.AggregateType, &e.AggregateID, &eventType, &e.Version,
			&e.CorrelationID, &e.CausationID, &e.Payload, &e.OccurredAt, &attempts, &createdAt); err != nil {
			return nil, fmt.Errorf("postgres: scanning outbox event: %w", err)
		}
		e.Type = event.Type(eventType)
		e.OccurredAt = e.OccurredAt.UTC()
		out = append(out, claimed{app.OutboxRecord{Event: e, Attempts: attempts}, createdAt})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: claiming outbox events: %w", err)
	}

	// RETURNING não garante ordem: publica na ordem de gravação.
	slices.SortStableFunc(out, func(a, b claimed) int { return a.createdAt.Compare(b.createdAt) })
	records := make([]app.OutboxRecord, len(out))
	for i := range out {
		records[i] = out[i].rec
	}
	return records, nil
}

// MarkOutboxPublished vale para qualquer publisher: se o lease venceu e outro
// assumiu, quem publicou primeiro ainda marca, evitando uma republicação.
func (d *DB) MarkOutboxPublished(ctx context.Context, id uuid.UUID) error {
	_, err := d.pool.Exec(ctx, `
		UPDATE outbox_events
		SET published_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL
		WHERE id = $1 AND published_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("postgres: marking outbox event as published: %w", err)
	}
	return nil
}

// MarkOutboxFailed só vale para o dono atual da reserva.
func (d *DB) MarkOutboxFailed(ctx context.Context, id uuid.UUID, owner string, retryIn time.Duration, reason string) error {
	if len(reason) > maxErrorLen {
		reason = reason[:maxErrorLen]
	}
	_, err := d.pool.Exec(ctx, `
		UPDATE outbox_events
		SET attempts = attempts + 1,
		    next_attempt_at = now() + make_interval(secs => $3),
		    last_error = $4,
		    locked_by = NULL,
		    locked_until = NULL
		WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`,
		id, owner, retryIn.Seconds(), reason)
	if err != nil {
		return fmt.Errorf("postgres: recording outbox failure: %w", err)
	}
	return nil
}

// ReleaseOutbox devolve reservas do owner que ainda não foram publicadas.
func (d *DB) ReleaseOutbox(ctx context.Context, ids []uuid.UUID, owner string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := d.pool.Exec(ctx, `
		UPDATE outbox_events
		SET locked_by = NULL, locked_until = NULL
		WHERE id = ANY($1) AND locked_by = $2 AND published_at IS NULL`, ids, owner)
	if err != nil {
		return fmt.Errorf("postgres: releasing outbox events: %w", err)
	}
	return nil
}

// OutboxLag devolve a idade do evento pendente mais antigo (zero se não há
// pendências). É a métrica de atraso da outbox.
func (d *DB) OutboxLag(ctx context.Context) (time.Duration, int, error) {
	var (
		seconds float64
		pending int
	)
	err := d.pool.QueryRow(ctx, `
		SELECT COALESCE(EXTRACT(EPOCH FROM now() - MIN(created_at)), 0)::float8, COUNT(*)
		FROM outbox_events
		WHERE published_at IS NULL`).Scan(&seconds, &pending)
	if err != nil {
		return 0, 0, fmt.Errorf("postgres: measuring outbox lag: %w", err)
	}
	return time.Duration(seconds * float64(time.Second)), pending, nil
}
