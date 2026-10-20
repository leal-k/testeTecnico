package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
)

// clearOutbox marca como publicados os eventos que outros testes deixaram,
// para cada teste da outbox trabalhar só com os próprios.
func (e *env) clearOutbox(t *testing.T) {
	t.Helper()
	if _, err := e.pool.Exec(context.Background(),
		`UPDATE outbox_events SET published_at = now(), locked_by = NULL, locked_until = NULL WHERE published_at IS NULL`); err != nil {
		t.Fatal(err)
	}
}

// makeEvents gera eventos reais: cada aposta grava 2 (saldo + processada).
func (e *env) makeEvents(t *testing.T, bets int) int {
	t.Helper()
	w := e.newWallet(t, "1000.00")
	for i := 0; i < bets; i++ {
		e.process(t, 0, w.cmd(t, wager.KindBet, "1.00", fmt.Sprintf("ext-%d", i), fmt.Sprintf("key-%d", i)))
	}
	return bets * 2
}

func ids(recs []app.OutboxRecord) map[uuid.UUID]bool {
	out := map[uuid.UUID]bool{}
	for _, r := range recs {
		out[r.Event.ID] = true
	}
	return out
}

func TestOutbox_ConcurrentPublishersNeverClaimTheSameEvent(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.clearOutbox(t)
	total := e.makeEvents(t, 10) // 20 eventos

	// 4 publishers disputam ao mesmo tempo, cada um pedindo até 8.
	claims := make([][]app.OutboxRecord, 4)
	errs := make([]error, 4)
	runParallel(4, func(i int) {
		claims[i], errs[i] = e.dbs[i%len(e.dbs)].ClaimOutbox(ctx, fmt.Sprintf("owner-%d", i), time.Minute, 8)
	})

	seen := map[uuid.UUID]string{}
	for i, recs := range claims {
		if errs[i] != nil {
			t.Fatalf("owner-%d: %v", i, errs[i])
		}
		for _, r := range recs {
			if prev, dup := seen[r.Event.ID]; dup {
				t.Errorf("event %s claimed by %s and owner-%d", r.Event.ID, prev, i)
			}
			seen[r.Event.ID] = fmt.Sprintf("owner-%d", i)
		}
	}
	if len(seen) != total {
		t.Errorf("claimed %d distinct events, want %d", len(seen), total)
	}
}

func TestOutbox_ClaimedEventCarriesTheStoredSnapshot(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.clearOutbox(t)
	e.makeEvents(t, 1)

	recs, err := e.dbs[0].ClaimOutbox(ctx, "owner", time.Minute, 10)
	if err != nil || len(recs) != 2 {
		t.Fatalf("claim = %d, %v", len(recs), err)
	}
	for _, r := range recs {
		ev := r.Event
		if ev.ID == uuid.Nil || ev.Type == "" || ev.Version != 1 || ev.AggregateID == uuid.Nil ||
			ev.CorrelationID == "" || ev.OccurredAt.IsZero() || len(ev.Payload) == 0 || r.Attempts != 0 {
			t.Errorf("claimed event = %+v", r)
		}
	}
}

func TestOutbox_AbandonedClaimIsRecoveredAfterTheLease(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.clearOutbox(t)
	total := e.makeEvents(t, 2)

	// O publisher A reserva e "morre" (nunca publica nem libera).
	first, err := e.dbs[0].ClaimOutbox(ctx, "dead-publisher", time.Second, 100)
	if err != nil || len(first) != total {
		t.Fatalf("first claim = %d, %v", len(first), err)
	}
	// Com a reserva válida, ninguém mais pega.
	if again, _ := e.dbs[1].ClaimOutbox(ctx, "publisher-b", time.Minute, 100); len(again) != 0 {
		t.Errorf("events with a valid lease were claimed again: %d", len(again))
	}
	// Vencido o lease, outro publisher assume os mesmos eventos (mesmo eventId).
	time.Sleep(1200 * time.Millisecond)
	recovered, err := e.dbs[1].ClaimOutbox(ctx, "publisher-b", time.Minute, 100)
	if err != nil {
		t.Fatal(err)
	}
	firstIDs := ids(first)
	if len(recovered) != total {
		t.Fatalf("recovered %d events, want %d", len(recovered), total)
	}
	for _, r := range recovered {
		if !firstIDs[r.Event.ID] {
			t.Errorf("recovered an event with a different id: %s", r.Event.ID)
		}
	}
}

func TestOutbox_PublishedAndFailedEvents(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	db := e.dbs[0]
	e.clearOutbox(t)
	e.makeEvents(t, 1) // 2 eventos

	recs, err := db.ClaimOutbox(ctx, "owner", time.Minute, 10)
	if err != nil || len(recs) != 2 {
		t.Fatalf("claim = %d, %v", len(recs), err)
	}
	published, failed := recs[0].Event.ID, recs[1].Event.ID

	if err := db.MarkOutboxPublished(ctx, published); err != nil {
		t.Fatal(err)
	}
	// Outro publisher não consegue registrar falha num evento que não é dele.
	if err := db.MarkOutboxFailed(ctx, failed, "intruder", time.Hour, "x"); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, `SELECT attempts FROM outbox_events WHERE id = $1`, failed); n != 0 {
		t.Errorf("a non-owner changed the attempts: %d", n)
	}
	if err := db.MarkOutboxFailed(ctx, failed, "owner", time.Second, "broker down"); err != nil {
		t.Fatal(err)
	}

	// Nada a pegar agora: um publicado, o outro agendado para daqui a 1s.
	if again, _ := db.ClaimOutbox(ctx, "owner-2", time.Minute, 10); len(again) != 0 {
		t.Errorf("claimed %d events, want 0", len(again))
	}
	time.Sleep(1200 * time.Millisecond)
	retry, err := db.ClaimOutbox(ctx, "owner-2", time.Minute, 10)
	if err != nil || len(retry) != 1 || retry[0].Event.ID != failed || retry[0].Attempts != 1 {
		t.Fatalf("retry claim = %+v, %v", retry, err)
	}
	var lastErr string
	if err := e.pool.QueryRow(ctx, `SELECT last_error FROM outbox_events WHERE id = $1`, failed).Scan(&lastErr); err != nil || lastErr != "broker down" {
		t.Errorf("last_error = %q, %v", lastErr, err)
	}
	// Marcar como publicado de novo é inofensivo.
	if err := db.MarkOutboxPublished(ctx, published); err != nil {
		t.Errorf("idempotent mark: %v", err)
	}
}

func TestOutbox_MarkPublishedBatch(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	db := e.dbs[0]
	e.clearOutbox(t)
	total := e.makeEvents(t, 3) // 6 eventos

	recs, err := db.ClaimOutbox(ctx, "owner", time.Minute, 10)
	if err != nil || len(recs) != total {
		t.Fatalf("claim = %d, %v", len(recs), err)
	}
	batch := []uuid.UUID{recs[0].Event.ID, recs[1].Event.ID, recs[2].Event.ID, recs[3].Event.ID}
	if err := db.MarkOutboxPublishedBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox_events WHERE id = ANY($1) AND published_at IS NOT NULL AND locked_by IS NULL`, batch); n != 4 {
		t.Errorf("published = %d, want 4", n)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`); n != total-4 {
		t.Errorf("pending = %d, want %d (only the batch is marked)", n, total-4)
	}
	// Repetir é inofensivo e não muda published_at.
	var first time.Time
	_ = e.pool.QueryRow(ctx, `SELECT published_at FROM outbox_events WHERE id = $1`, batch[0]).Scan(&first)
	if err := db.MarkOutboxPublishedBatch(ctx, batch); err != nil {
		t.Errorf("idempotent mark: %v", err)
	}
	var again time.Time
	_ = e.pool.QueryRow(ctx, `SELECT published_at FROM outbox_events WHERE id = $1`, batch[0]).Scan(&again)
	if !first.Equal(again) {
		t.Errorf("published_at changed on the repeated mark")
	}
	if err := db.MarkOutboxPublishedBatch(ctx, nil); err != nil {
		t.Errorf("empty batch: %v", err)
	}
}

func TestOutbox_ReleaseAndLag(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	db := e.dbs[0]
	e.clearOutbox(t)

	if lag, pending, err := db.OutboxLag(ctx); err != nil || pending != 0 || lag != 0 {
		t.Errorf("empty outbox lag = %v pending = %d, %v", lag, pending, err)
	}

	total := e.makeEvents(t, 2)
	recs, err := db.ClaimOutbox(ctx, "owner", time.Hour, 100)
	if err != nil || len(recs) != total {
		t.Fatalf("claim = %d, %v", len(recs), err)
	}
	evIDs := make([]uuid.UUID, len(recs))
	for i, r := range recs {
		evIDs[i] = r.Event.ID
	}
	if err := db.ReleaseOutbox(ctx, evIDs, "owner"); err != nil {
		t.Fatal(err)
	}
	// Liberado: outro publisher pega na hora, sem esperar a hora de lease.
	if again, _ := db.ClaimOutbox(ctx, "owner-2", time.Minute, 100); len(again) != total {
		t.Errorf("after release, claimed %d, want %d", len(again), total)
	}

	time.Sleep(50 * time.Millisecond)
	lag, pending, err := db.OutboxLag(ctx)
	if err != nil || pending != total || lag <= 0 {
		t.Errorf("lag = %v pending = %d, %v", lag, pending, err)
	}
}
