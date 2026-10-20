package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/event"
)

type fakeOutboxStore struct {
	mu        sync.Mutex
	pending   []OutboxRecord
	published []uuid.UUID
	failed    map[uuid.UUID]time.Duration
	released  []uuid.UUID
	markErr   error
}

func (s *fakeOutboxStore) ClaimOutbox(_ context.Context, _ string, _ time.Duration, limit int) ([]OutboxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := min(limit, len(s.pending))
	out := s.pending[:n]
	s.pending = s.pending[n:]
	return out, nil
}

func (s *fakeOutboxStore) MarkOutboxPublished(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.markErr != nil {
		return s.markErr
	}
	s.published = append(s.published, id)
	return nil
}

func (s *fakeOutboxStore) MarkOutboxFailed(_ context.Context, id uuid.UUID, _ string, retryIn time.Duration, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed == nil {
		s.failed = map[uuid.UUID]time.Duration{}
	}
	s.failed[id] = retryIn
	return nil
}

func (s *fakeOutboxStore) ReleaseOutbox(_ context.Context, ids []uuid.UUID, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released = append(s.released, ids...)
	return nil
}

type fakePublisher struct {
	mu     sync.Mutex
	sent   []event.Event
	failOn map[uuid.UUID]bool
	hook   func()
}

func (p *fakePublisher) Publish(_ context.Context, e event.Event) error {
	p.mu.Lock()
	hook := p.hook
	fail := p.failOn[e.ID]
	if !fail {
		p.sent = append(p.sent, e)
	}
	p.mu.Unlock()
	if hook != nil {
		hook()
	}
	if fail {
		return errors.New("broker unavailable")
	}
	return nil
}

func records(n int) []OutboxRecord {
	out := make([]OutboxRecord, n)
	for i := range out {
		out[i] = OutboxRecord{Event: event.Event{ID: uuid.New(), Type: event.TypeWalletBalanceChanged, AggregateID: uuid.New()}}
	}
	return out
}

func relayConfig() OutboxConfig {
	cfg := DefaultOutboxConfig()
	cfg.BatchSize = 10
	cfg.BackoffBase = time.Second
	cfg.BackoffMax = time.Minute
	return cfg
}

func TestOutboxRelay_PublishesAndMarksEveryEvent(t *testing.T) {
	store := &fakeOutboxStore{pending: records(3)}
	pub := &fakePublisher{}
	relay := NewOutboxRelay(store, pub, "owner-1", relayConfig(), nil)

	if n := relay.RunOnce(context.Background()); n != 3 {
		t.Errorf("RunOnce = %d, want 3", n)
	}
	if len(pub.sent) != 3 || len(store.published) != 3 {
		t.Errorf("sent=%d published=%d", len(pub.sent), len(store.published))
	}
	for i := range pub.sent {
		if pub.sent[i].ID != store.published[i] {
			t.Errorf("event %d: published id mismatch", i)
		}
	}
}

func TestOutboxRelay_FailureIsRetriedWithBackoffAndNeverMarkedPublished(t *testing.T) {
	recs := records(2)
	recs[0].Attempts = 0
	recs[1].Attempts = 3
	store := &fakeOutboxStore{pending: recs}
	pub := &fakePublisher{failOn: map[uuid.UUID]bool{recs[0].Event.ID: true, recs[1].Event.ID: true}}
	relay := NewOutboxRelay(store, pub, "owner-1", relayConfig(), nil)

	relay.RunOnce(context.Background())

	if len(store.published) != 0 {
		t.Errorf("failed events were marked as published")
	}
	if store.failed[recs[0].Event.ID] != time.Second || store.failed[recs[1].Event.ID] != 8*time.Second {
		t.Errorf("backoff = %v", store.failed)
	}
}

func TestOutboxRelay_MarkFailureDoesNotLoseTheEvent(t *testing.T) {
	// Publicou, mas não conseguiu marcar: não é erro fatal; o evento volta pelo
	// lease e é republicado com o mesmo eventId.
	store := &fakeOutboxStore{pending: records(1), markErr: errors.New("db down")}
	pub := &fakePublisher{}
	relay := NewOutboxRelay(store, pub, "owner-1", relayConfig(), nil)
	relay.RunOnce(context.Background())
	if len(pub.sent) != 1 {
		t.Errorf("sent = %d", len(pub.sent))
	}
}

func TestOutboxRelay_ShutdownReleasesTheRestOfTheBatch(t *testing.T) {
	recs := records(4)
	store := &fakeOutboxStore{pending: recs}
	ctx, cancel := context.WithCancel(context.Background())
	pub := &fakePublisher{hook: cancel} // o encerramento chega durante o 1º evento
	relay := NewOutboxRelay(store, pub, "owner-1", relayConfig(), nil)

	relay.RunOnce(ctx)

	if len(pub.sent) != 1 || len(store.published) != 1 {
		t.Errorf("sent=%d published=%d, want only the event in progress", len(pub.sent), len(store.published))
	}
	if len(store.released) != 3 {
		t.Errorf("released = %d, want the 3 not yet published", len(store.released))
	}
}

func TestOutboxRelay_ReleasesWhatTheLeaseNoLongerCovers(t *testing.T) {
	store := &fakeOutboxStore{pending: records(5)}
	pub := &fakePublisher{hook: func() { time.Sleep(30 * time.Millisecond) }} // broker lento
	cfg := relayConfig()
	cfg.Lease = 100 * time.Millisecond
	cfg.PublishTimeout = 50 * time.Millisecond // margem de 50ms para começar publicações

	NewOutboxRelay(store, pub, "owner-1", cfg, nil).RunOnce(context.Background())

	if len(pub.sent) == 0 || len(pub.sent) == 5 {
		t.Fatalf("sent %d events; expected some to be released before the lease ran out", len(pub.sent))
	}
	if len(pub.sent)+len(store.released) != 5 {
		t.Errorf("sent=%d released=%d, want 5 in total", len(pub.sent), len(store.released))
	}
}

func TestOutboxRelay_RunStopsOnCancel(t *testing.T) {
	relay := NewOutboxRelay(&fakeOutboxStore{}, &fakePublisher{}, "owner-1", relayConfig(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		relay.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

// ---- publicação em lote -----------------------------------------------------

// fakeBatchPublisher publica em lote reaproveitando as regras do fakePublisher
// (failOn, hook), e registra o tamanho de cada chamada.
type fakeBatchPublisher struct {
	fakePublisher
	calls []int
}

func (p *fakeBatchPublisher) PublishBatch(ctx context.Context, events []event.Event) []error {
	p.mu.Lock()
	p.calls = append(p.calls, len(events))
	p.mu.Unlock()
	errs := make([]error, len(events))
	for i, e := range events {
		errs[i] = p.Publish(ctx, e)
	}
	return errs
}

// fakeBatchStore também marca em lote, e registra cada chamada.
type fakeBatchStore struct {
	*fakeOutboxStore
	markCalls int
}

func (s *fakeBatchStore) MarkOutboxPublishedBatch(_ context.Context, ids []uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markCalls++
	if s.markErr != nil {
		return s.markErr
	}
	s.published = append(s.published, ids...)
	return nil
}

func TestOutboxRelay_Batch_SplitsIntoBrokerSizedCallsAndMarksOncePerCall(t *testing.T) {
	store := &fakeBatchStore{fakeOutboxStore: &fakeOutboxStore{pending: records(25)}}
	pub := &fakeBatchPublisher{}
	cfg := relayConfig()
	cfg.BatchSize = 25
	NewOutboxRelay(store, pub, "owner-1", cfg, nil).RunOnce(context.Background())

	if len(pub.calls) != 3 || pub.calls[0] != 10 || pub.calls[1] != 10 || pub.calls[2] != 5 {
		t.Errorf("publish calls = %v, want [10 10 5]", pub.calls)
	}
	if store.markCalls != 3 || len(store.published) != 25 {
		t.Errorf("mark calls = %d, published = %d", store.markCalls, len(store.published))
	}
	for i := range pub.sent {
		if pub.sent[i].ID != store.published[i] {
			t.Fatalf("event %d: order or id mismatch", i)
		}
	}
}

func TestOutboxRelay_Batch_PartialFailureOnlyRetriesTheFailedEvents(t *testing.T) {
	recs := records(4)
	recs[2].Attempts = 2
	store := &fakeOutboxStore{pending: recs} // sem marcação em lote: marca um por um
	pub := &fakeBatchPublisher{fakePublisher: fakePublisher{failOn: map[uuid.UUID]bool{recs[2].Event.ID: true}}}
	NewOutboxRelay(store, pub, "owner-1", relayConfig(), nil).RunOnce(context.Background())

	if len(pub.calls) != 1 {
		t.Errorf("publish calls = %v, want one batch", pub.calls)
	}
	if len(store.published) != 3 {
		t.Errorf("published = %d, want the 3 delivered", len(store.published))
	}
	for _, id := range store.published {
		if id == recs[2].Event.ID {
			t.Errorf("the failed event was marked as published")
		}
	}
	if len(store.failed) != 1 || store.failed[recs[2].Event.ID] != 4*time.Second {
		t.Errorf("failed = %v, want only the 3rd event with backoff 4s", store.failed)
	}
}

func TestOutboxRelay_Batch_MarkFailureDoesNotLoseEvents(t *testing.T) {
	store := &fakeBatchStore{fakeOutboxStore: &fakeOutboxStore{pending: records(3), markErr: errors.New("db down")}}
	pub := &fakeBatchPublisher{}
	NewOutboxRelay(store, pub, "owner-1", relayConfig(), nil).RunOnce(context.Background())

	if len(pub.sent) != 3 || len(store.failed) != 0 {
		t.Errorf("sent=%d failed=%d: a mark failure must not be treated as a publish failure", len(pub.sent), len(store.failed))
	}
}

func TestOutboxRelay_Batch_ShutdownReleasesTheBatchesNotStarted(t *testing.T) {
	store := &fakeOutboxStore{pending: records(25)}
	ctx, cancel := context.WithCancel(context.Background())
	pub := &fakeBatchPublisher{fakePublisher: fakePublisher{hook: cancel}} // encerramento durante o 1º lote
	cfg := relayConfig()
	cfg.BatchSize = 25
	NewOutboxRelay(store, pub, "owner-1", cfg, nil).RunOnce(ctx)

	if len(store.published) != 10 {
		t.Errorf("published = %d, want the 10 of the batch in progress", len(store.published))
	}
	if len(store.released) != 15 {
		t.Errorf("released = %d, want the 15 not started", len(store.released))
	}
}
