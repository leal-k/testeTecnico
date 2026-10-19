package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type stubLister struct {
	mu    sync.Mutex
	ids   []uuid.UUID
	err   error
	calls int
}

func (s *stubLister) ListDuePending(_ context.Context, _ time.Time, limit int) ([]uuid.UUID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	n := min(limit, len(s.ids))
	out := s.ids[:n]
	s.ids = s.ids[n:]
	return out, nil
}

type stubRetrier struct {
	mu      sync.Mutex
	retried []uuid.UUID
	failOn  uuid.UUID
}

func (s *stubRetrier) RetryPending(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retried = append(s.retried, id)
	if id == s.failOn {
		return errors.New("database down")
	}
	return nil
}

func TestPendingWorker_RunOnceRetriesEveryDueTransaction(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	lister := &stubLister{ids: []uuid.UUID{a, b, c}}
	retrier := &stubRetrier{failOn: b}
	w := NewPendingWorker(lister, retrier, time.Hour, 10, nil)

	if n := w.RunOnce(context.Background()); n != 3 {
		t.Errorf("RunOnce = %d, want 3", n)
	}
	// uma falha em b não impede c
	if len(retrier.retried) != 3 || retrier.retried[2] != c {
		t.Errorf("retried = %v", retrier.retried)
	}
}

func TestPendingWorker_ListingFailureIsNotFatal(t *testing.T) {
	w := NewPendingWorker(&stubLister{err: errors.New("down")}, &stubRetrier{}, time.Hour, 10, nil)
	if n := w.RunOnce(context.Background()); n != 0 {
		t.Errorf("RunOnce = %d, want 0", n)
	}
}

func TestPendingWorker_DrainsFullBatchesWithoutWaiting(t *testing.T) {
	ids := make([]uuid.UUID, 25)
	for i := range ids {
		ids[i] = uuid.New()
	}
	lister := &stubLister{ids: ids}
	retrier := &stubRetrier{}
	// intervalo longo: só termina rápido se os lotes cheios não esperarem
	w := NewPendingWorker(lister, retrier, time.Hour, 10, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	deadline := time.After(5 * time.Second)
	for {
		retrier.mu.Lock()
		n := len(retrier.retried)
		retrier.mu.Unlock()
		if n == 25 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d of 25 pending transactions were retried", n)
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker did not stop after cancellation")
	}
}
