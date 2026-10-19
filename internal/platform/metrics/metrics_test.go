package metrics

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
)

type stubProcessor struct {
	res app.Result
	err error
}

func (s stubProcessor) Process(context.Context, app.ProcessCommand) (app.Result, error) {
	return s.res, s.err
}

func TestInstrument_CountsResultsDuplicatesErrorsAndLatency(t *testing.T) {
	m := New()
	ctx := context.Background()

	_, _ = m.Instrument("http", stubProcessor{res: app.Result{Status: wager.StatusProcessed}}).Process(ctx, app.ProcessCommand{})
	_, _ = m.Instrument("http", stubProcessor{res: app.Result{Status: wager.StatusProcessed, Replay: true}}).Process(ctx, app.ProcessCommand{})
	_, _ = m.Instrument("sqs", stubProcessor{res: app.Result{Status: wager.StatusProcessed, Replay: true, DuplicateMessage: true}}).Process(ctx, app.ProcessCommand{})
	_, _ = m.Instrument("http", stubProcessor{res: app.Result{Status: wager.StatusRejected}}).Process(ctx, app.ProcessCommand{})
	_, _ = m.Instrument("http", stubProcessor{err: app.ErrIdempotencyConflict}).Process(ctx, app.ProcessCommand{})
	_, _ = m.Instrument("sqs", stubProcessor{err: errors.New("db down")}).Process(ctx, app.ProcessCommand{})

	checks := map[string]float64{
		"http PROCESSED":          testutil.ToFloat64(m.results.WithLabelValues("http", "PROCESSED")),
		"http REJECTED":           testutil.ToFloat64(m.results.WithLabelValues("http", "REJECTED")),
		"sqs PROCESSED":           testutil.ToFloat64(m.results.WithLabelValues("sqs", "PROCESSED")),
		"http replays":            testutil.ToFloat64(m.duplicates.WithLabelValues("http", "idempotent_replay")),
		"sqs duplicate messages":  testutil.ToFloat64(m.duplicates.WithLabelValues("sqs", "duplicate_message")),
		"http idempotency errors": testutil.ToFloat64(m.errors.WithLabelValues("http", "idempotency_conflict")),
		"sqs internal errors":     testutil.ToFloat64(m.errors.WithLabelValues("sqs", "internal")),
	}
	want := map[string]float64{
		"http PROCESSED": 2, "http REJECTED": 1, "sqs PROCESSED": 1, "http replays": 1,
		"sqs duplicate messages": 1, "http idempotency errors": 1, "sqs internal errors": 1,
	}
	for k, w := range want {
		if checks[k] != w {
			t.Errorf("%s = %v, want %v", k, checks[k], w)
		}
	}
	if n := testutil.CollectAndCount(m.latency); n != 2 { // uma série por origem
		t.Errorf("latency series = %d, want 2", n)
	}
}

type stubRetrier struct{ err error }

func (s stubRetrier) RetryPending(context.Context, uuid.UUID) error { return s.err }

func TestInstrumentRetrier(t *testing.T) {
	m := New()
	_ = m.InstrumentRetrier(stubRetrier{}).RetryPending(context.Background(), uuid.New())
	_ = m.InstrumentRetrier(stubRetrier{err: errors.New("x")}).RetryPending(context.Background(), uuid.New())
	if ok, bad := testutil.ToFloat64(m.pending.WithLabelValues("ok")), testutil.ToFloat64(m.pending.WithLabelValues("error")); ok != 1 || bad != 1 {
		t.Errorf("ok=%v error=%v", ok, bad)
	}
}

func TestHandler_ExposesEveryRequiredMetric(t *testing.T) {
	m := New()
	m.RegisterOutboxLag(func(context.Context) (time.Duration, int, error) { return 3 * time.Second, 7, nil })
	m.ConcurrencyConflict()
	m.SQSOutcome("dlq")
	m.SQSOutcome("retry")
	m.OutboxPublished()
	m.OutboxFailed()
	m.ReconciliationDivergence()
	_, _ = m.Instrument("http", stubProcessor{res: app.Result{Status: wager.StatusProcessed, Replay: true}}).Process(context.Background(), app.ProcessCommand{})
	_ = m.InstrumentRetrier(stubRetrier{}).RetryPending(context.Background(), uuid.New())

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	text := string(body)

	for _, want := range []string{
		`testetecnico_wager_transactions_total{source="http",status="PROCESSED"} 1`,
		`testetecnico_wager_duplicates_total{kind="idempotent_replay",source="http"} 1`,
		`testetecnico_wager_processing_duration_seconds_count{source="http"} 1`,
		`testetecnico_concurrency_conflicts_total 1`,
		`testetecnico_sqs_messages_total{outcome="dlq"} 1`,
		`testetecnico_sqs_messages_total{outcome="retry"} 1`,
		`testetecnico_pending_reference_retries_total{result="ok"} 1`,
		`testetecnico_outbox_published_total 1`,
		`testetecnico_outbox_publish_failures_total 1`,
		`testetecnico_outbox_lag_seconds 3`,
		`testetecnico_outbox_pending_events 7`,
		`testetecnico_reconciliation_divergences_total 1`,
		`go_goroutines`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("/metrics does not contain %q", want)
		}
	}
}

func TestOutboxLag_SkipsTheSampleWhenTheDatabaseFails(t *testing.T) {
	m := New()
	m.RegisterOutboxLag(func(context.Context) (time.Duration, int, error) { return 0, 0, errors.New("down") })
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if strings.Contains(rec.Body.String(), "outbox_lag_seconds ") {
		t.Errorf("a failed measurement must not publish a value")
	}
	if rec.Code != 200 {
		t.Errorf("status = %d", rec.Code)
	}
}
