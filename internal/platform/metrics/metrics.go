// Package metrics expõe as métricas da aplicação no formato do Prometheus.
//
// As métricas são coletadas "por fora": decoradores envolvem os casos de uso e
// ganchos (OnOutcome, OnPublished...) recebem os eventos dos workers. O
// domínio e os casos de uso não importam este pacote.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/leal-k/testeTecnico/internal/app"
)

const namespace = "testetecnico"

// Metrics reúne o registro e todas as métricas.
type Metrics struct {
	registry *prometheus.Registry

	results     *prometheus.CounterVec   // operações concluídas, por origem e status
	errors      *prometheus.CounterVec   // operações que retornaram erro, por origem e motivo
	duplicates  *prometheus.CounterVec   // replays e mensagens duplicadas
	latency     *prometheus.HistogramVec // duração do processamento
	conflicts   prometheus.Counter       // disputas resolvidas refazendo a operação
	sqs         *prometheus.CounterVec   // desfecho de cada mensagem: processed, duplicate, retry, dlq
	pending     *prometheus.CounterVec   // tentativas do worker de referências pendentes
	outboxSent  prometheus.Counter
	outboxFails prometheus.Counter
	divergences prometheus.Counter
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		results: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "wager_transactions_total",
			Help: "Operations handled, by source (http, sqs) and resulting status (PROCESSED, REJECTED, PENDING_REFERENCE...).",
		}, []string{"source", "status"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "wager_transaction_errors_total",
			Help: "Operations that returned an error, by source and reason.",
		}, []string{"source", "reason"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "wager_duplicates_total",
			Help: "Repeated operations answered without reapplying: idempotent replays and duplicated messages.",
		}, []string{"source", "kind"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "wager_processing_duration_seconds",
			Help:    "Time to process one operation, including the database transaction.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"source"}),
		conflicts: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "concurrency_conflicts_total",
			Help: "Concurrent writers that raced on the same operation; the loser redoes it and sees a replay.",
		}),
		sqs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "sqs_messages_total",
			Help: "Outcome of each SQS message: processed, duplicate, retry (transient failure) or dlq.",
		}, []string{"outcome"}),
		pending: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "pending_reference_retries_total",
			Help: "Re-evaluations of transactions waiting for their reference, by result (ok, error).",
		}, []string{"result"}),
		outboxSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "outbox_published_total",
			Help: "Events published from the outbox (a republication counts again).",
		}),
		outboxFails: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "outbox_publish_failures_total",
			Help: "Failed publication attempts; the event stays in the outbox and is retried.",
		}),
		divergences: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "reconciliation_divergences_total",
			Help: "Reconciliations whose stored balance differed from the ledger.",
		}),
	}
	reg.MustRegister(
		m.results, m.errors, m.duplicates, m.latency, m.conflicts, m.sqs,
		m.pending, m.outboxSent, m.outboxFails, m.divergences,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Handler serve as métricas (GET /metrics).
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}

func (m *Metrics) ConcurrencyConflict()      { m.conflicts.Inc() }
func (m *Metrics) SQSOutcome(outcome string) { m.sqs.WithLabelValues(outcome).Inc() }
func (m *Metrics) OutboxPublished()          { m.outboxSent.Inc() }
func (m *Metrics) OutboxFailed()             { m.outboxFails.Inc() }
func (m *Metrics) ReconciliationDivergence() { m.divergences.Inc() }

// ---- decoradores ------------------------------------------------------------

// Processor é o contrato comum de HTTP e SQS (app.Processor o implementa).
type Processor interface {
	Process(ctx context.Context, cmd app.ProcessCommand) (app.Result, error)
}

type instrumentedProcessor struct {
	inner  Processor
	m      *Metrics
	source string
}

// Instrument mede cada operação: duração, status, duplicatas e erros.
func (m *Metrics) Instrument(source string, p Processor) Processor {
	return &instrumentedProcessor{inner: p, m: m, source: source}
}

func (p *instrumentedProcessor) Process(ctx context.Context, cmd app.ProcessCommand) (app.Result, error) {
	start := time.Now()
	res, err := p.inner.Process(ctx, cmd)
	p.m.latency.WithLabelValues(p.source).Observe(time.Since(start).Seconds())

	if err != nil {
		p.m.errors.WithLabelValues(p.source, errorReason(err)).Inc()
		return res, err
	}
	p.m.results.WithLabelValues(p.source, string(res.Status)).Inc()
	if res.Replay {
		p.m.duplicates.WithLabelValues(p.source, "idempotent_replay").Inc()
	}
	if res.DuplicateMessage {
		p.m.duplicates.WithLabelValues(p.source, "duplicate_message").Inc()
	}
	return res, nil
}

// errorReason mantém o rótulo com poucos valores possíveis.
func errorReason(err error) string {
	switch {
	case errors.Is(err, app.ErrIdempotencyConflict):
		return "idempotency_conflict"
	case errors.Is(err, app.ErrExternalIDConflict):
		return "external_id_conflict"
	case errors.Is(err, app.ErrInboxConflict):
		return "message_id_conflict"
	case errors.Is(err, app.ErrInvalidCommand):
		return "invalid"
	case errors.Is(err, app.ErrWalletNotFound):
		return "wallet_not_found"
	case errors.Is(err, app.ErrWalletPlayerMismatch), errors.Is(err, app.ErrCurrencyMismatch):
		return "wallet_mismatch"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "timeout"
	default:
		return "internal"
	}
}

type instrumentedRetrier struct {
	inner app.PendingRetrier
	m     *Metrics
}

// InstrumentRetrier conta as tentativas do worker de referências pendentes.
func (m *Metrics) InstrumentRetrier(r app.PendingRetrier) app.PendingRetrier {
	return &instrumentedRetrier{inner: r, m: m}
}

func (r *instrumentedRetrier) RetryPending(ctx context.Context, id uuid.UUID) error {
	err := r.inner.RetryPending(ctx, id)
	result := "ok"
	if err != nil {
		result = "error"
	}
	r.m.pending.WithLabelValues(result).Inc()
	return err
}

// ---- atraso da outbox -------------------------------------------------------

// LagFunc mede a idade do evento pendente mais antigo e quantos estão pendentes.
type LagFunc func(ctx context.Context) (time.Duration, int, error)

type outboxCollector struct {
	measure LagFunc
	lag     *prometheus.Desc
	pending *prometheus.Desc
}

// RegisterOutboxLag mede o atraso da outbox no momento de cada coleta, direto
// do banco (vale para todas as instâncias, não só para esta).
func (m *Metrics) RegisterOutboxLag(measure LagFunc) {
	m.registry.MustRegister(&outboxCollector{
		measure: measure,
		lag: prometheus.NewDesc(namespace+"_outbox_lag_seconds",
			"Age of the oldest unpublished outbox event (0 when nothing is pending).", nil, nil),
		pending: prometheus.NewDesc(namespace+"_outbox_pending_events",
			"Outbox events not yet published.", nil, nil),
	})
}

func (c *outboxCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.lag
	ch <- c.pending
}

func (c *outboxCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	lag, pending, err := c.measure(ctx)
	if err != nil {
		// Sem leitura nesta coleta: melhor faltar o ponto do que mentir.
		return
	}
	ch <- prometheus.MustNewConstMetric(c.lag, prometheus.GaugeValue, lag.Seconds())
	ch <- prometheus.MustNewConstMetric(c.pending, prometheus.GaugeValue, float64(pending))
}
