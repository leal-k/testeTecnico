package app

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/domain/event"
)

// OutboxRecord é um evento pendente reservado para publicação.
type OutboxRecord struct {
	Event    event.Event
	Attempts int
}

// OutboxStore guarda os eventos e coordena a reserva entre publishers.
type OutboxStore interface {
	// ClaimOutbox reserva até limit eventos pendentes e vencidos para owner,
	// por lease. Eventos reservados por outro publisher com lease ainda válido
	// ficam de fora; reservas vencidas (publisher que morreu) voltam a valer.
	ClaimOutbox(ctx context.Context, owner string, lease time.Duration, limit int) ([]OutboxRecord, error)
	// MarkOutboxPublished registra a publicação. Idempotente.
	MarkOutboxPublished(ctx context.Context, id uuid.UUID) error
	// MarkOutboxFailed registra a falha e agenda a próxima tentativa.
	MarkOutboxFailed(ctx context.Context, id uuid.UUID, owner string, retryIn time.Duration, reason string) error
	// ReleaseOutbox devolve reservas ainda não publicadas (encerramento).
	ReleaseOutbox(ctx context.Context, ids []uuid.UUID, owner string) error
}

// EventPublisher entrega um evento ao destino externo.
type EventPublisher interface {
	Publish(ctx context.Context, e event.Event) error
}

// MaxPublishBatch é o maior lote enviado de uma vez (limite do SQS).
const MaxPublishBatch = 10

// BatchPublisher é opcional: um publisher que entrega vários eventos numa só
// chamada. Devolve um erro por evento, na mesma ordem (nil = entregue), porque
// o lote pode falhar em parte. O relay usa este caminho quando disponível.
type BatchPublisher interface {
	EventPublisher
	PublishBatch(ctx context.Context, events []event.Event) []error
}

// OutboxBatchMarker é opcional: marca vários eventos como publicados numa só
// operação. Sem ele, o relay marca um por um.
type OutboxBatchMarker interface {
	MarkOutboxPublishedBatch(ctx context.Context, ids []uuid.UUID) error
}

type OutboxConfig struct {
	Interval       time.Duration // espera entre rodadas sem trabalho
	BatchSize      int
	Lease          time.Duration // validade da reserva
	PublishTimeout time.Duration // limite para publicar UM evento ou UM lote de até MaxPublishBatch
	BackoffBase    time.Duration
	BackoffMax     time.Duration
}

func DefaultOutboxConfig() OutboxConfig {
	return OutboxConfig{
		Interval:       500 * time.Millisecond,
		BatchSize:      50,
		Lease:          30 * time.Second,
		PublishTimeout: 5 * time.Second,
		BackoffBase:    time.Second,
		BackoffMax:     time.Minute,
	}
}

// OutboxRelay publica os eventos gravados na outbox. Garantias:
//   - só publica o que já foi confirmado no banco (a outbox é gravada na mesma
//     transação da operação);
//   - entrega pelo menos uma vez: se o processo morrer entre publicar e marcar
//     como publicado, o lease vence e o evento é republicado com o MESMO eventId,
//     para os consumidores deduplicarem;
//   - várias instâncias dividem o trabalho sem publicar o mesmo evento ao
//     mesmo tempo (reserva com SKIP LOCKED + lease);
//   - falha de publicação não perde o evento: ele volta com backoff, sem limite
//     de tentativas;
//   - com um BatchPublisher, publica em lotes de até MaxPublishBatch: uma ida
//     ao broker e uma ao banco por lote, em vez de duas por evento. Cada evento
//     do lote tem resultado próprio (falha parcial não afeta os outros).
type OutboxRelay struct {
	store       OutboxStore
	publisher   EventPublisher
	owner       string
	cfg         OutboxConfig
	log         *slog.Logger
	onPublished func(e event.Event, attempts int)
	onFailed    func(e event.Event, err error)
}

func NewOutboxRelay(store OutboxStore, publisher EventPublisher, owner string, cfg OutboxConfig, log *slog.Logger) *OutboxRelay {
	if log == nil {
		log = slog.Default()
	}
	return &OutboxRelay{
		store:       store,
		publisher:   publisher,
		owner:       owner,
		cfg:         cfg,
		log:         log.With("worker", "outbox-publisher", "owner", owner),
		onPublished: func(event.Event, int) {},
		onFailed:    func(event.Event, error) {},
	}
}

// OnPublished e OnFailed permitem medir (métricas) cada publicação.
func (r *OutboxRelay) OnPublished(fn func(e event.Event, attempts int)) { r.onPublished = fn }
func (r *OutboxRelay) OnFailed(fn func(e event.Event, err error))       { r.onFailed = fn }

// Run publica até ctx ser cancelado.
func (r *OutboxRelay) Run(ctx context.Context) {
	r.log.Info("worker started", "interval", r.cfg.Interval.String(), "lease", r.cfg.Lease.String())
	defer r.log.Info("worker stopped")
	for {
		n := r.RunOnce(ctx)
		wait := r.cfg.Interval
		if n >= r.cfg.BatchSize {
			wait = 0 // lote cheio: provavelmente há mais
		} else {
			wait += time.Duration(rand.Int64N(int64(r.cfg.Interval)/5 + 1))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// RunOnce reserva um lote e publica. Devolve quantos eventos reservou.
func (r *OutboxRelay) RunOnce(ctx context.Context) int {
	if ctx.Err() != nil {
		return 0
	}
	claimedAt := time.Now()
	records, err := r.store.ClaimOutbox(ctx, r.owner, r.cfg.Lease, r.cfg.BatchSize)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("claiming outbox events failed", "error", err.Error())
		}
		return 0
	}

	// Margem: só começa a publicar um evento se a reserva ainda cobrir uma
	// publicação inteira; senão devolve o resto do lote.
	deadline := claimedAt.Add(r.cfg.Lease - r.cfg.PublishTimeout)
	if bp, ok := r.publisher.(BatchPublisher); ok {
		for start := 0; start < len(records); start += MaxPublishBatch {
			if ctx.Err() != nil || time.Now().After(deadline) {
				r.release(records[start:])
				break
			}
			r.publishBatch(bp, records[start:min(start+MaxPublishBatch, len(records))])
		}
		return len(records)
	}
	for i, rec := range records {
		if ctx.Err() != nil || time.Now().After(deadline) {
			r.release(records[i:])
			break
		}
		r.publish(rec)
	}
	return len(records)
}

// publishBatch publica um lote numa só chamada e marca os entregues numa só
// operação. Como em publish, usa um contexto próprio.
func (r *OutboxRelay) publishBatch(bp BatchPublisher, recs []OutboxRecord) {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.PublishTimeout)
	defer cancel()

	events := make([]event.Event, len(recs))
	for i, rec := range recs {
		events[i] = rec.Event
	}
	errs := bp.PublishBatch(ctx, events)

	var delivered []OutboxRecord
	for i, rec := range recs {
		var err error
		if i < len(errs) {
			err = errs[i]
		} else {
			err = errors.New("publisher returned no result for this event")
		}
		if err != nil {
			r.failed(ctx, rec, err)
			continue
		}
		delivered = append(delivered, rec)
	}
	if len(delivered) == 0 {
		return
	}

	ids := make([]uuid.UUID, len(delivered))
	for i, rec := range delivered {
		ids[i] = rec.Event.ID
	}
	if marker, ok := r.store.(OutboxBatchMarker); ok {
		if err := marker.MarkOutboxPublishedBatch(ctx, ids); err != nil {
			r.log.Warn("marking events as published failed, they may be republished",
				"error", err.Error(), "count", len(ids))
		}
	} else {
		for _, id := range ids {
			if err := r.store.MarkOutboxPublished(ctx, id); err != nil {
				r.log.Warn("marking event as published failed, it may be republished",
					"error", err.Error(), "eventId", id.String())
			}
		}
	}
	for _, rec := range delivered {
		r.onPublished(rec.Event, rec.Attempts+1)
	}
	r.log.Debug("event batch published", "count", len(delivered))
}

// failed registra a falha de publicação e agenda a próxima tentativa.
func (r *OutboxRelay) failed(ctx context.Context, rec OutboxRecord, err error) {
	e := rec.Event
	retryIn := r.backoff(rec.Attempts + 1)
	r.onFailed(e, err)
	log := r.log.With("eventId", e.ID.String(), "eventType", string(e.Type),
		"aggregateId", e.AggregateID.String(), "correlationId", e.CorrelationID)
	log.Warn("publishing event failed, will retry", "error", err.Error(),
		"attempt", rec.Attempts+1, "retryIn", retryIn.String())
	if mErr := r.store.MarkOutboxFailed(ctx, e.ID, r.owner, retryIn, err.Error()); mErr != nil {
		// O lease vence e o evento volta de qualquer forma.
		log.Warn("recording the publish failure failed", "error", mErr.Error())
	}
}

// publish usa um contexto próprio: um evento em andamento termina mesmo
// durante o encerramento.
func (r *OutboxRelay) publish(rec OutboxRecord) {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.PublishTimeout)
	defer cancel()
	e := rec.Event
	log := r.log.With("eventId", e.ID.String(), "eventType", string(e.Type),
		"aggregateId", e.AggregateID.String(), "correlationId", e.CorrelationID)

	if err := r.publisher.Publish(ctx, e); err != nil {
		r.failed(ctx, rec, err)
		return
	}
	if err := r.store.MarkOutboxPublished(ctx, e.ID); err != nil {
		// Já foi publicado: quando o lease vencer, será publicado de novo com o
		// mesmo eventId (entrega pelo menos uma vez).
		log.Warn("marking event as published failed, it may be republished", "error", err.Error())
	}
	r.onPublished(e, rec.Attempts+1)
	log.Debug("event published", "attempt", rec.Attempts+1)
}

func (r *OutboxRelay) release(records []OutboxRecord) {
	ids := make([]uuid.UUID, len(records))
	for i, rec := range records {
		ids[i] = rec.Event.ID
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.store.ReleaseOutbox(ctx, ids, r.owner); err != nil {
		r.log.Warn("releasing claimed events failed; they return when the lease expires", "error", err.Error())
		return
	}
	r.log.Info("released claimed events", "count", len(ids))
}

func (r *OutboxRelay) backoff(attempt int) time.Duration {
	d := r.cfg.BackoffBase
	for i := 1; i < attempt && d < r.cfg.BackoffMax; i++ {
		d *= 2
	}
	if d > r.cfg.BackoffMax {
		d = r.cfg.BackoffMax
	}
	return d
}
