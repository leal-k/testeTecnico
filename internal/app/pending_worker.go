package app

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
)

// PendingRetrier reavalia uma transação em PENDING_REFERENCE.
type PendingRetrier interface {
	RetryPending(ctx context.Context, id uuid.UUID) error
}

// PendingWorker busca periodicamente as transações em PENDING_REFERENCE cuja
// próxima tentativa venceu e as reavalia. O estado fica todo no banco, então
// qualquer instância retoma o trabalho de outra que tenha parado, inclusive
// depois de um reinício. Várias instâncias podem rodar ao mesmo tempo: o lock
// da carteira garante que cada pendência é concluída uma única vez.
type PendingWorker struct {
	lister    PendingLister
	retrier   PendingRetrier
	interval  time.Duration
	batchSize int
	log       *slog.Logger
	now       func() time.Time
}

func NewPendingWorker(lister PendingLister, retrier PendingRetrier, interval time.Duration, batchSize int, log *slog.Logger) *PendingWorker {
	if log == nil {
		log = slog.Default()
	}
	return &PendingWorker{
		lister:    lister,
		retrier:   retrier,
		interval:  interval,
		batchSize: batchSize,
		log:       log.With("worker", "pending-references"),
		now:       func() time.Time { return time.Now().UTC() },
	}
}

// Run executa até ctx ser cancelado. Uma rodada em andamento termina a
// transação atual antes de sair (cada retry é uma transação curta e atômica).
func (w *PendingWorker) Run(ctx context.Context) {
	w.log.Info("worker started", "interval", w.interval.String())
	defer w.log.Info("worker stopped")

	for {
		processed := w.RunOnce(ctx)
		// Lote cheio: provavelmente há mais pendências vencidas; segue sem esperar.
		wait := w.interval
		if processed >= w.batchSize {
			wait = 0
		}
		// Pequena variação aleatória para as instâncias não baterem juntas.
		if wait > 0 {
			wait += time.Duration(rand.Int64N(int64(w.interval)/5 + 1))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// RunOnce processa um lote e devolve quantas pendências tentou.
func (w *PendingWorker) RunOnce(ctx context.Context) int {
	ids, err := w.lister.ListDuePending(ctx, w.now(), w.batchSize)
	if err != nil {
		if ctx.Err() == nil {
			w.log.Warn("listing due pending transactions failed", "error", err.Error())
		}
		return 0
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return 0
		}
		if err := w.retrier.RetryPending(ctx, id); err != nil && !errors.Is(err, context.Canceled) {
			// Falha transitória: a transação continua pendente e volta na próxima rodada.
			w.log.Warn("retrying pending transaction failed", "transactionId", id.String(), "error", err.Error())
		}
	}
	return len(ids)
}
