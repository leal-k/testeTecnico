package fxapp

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"go.uber.org/fx"
)

// Workers registra os workers em execução, para que o encerramento seja
// observável (logs e testes conferem que nenhum ficou rodando).
type Workers struct {
	mu      sync.Mutex
	running map[string]bool
}

func NewWorkers() *Workers {
	return &Workers{running: map[string]bool{}}
}

// Running devolve os nomes dos workers ainda em execução.
func (w *Workers) Running() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var names []string
	for name, on := range w.running {
		if on {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

func (w *Workers) set(name string, on bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.running[name] = on
}

// StartWorker liga run ao ciclo de vida da aplicação:
//   - OnStart: roda run em uma goroutine com um contexto próprio;
//   - OnStop: cancela esse contexto e espera run terminar, até o prazo de
//     encerramento. Se o prazo estourar, o encerramento reporta o erro.
func StartWorker(lc fx.Lifecycle, ws *Workers, log *slog.Logger, name string, run func(ctx context.Context)) {
	var (
		cancel context.CancelFunc
		done   chan struct{}
	)
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			// O contexto de OnStart vale só durante a inicialização; o worker
			// precisa de um que dure até o OnStop.
			var ctx context.Context
			ctx, cancel = context.WithCancel(context.Background())
			done = make(chan struct{})
			ws.set(name, true)
			go func() {
				defer close(done)
				defer ws.set(name, false)
				run(ctx)
			}()
			return nil
		},
		OnStop: func(stopCtx context.Context) error {
			log.Info("stopping worker", "worker", name)
			cancel()
			select {
			case <-done:
				return nil
			case <-stopCtx.Done():
				return fmt.Errorf("worker %s did not stop before the shutdown deadline", name)
			}
		},
	})
}
