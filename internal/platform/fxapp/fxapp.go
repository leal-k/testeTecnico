// Package fxapp compõe a aplicação com Uber Fx: configuração, conexões,
// repositórios, casos de uso, handlers e workers, com início e encerramento
// controlados pelo fx.Lifecycle.
//
// Ordem de encerramento (o Fx executa os OnStop na ordem inversa dos OnStart):
//  1. entradas: o servidor HTTP para de aceitar conexões e espera as
//     requisições em curso; o consumidor SQS para de buscar mensagens,
//     termina a que está tratando e devolve as outras à fila;
//  2. worker de referências pendentes: termina a transação em andamento;
//  3. pool do PostgreSQL: fechado por último, depois de quem o usa.
package fxapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/leal-k/testeTecnico/internal/adapters/auth"
	"github.com/leal-k/testeTecnico/internal/adapters/httpapi"
	"github.com/leal-k/testeTecnico/internal/adapters/postgres"
	sqsadapter "github.com/leal-k/testeTecnico/internal/adapters/sqs"
	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/platform/config"
)

// Options devolve a aplicação completa.
func Options(cfg config.Config) fx.Option {
	return fx.Options(
		fx.Supply(cfg),
		fx.Provide(NewLogger, NewWorkers),
		fx.WithLogger(func(l *slog.Logger) fxevent.Logger {
			// Eventos internos do Fx (cada hook executado) só em debug; os
			// erros do Fx continuam em error.
			fl := &fxevent.SlogLogger{Logger: l.With("component", "fx")}
			fl.UseLogLevel(slog.LevelDebug)
			return fl
		}),
		fx.StartTimeout(30*time.Second),
		fx.StopTimeout(cfg.ShutdownTimeout),

		DatabaseModule,
		AuthModule,
		UseCasesModule,
		// A ordem dos módulos define a ordem de encerramento (inversa):
		// HTTP, depois SQS, depois o worker de pendências, por fim o banco.
		WorkersModule,
		sqsModule(cfg),
		HTTPModule,
	)
}

func sqsModule(cfg config.Config) fx.Option {
	if !cfg.SQSEnabled {
		return fx.Options()
	}
	return SQSModule
}

// NewLogger cria o logger JSON da aplicação.
func NewLogger(cfg config.Config) *slog.Logger {
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.LogLevel))
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})).
		With("service", "testetecnico", "instanceId", cfg.InstanceID)
}

// ---- banco de dados ---------------------------------------------------------

var DatabaseModule = fx.Module("database",
	fx.Provide(
		NewPool,
		postgres.New,
		postgres.NewQueries,
		func(db *postgres.DB) app.TxManager { return db },
		func(db *postgres.DB) app.PendingLister { return db },
		func(q *postgres.Queries) app.Queries { return q },
		asHealthCheck(func(pool *pgxpool.Pool) httpapi.HealthCheck {
			return httpapi.HealthCheck{Name: "postgres", Check: pool.Ping}
		}),
	),
)

// asHealthCheck coloca a verificação no grupo "health", lido pelo readiness.
func asHealthCheck(f any) any {
	return fx.Annotate(f, fx.ResultTags(`group:"health"`))
}

// NewPool cria o pool de conexões. No início, confere que o banco responde
// (falha rápida); no encerramento, fecha as conexões.
func NewPool(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing DATABASE_URL: %w", err)
	}
	pc.MaxConns = cfg.DBMaxConns
	pool, err := pgxpool.NewWithConfig(context.Background(), pc)
	if err != nil {
		return nil, fmt.Errorf("creating database pool: %w", err)
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := pool.Ping(ctx); err != nil {
				return fmt.Errorf("database is not reachable: %w", err)
			}
			log.Info("database connected", "maxConns", cfg.DBMaxConns)
			return nil
		},
		OnStop: func(context.Context) error {
			pool.Close()
			log.Info("database pool closed")
			return nil
		},
	})
	return pool, nil
}

// ---- autenticação -----------------------------------------------------------

var AuthModule = fx.Module("auth",
	fx.Provide(
		NewVerifier,
		func(v *auth.Verifier) httpapi.Authenticator { return v },
	),
)

func NewVerifier(lc fx.Lifecycle, cfg config.Config) (*auth.Verifier, error) {
	// As chaves do IdP são buscadas com este contexto enquanto a aplicação vive.
	ctx, cancel := context.WithCancel(context.Background())
	lc.Append(fx.Hook{OnStop: func(context.Context) error { cancel(); return nil }})
	v, err := auth.NewVerifier(ctx, auth.Config{
		IssuerURL: cfg.AuthIssuerURL,
		JWKSURL:   cfg.AuthJWKSURL,
		Audience:  cfg.AuthAudience,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	return v, nil
}

// ---- casos de uso -----------------------------------------------------------

var UseCasesModule = fx.Module("usecases",
	fx.Provide(
		NewProcessor,
		NewWalletService,
		app.NewTransactionReader,
		func(p *app.Processor) httpapi.TransactionProcessor { return p },
		func(p *app.Processor) app.PendingRetrier { return p },
		func(s *app.WalletService) httpapi.Wallets { return s },
		func(r *app.TransactionReader) httpapi.Transactions { return r },
	),
)

func NewProcessor(tm app.TxManager, cfg config.Config) *app.Processor {
	return app.NewProcessor(tm, app.Config{
		ReferenceTTL: cfg.ReferenceTTL,
		BackoffBase:  cfg.ReferenceBackoffBase,
		BackoffMax:   cfg.ReferenceBackoffMax,
	}, nil, nil)
}

func NewWalletService(tm app.TxManager, q app.Queries, log *slog.Logger) *app.WalletService {
	s := app.NewWalletService(tm, q, nil, nil)
	s.OnDivergence(func(r app.Reconciliation) {
		log.Error("reconciliation divergence",
			"walletId", r.WalletID.String(),
			"storedBalance", r.Stored.Amount(),
			"calculatedBalance", r.Calculated.Amount(),
			"difference", r.Difference.Amount(),
			"currency", r.Stored.Currency().String(),
			"checkedEntries", r.CheckedEntries)
	})
	return s
}

// ---- workers ----------------------------------------------------------------

var WorkersModule = fx.Module("workers",
	fx.Invoke(func(lc fx.Lifecycle, ws *Workers, log *slog.Logger, cfg config.Config, lister app.PendingLister, retrier app.PendingRetrier) {
		w := app.NewPendingWorker(lister, retrier, cfg.PendingWorkerInterval, cfg.PendingBatchSize, log)
		StartWorker(lc, ws, log, "pending-references", w.Run)
	}),
)

// ---- SQS --------------------------------------------------------------------

var SQSModule = fx.Module("sqs",
	fx.Provide(
		NewSQSConsumer,
		asHealthCheck(func(c *sqsadapter.Consumer) httpapi.HealthCheck {
			return httpapi.HealthCheck{Name: "sqs", Check: c.Ready}
		}),
	),
	fx.Invoke(func(lc fx.Lifecycle, ws *Workers, log *slog.Logger, c *sqsadapter.Consumer) {
		StartWorker(lc, ws, log, "sqs-consumer", c.Run)
	}),
)

func NewSQSConsumer(cfg config.Config, log *slog.Logger, proc *app.Processor) (*sqsadapter.Consumer, error) {
	client, err := sqsadapter.NewClient(context.Background(), sqsadapter.ClientConfig{
		Region:   cfg.AWSRegion,
		Endpoint: cfg.SQSEndpointURL,
	})
	if err != nil {
		return nil, err
	}
	return sqsadapter.NewConsumer(client, proc, sqsadapter.Config{
		ConsumerName:      cfg.SQSConsumerName,
		QueueName:         cfg.SQSQueueName,
		DLQName:           cfg.SQSDLQName,
		WaitTime:          cfg.SQSWaitTime,
		MaxMessages:       int32(cfg.SQSMaxMessages),
		VisibilityTimeout: cfg.SQSVisibilityTimeout,
		HandleTimeout:     cfg.SQSHandleTimeout,
		MaxReceives:       cfg.SQSMaxReceives,
		RetryBase:         cfg.SQSRetryBase,
		RetryMax:          cfg.SQSRetryMax,
	}, log), nil
}

// ---- HTTP -------------------------------------------------------------------

var HTTPModule = fx.Module("http",
	fx.Provide(NewHTTPServer),
	fx.Invoke(func(*HTTPServer) {}),
)

// HTTPServer é o servidor HTTP com o endereço real em que está escutando
// (útil com a porta 0 nos testes).
type HTTPServer struct {
	srv  *http.Server
	addr atomic.Value
}

func (s *HTTPServer) Addr() string {
	if v, ok := s.addr.Load().(string); ok {
		return v
	}
	return ""
}

type httpParams struct {
	fx.In
	Lifecycle    fx.Lifecycle
	Config       config.Config
	Logger       *slog.Logger
	Checks       []httpapi.HealthCheck `group:"health"`
	Auth         httpapi.Authenticator
	Processor    httpapi.TransactionProcessor
	Wallets      httpapi.Wallets
	Transactions httpapi.Transactions
}

func NewHTTPServer(p httpParams) *HTTPServer {
	handler := httpapi.NewHandler(httpapi.Deps{
		Auth:           p.Auth,
		Processor:      p.Processor,
		Wallets:        p.Wallets,
		Transactions:   p.Transactions,
		Checks:         p.Checks,
		Logger:         p.Logger,
		RequestTimeout: p.Config.RequestTimeout,
	})
	hs := &HTTPServer{srv: &http.Server{
		Addr:              p.Config.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          slog.NewLogLogger(p.Logger.Handler(), slog.LevelWarn),
	}}

	p.Lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			// Abre a porta aqui (e não na goroutine) para um erro como "porta
			// em uso" impedir a aplicação de iniciar.
			ln, err := net.Listen("tcp", hs.srv.Addr)
			if err != nil {
				return fmt.Errorf("listening on %s: %w", hs.srv.Addr, err)
			}
			hs.addr.Store(ln.Addr().String())
			p.Logger.Info("http server listening", "addr", ln.Addr().String())
			go func() {
				if err := hs.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					p.Logger.Error("http server stopped unexpectedly", "error", err.Error())
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			p.Logger.Info("http server shutting down")
			// Para de aceitar conexões e espera as requisições em andamento.
			if err := hs.srv.Shutdown(ctx); err != nil {
				_ = hs.srv.Close() // prazo estourou: encerra à força
				return fmt.Errorf("http shutdown: %w", err)
			}
			p.Logger.Info("http server stopped")
			return nil
		},
	})
	return hs
}
