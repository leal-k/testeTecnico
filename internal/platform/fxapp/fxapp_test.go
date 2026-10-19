package fxapp_test

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
	"github.com/leal-k/testeTecnico/internal/platform/config"
	"github.com/leal-k/testeTecnico/internal/platform/fxapp"
	"github.com/leal-k/testeTecnico/internal/testsupport/pgtest"
)

var dsn string

func TestMain(m *testing.M) {
	flag.Parse()
	// Só os testes de ciclo de vida precisam do banco; os demais rodam sempre.
	if !testing.Short() {
		ctx := context.Background()
		d, stop, err := pgtest.Start(ctx)
		if err == nil {
			dsn = d
			code := m.Run()
			stop()
			os.Exit(code)
		}
	}
	os.Exit(m.Run())
}

func testConfig(databaseURL string) config.Config {
	return config.Config{
		InstanceID:            "test",
		LogLevel:              "error",
		HTTPAddr:              "127.0.0.1:0", // porta livre qualquer
		RequestTimeout:        5 * time.Second,
		ShutdownTimeout:       10 * time.Second,
		DatabaseURL:           databaseURL,
		DBMaxConns:            5,
		AuthIssuerURL:         "http://127.0.0.1:1/realms/testetecnico",
		AuthJWKSURL:           "http://127.0.0.1:1/realms/testetecnico/protocol/openid-connect/certs",
		AuthAudience:          "testetecnico-api",
		PendingWorkerInterval: 20 * time.Millisecond,
		PendingBatchSize:      10,
		ReferenceTTL:          time.Minute,
		ReferenceBackoffBase:  10 * time.Millisecond,
		ReferenceBackoffMax:   20 * time.Millisecond,
	}
}

func requireDB(t *testing.T) {
	t.Helper()
	if testing.Short() || dsn == "" {
		t.Skip("needs Docker for the PostgreSQL container")
	}
}

// O grafo de dependências fecha (nada faltando, nada ambíguo), sem conectar em nada.
func TestGraphIsValid(t *testing.T) {
	if err := fx.ValidateApp(fxapp.Options(testConfig("postgres://app:app@127.0.0.1:1/x"))); err != nil {
		t.Fatalf("invalid dependency graph: %v", err)
	}
}

func TestGraphIsValid_WithTheSQSConsumer(t *testing.T) {
	cfg := testConfig("postgres://app:app@127.0.0.1:1/x")
	cfg.SQSEnabled = true
	cfg.AWSRegion = "us-east-1"
	cfg.SQSEndpointURL = "http://127.0.0.1:1"
	cfg.SQSQueueName = "wager-transactions.fifo"
	cfg.SQSDLQName = "wager-transactions-dlq.fifo"
	cfg.OutboxEnabled = true
	cfg.SQSEventsQueueName = "wager-events.fifo"
	if err := fx.ValidateApp(fxapp.Options(cfg)); err != nil {
		t.Fatalf("invalid dependency graph with SQS and the outbox publisher: %v", err)
	}
}

func TestStartFailsFastWhenTheDatabaseIsUnreachable(t *testing.T) {
	application := fx.New(fxapp.Options(testConfig("postgres://app:app@127.0.0.1:1/x?sslmode=disable&connect_timeout=2")))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := application.Start(ctx); err == nil {
		_ = application.Stop(ctx)
		t.Fatal("the application started without a database")
	}
}

func TestLifecycle_StartsServesAndReleasesEverythingOnStop(t *testing.T) {
	requireDB(t)
	var (
		hs   *fxapp.HTTPServer
		ws   *fxapp.Workers
		pool *pgxpool.Pool
	)
	application := fxtest.New(t, fxapp.Options(testConfig(dsn)), fx.Populate(&hs, &ws, &pool))
	application.RequireStart()

	base := "http://" + hs.Addr()
	if code := get(t, base+"/health/live"); code != http.StatusOK {
		t.Errorf("live = %d", code)
	}
	resp, err := http.Get(base + "/health/ready")
	if err != nil {
		t.Fatal(err)
	}
	var ready struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&ready)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || ready.Checks["postgres"] != "ok" {
		t.Errorf("ready = %d %+v", resp.StatusCode, ready)
	}
	// rota de negócio protegida pela autenticação
	if code := get(t, base+"/wallets/"+uuid.NewString()); code != http.StatusUnauthorized {
		t.Errorf("business route without a token = %d, want 401", code)
	}
	if running := ws.Running(); !slices.Equal(running, []string{"pending-references"}) {
		t.Errorf("running workers = %v", running)
	}

	application.RequireStop()

	if running := ws.Running(); len(running) != 0 {
		t.Errorf("workers still running after stop: %v", running)
	}
	client := &http.Client{Timeout: time.Second}
	if resp, err := client.Get(base + "/health/live"); err == nil {
		resp.Body.Close()
		t.Error("the HTTP server still answers after stop")
	}
	if err := pool.Ping(context.Background()); err == nil {
		t.Error("the database pool is still open after stop")
	}
}

// Uma pendência registrada por uma instância é concluída por OUTRA instância,
// iniciada depois que a primeira parou (estado 100% no banco).
func TestLifecycle_AnotherInstanceResumesPendingWork(t *testing.T) {
	requireDB(t)
	ctx := context.Background()
	brl := func(s string) money.Money {
		m, err := money.Parse(s, "BRL")
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	provider := "provider-" + uuid.NewString()

	// Instância 1: abre a carteira e recebe um REFUND antes da aposta.
	var (
		wallets1 *app.WalletService
		proc1    *app.Processor
	)
	first := fxtest.New(t, fxapp.Options(testConfig(dsn)), fx.Populate(&wallets1, &proc1))
	first.RequireStart()
	w, err := wallets1.Open(ctx, app.Caller{Internal: true}, app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: brl("100.00")})
	if err != nil {
		t.Fatal(err)
	}
	cmd := func(kind wager.Kind, amount, ext, key, ref string) app.ProcessCommand {
		return app.ProcessCommand{
			ProviderID: provider, IdempotencyKey: key, ExternalID: ext, Kind: kind,
			WalletID: w.ID(), PlayerID: w.PlayerID(), RoundID: "r1", GameID: "g1",
			Amount: brl(amount), ReferenceExternalID: ref, CorrelationID: key,
		}
	}
	refund, err := proc1.Process(ctx, cmd(wager.KindRefund, "40.00", "refund-1", "k-refund", "bet-1"))
	if err != nil || refund.Status != wager.StatusPendingReference {
		t.Fatalf("refund = %+v, %v", refund, err)
	}
	first.RequireStop()

	// Instância 2: nova, sem nada em memória. A aposta chega e o worker dela
	// conclui o REFUND registrado pela instância 1.
	var (
		proc2 *app.Processor
		pool  *pgxpool.Pool
	)
	second := fxtest.New(t, fxapp.Options(testConfig(dsn)), fx.Populate(&proc2, &pool))
	second.RequireStart()
	defer second.RequireStop()

	if bet, err := proc2.Process(ctx, cmd(wager.KindBet, "40.00", "bet-1", "k-bet", "")); err != nil || bet.Status != wager.StatusProcessed {
		t.Fatalf("bet = %+v, %v", bet, err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM wager_transactions WHERE id = $1`, refund.TransactionID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(wager.StatusProcessed) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the refund is still %s; the second instance did not resume it", status)
		}
		time.Sleep(50 * time.Millisecond)
	}

	var balance int64
	if err := pool.QueryRow(ctx, `SELECT balance_minor FROM wallets WHERE id = $1`, w.ID()).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 10000 {
		t.Errorf("balance = %d, want 10000 (bet of 40.00 refunded once)", balance)
	}
}

func get(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}
