package postgres_test

// Testes de integração: rodam contra um PostgreSQL REAL (container via
// testcontainers) com o schema aplicado pelas migrations. Exigem Docker; sem
// Docker, ou com `go test -short`, são ignorados.
//
// A concorrência é exercitada com 3 "instâncias" independentes da aplicação:
// cada uma tem o próprio pool de conexões e o próprio Processor, e a única
// coisa que compartilham é o banco.

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	testetecnico "github.com/leal-k/testeTecnico"
	"github.com/leal-k/testeTecnico/internal/adapters/postgres"
	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/event"
	"github.com/leal-k/testeTecnico/internal/domain/ledger"
	"github.com/leal-k/testeTecnico/internal/domain/money"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
)

var (
	dsn        string
	skipReason string
	errBoom    = errors.New("boom")
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(run(m))
}

func run(m *testing.M) int {
	if testing.Short() {
		skipReason = "integration tests are skipped with -short"
		return m.Run()
	}

	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithDatabase("testetecnico_test"),
		tcpostgres.WithUsername("app"),
		tcpostgres.WithPassword("app"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second)),
	)
	if container != nil {
		defer func() { _ = container.Terminate(context.Background()) }()
	}
	if err != nil {
		skipReason = fmt.Sprintf("could not start the PostgreSQL container (is Docker running?): %v", err)
		return m.Run()
	}

	dsn, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection string: %v\n", err)
		return 1
	}
	if err := migrate(ctx, dsn); err != nil {
		fmt.Fprintf(os.Stderr, "migrations: %v\n", err)
		return 1
	}
	return m.Run()
}

func migrate(ctx context.Context, dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	migrations, err := fs.Sub(testetecnico.MigrationsFS, "migrations")
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations)
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}

// ---- ambiente de teste ------------------------------------------------------

type env struct {
	pool  *pgxpool.Pool // asserções e dados de teste
	dbs   []*postgres.DB
	procs []*app.Processor // 3 instâncias independentes
}

func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	cfg.MaxConns = 10
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func newEnv(t *testing.T) *env {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	e := &env{pool: newPool(t)}
	for i := 0; i < 3; i++ {
		db := postgres.New(newPool(t))
		e.dbs = append(e.dbs, db)
		e.procs = append(e.procs, app.NewProcessor(db, app.DefaultConfig(), nil, nil))
	}
	return e
}

type testWallet struct {
	id       uuid.UUID
	player   uuid.UUID
	provider string
}

func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	if err != nil {
		t.Fatalf("Parse(%q): %v", amount, err)
	}
	return m
}

// newWallet cria uma carteira com saldo inicial, a transação OPENING e o
// lançamento de crédito correspondente, como a abertura real faria. Cada teste
// usa carteira e provedor próprios, então os testes não se interferem.
func (e *env) newWallet(t *testing.T, balance string) testWallet {
	t.Helper()
	ctx := context.Background()
	minor := mustMoney(t, balance).Minor()
	w := testWallet{id: uuid.New(), player: uuid.New(), provider: "provider-" + uuid.NewString()}

	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	openingID := uuid.New()
	mustExec(t, tx, `INSERT INTO wallets (id, player_id, currency, balance_minor) VALUES ($1, $2, 'BRL', $3)`,
		w.id, w.player, minor)
	mustExec(t, tx, `
		INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, currency, amount_minor, result_balance_minor)
		VALUES ($1, 'INTERNAL', 'OPENING', 'PROCESSED', $2, $3, 'BRL', $4, $4)`,
		openingID, w.id, w.player, minor)
	mustExec(t, tx, `
		INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before_minor, balance_after_minor)
		VALUES ($1, $2, $3, 'CREDIT', $4, 'BRL', 0, $4)`,
		uuid.New(), w.id, openingID, minor)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return w
}

func mustExec(t *testing.T, tx pgx.Tx, sql string, args ...any) {
	t.Helper()
	if _, err := tx.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec failed: %v\n%s", err, sql)
	}
}

func (w testWallet) cmd(t *testing.T, kind wager.Kind, amount, ext, key string) app.ProcessCommand {
	t.Helper()
	return app.ProcessCommand{
		ProviderID:     w.provider,
		IdempotencyKey: key,
		ExternalID:     ext,
		Kind:           kind,
		WalletID:       w.id,
		PlayerID:       w.player,
		RoundID:        "round-1",
		GameID:         "game-1",
		Amount:         mustMoney(t, amount),
		CorrelationID:  "corr-" + key,
	}
}

func (w testWallet) refCmd(t *testing.T, kind wager.Kind, amount, ext, key, refExt string) app.ProcessCommand {
	t.Helper()
	c := w.cmd(t, kind, amount, ext, key)
	c.ReferenceExternalID = refExt
	return c
}

// process roda o comando em uma das 3 instâncias.
func (e *env) process(t *testing.T, instance int, cmd app.ProcessCommand) app.Result {
	t.Helper()
	res, err := e.procs[instance%len(e.procs)].Process(context.Background(), cmd)
	if err != nil {
		t.Fatalf("Process(%s %s): %v", cmd.Kind, cmd.ExternalID, err)
	}
	return res
}

func (e *env) balance(t *testing.T, w testWallet) (string, int64) {
	t.Helper()
	var minor, version int64
	err := e.pool.QueryRow(context.Background(),
		`SELECT balance_minor, version FROM wallets WHERE id = $1`, w.id).Scan(&minor, &version)
	if err != nil {
		t.Fatalf("reading wallet: %v", err)
	}
	m, err := money.FromMinor(minor, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m.Amount(), version
}

func (e *env) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count failed: %v\n%s", err, query)
	}
	return n
}

// assertLedgerConsistent confere a invariante central: o saldo armazenado é
// exatamente a soma dos lançamentos do ledger.
func (e *env) assertLedgerConsistent(t *testing.T, w testWallet) {
	t.Helper()
	var stored, net int64
	err := e.pool.QueryRow(context.Background(), `
		SELECT w.balance_minor,
		       COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount_minor ELSE -l.amount_minor END), 0)::bigint
		FROM wallets w
		LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
		WHERE w.id = $1
		GROUP BY w.balance_minor`, w.id).Scan(&stored, &net)
	if err != nil {
		t.Fatalf("ledger check: %v", err)
	}
	if stored != net {
		t.Errorf("stored balance %d differs from the ledger sum %d", stored, net)
	}
}

func runParallel(n int, fn func(i int)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // todos largam juntos, para maximizar a disputa
			fn(i)
		}(i)
	}
	close(start)
	wg.Wait()
}

type outcome struct {
	res app.Result
	err error
}

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// ---- testes -----------------------------------------------------------------

func TestIntegration_Bet_PersistsEverythingAtomically(t *testing.T) {
	e := newEnv(t)
	w := e.newWallet(t, "100.00")

	res := e.process(t, 0, w.cmd(t, wager.KindBet, "80.00", "ext-1", "key-1"))

	if res.Status != wager.StatusProcessed || res.Replay || res.Balance.Amount() != "20.00" {
		t.Errorf("result = %+v", res)
	}
	if bal, version := e.balance(t, w); bal != "20.00" || version != 2 {
		t.Errorf("wallet = %s v%d, want 20.00 v2", bal, version)
	}
	if n := e.count(t, `SELECT count(*) FROM wallet_ledger_entries WHERE transaction_id = $1`, res.TransactionID); n != 1 {
		t.Errorf("ledger entries for the transaction = %d, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = $2`,
		res.TransactionID, string(event.TypeWagerTransactionProcessed)); n != 1 {
		t.Errorf("processed events = %d, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = $2`,
		w.id, string(event.TypeWalletBalanceChanged)); n != 1 {
		t.Errorf("balance-changed events = %d, want 1", n)
	}
	e.assertLedgerConsistent(t, w)
}

func TestIntegration_Replay_ReturnsTheStoredResult(t *testing.T) {
	e := newEnv(t)
	w := e.newWallet(t, "100.00")
	cmd := w.cmd(t, wager.KindBet, "80.00", "ext-1", "key-1")

	first := e.process(t, 0, cmd)
	second := e.process(t, 1, cmd) // outra instância

	if !second.Replay || second.TransactionID != first.TransactionID || second.Balance.Amount() != "20.00" {
		t.Errorf("replay = %+v, first = %+v", second, first)
	}
	if bal, version := e.balance(t, w); bal != "20.00" || version != 2 {
		t.Errorf("replay changed the wallet: %s v%d", bal, version)
	}

	changed := cmd
	changed.Amount = mustMoney(t, "81.00")
	if _, err := e.procs[2].Process(context.Background(), changed); !errors.Is(err, app.ErrIdempotencyConflict) {
		t.Errorf("same key with another payload: error = %v, want ErrIdempotencyConflict", err)
	}
}

// Cenário do README: 50 requisições idênticas em paralelo, um único débito.
func TestIntegration_FiftyIdenticalRequests_DebitExactlyOnce(t *testing.T) {
	e := newEnv(t)
	w := e.newWallet(t, "100.00")
	cmd := w.cmd(t, wager.KindBet, "10.00", "ext-1", "key-1")

	results := make([]outcome, 50)
	runParallel(len(results), func(i int) {
		res, err := e.procs[i%len(e.procs)].Process(context.Background(), cmd)
		results[i] = outcome{res, err}
	})

	fresh := 0
	var txID uuid.UUID
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("request %d failed: %v", i, r.err)
		}
		if r.res.Status != wager.StatusProcessed {
			t.Errorf("request %d status = %s", i, r.res.Status)
		}
		if txID == uuid.Nil {
			txID = r.res.TransactionID
		}
		if r.res.TransactionID != txID {
			t.Errorf("request %d returned another transaction id", i)
		}
		if !r.res.Replay {
			fresh++
		}
	}
	if fresh != 1 {
		t.Errorf("%d requests executed the operation, want exactly 1", fresh)
	}
	if bal, version := e.balance(t, w); bal != "90.00" || version != 2 {
		t.Errorf("wallet = %s v%d, want 90.00 v2 (a single debit)", bal, version)
	}
	if n := e.count(t, `SELECT count(*) FROM wager_transactions WHERE provider_id = $1`, w.provider); n != 1 {
		t.Errorf("stored transactions = %d, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.id); n != 1 {
		t.Errorf("debit entries = %d, want 1", n)
	}
	e.assertLedgerConsistent(t, w)
}

// Cenário do README: 100.00 com duas apostas simultâneas de 80.00.
func TestIntegration_TwoSimultaneousBetsOfEightyOnHundred(t *testing.T) {
	e := newEnv(t)

	for round := 0; round < 15; round++ {
		w := e.newWallet(t, "100.00")

		results := make([]outcome, 2)
		runParallel(2, func(i int) {
			cmd := w.cmd(t, wager.KindBet, "80.00", fmt.Sprintf("ext-%d", i), fmt.Sprintf("key-%d", i))
			res, err := e.procs[i].Process(context.Background(), cmd)
			results[i] = outcome{res, err}
		})

		processed, rejected := 0, 0
		for _, r := range results {
			if r.err != nil {
				t.Fatalf("round %d: %v", round, r.err)
			}
			switch {
			case r.res.Status == wager.StatusProcessed:
				processed++
			case r.res.Status == wager.StatusRejected && r.res.FailureCode == wager.FailureInsufficientFunds:
				rejected++
			}
		}
		if processed != 1 || rejected != 1 {
			t.Fatalf("round %d: processed=%d rejected=%d, want 1 and 1", round, processed, rejected)
		}
		if bal, _ := e.balance(t, w); bal != "20.00" {
			t.Fatalf("round %d: balance = %s, want 20.00", round, bal)
		}
		if n := e.count(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.id); n != 1 {
			t.Fatalf("round %d: debit entries = %d, want 1", round, n)
		}
		e.assertLedgerConsistent(t, w)
	}
}

func TestIntegration_ManyConcurrentBets_NeverOverdraw(t *testing.T) {
	e := newEnv(t)
	w := e.newWallet(t, "100.00")

	results := make([]outcome, 25)
	runParallel(len(results), func(i int) {
		cmd := w.cmd(t, wager.KindBet, "10.00", fmt.Sprintf("ext-%d", i), fmt.Sprintf("key-%d", i))
		res, err := e.procs[i%len(e.procs)].Process(context.Background(), cmd)
		results[i] = outcome{res, err}
	})

	processed, rejected := 0, 0
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("request %d: %v", i, r.err)
		}
		switch {
		case r.res.Status == wager.StatusProcessed:
			processed++
		case r.res.Status == wager.StatusRejected && r.res.FailureCode == wager.FailureInsufficientFunds:
			rejected++
		default:
			t.Errorf("request %d: unexpected result %+v", i, r.res)
		}
	}
	if processed != 10 || rejected != 15 {
		t.Errorf("processed=%d rejected=%d, want 10 and 15", processed, rejected)
	}
	if bal, version := e.balance(t, w); bal != "0.00" || version != 11 {
		t.Errorf("wallet = %s v%d, want 0.00 v11", bal, version)
	}
	e.assertLedgerConsistent(t, w)
}

func TestIntegration_DifferentWalletsDoNotInterfere(t *testing.T) {
	e := newEnv(t)
	const wallets, betsPerWallet = 20, 5

	ws := make([]testWallet, wallets)
	for i := range ws {
		ws[i] = e.newWallet(t, "100.00")
	}

	// 100 requisições simultâneas: 5 apostas de 30.00 em cada uma de 20 carteiras.
	results := make([]outcome, wallets*betsPerWallet)
	runParallel(len(results), func(i int) {
		w := ws[i/betsPerWallet]
		cmd := w.cmd(t, wager.KindBet, "30.00", fmt.Sprintf("ext-%d", i), fmt.Sprintf("key-%d", i))
		res, err := e.procs[i%len(e.procs)].Process(context.Background(), cmd)
		results[i] = outcome{res, err}
	})

	for i, r := range results {
		if r.err != nil {
			t.Fatalf("request %d: %v", i, r.err)
		}
	}
	for i, w := range ws {
		// 100.00 comporta 3 apostas de 30.00; as outras 2 são recusadas
		if bal, version := e.balance(t, w); bal != "10.00" || version != 4 {
			t.Errorf("wallet %d = %s v%d, want 10.00 v4", i, bal, version)
		}
		e.assertLedgerConsistent(t, w)
	}
}

func TestIntegration_ConcurrentReversalsOfTheSameBet_OnlyOneWins(t *testing.T) {
	e := newEnv(t)

	for round := 0; round < 10; round++ {
		w := e.newWallet(t, "100.00")
		e.process(t, 0, w.cmd(t, wager.KindBet, "80.00", "bet-1", "key-bet"))

		results := make([]outcome, 2)
		runParallel(2, func(i int) {
			kind := wager.KindRefund
			if i == 1 {
				kind = wager.KindRollback
			}
			cmd := w.refCmd(t, kind, "80.00", fmt.Sprintf("rev-%d", i), fmt.Sprintf("key-rev-%d", i), "bet-1")
			res, err := e.procs[i].Process(context.Background(), cmd)
			results[i] = outcome{res, err}
		})

		processed, alreadyReversed := 0, 0
		for _, r := range results {
			if r.err != nil {
				t.Fatalf("round %d: %v", round, r.err)
			}
			switch {
			case r.res.Status == wager.StatusProcessed:
				processed++
			case r.res.Status == wager.StatusRejected && r.res.FailureCode == wager.FailureAlreadyReversed:
				alreadyReversed++
			}
		}
		if processed != 1 || alreadyReversed != 1 {
			t.Fatalf("round %d: processed=%d alreadyReversed=%d, want 1 and 1", round, processed, alreadyReversed)
		}
		if bal, _ := e.balance(t, w); bal != "100.00" {
			t.Fatalf("round %d: balance = %s, want 100.00 (the bet is given back exactly once)", round, bal)
		}
		e.assertLedgerConsistent(t, w)
	}
}

func TestIntegration_PendingReference_ResolvedOnceEvenWithConcurrentRetries(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	w := e.newWallet(t, "100.00")

	// O REFUND chega antes da aposta que ele referencia.
	pending := e.process(t, 0, w.refCmd(t, wager.KindRefund, "80.00", "refund-1", "key-refund", "bet-1"))
	if pending.Status != wager.StatusPendingReference {
		t.Fatalf("status = %s, want PENDING_REFERENCE", pending.Status)
	}

	due, err := e.dbs[0].ListDuePending(ctx, time.Now().Add(time.Hour), 1000)
	if err != nil {
		t.Fatalf("ListDuePending: %v", err)
	}
	found := false
	for _, id := range due {
		if id == pending.TransactionID {
			found = true
		}
	}
	if !found {
		t.Errorf("the pending transaction is not listed as due")
	}

	// A aposta chega. Três instâncias tentam concluir o REFUND ao mesmo tempo.
	e.process(t, 1, w.cmd(t, wager.KindBet, "80.00", "bet-1", "key-bet"))
	runParallel(3, func(i int) {
		if err := e.procs[i].RetryPending(ctx, pending.TransactionID); err != nil {
			t.Errorf("RetryPending on instance %d: %v", i, err)
		}
	})

	var status string
	if err := e.pool.QueryRow(ctx, `SELECT status FROM wager_transactions WHERE id = $1`, pending.TransactionID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(wager.StatusProcessed) {
		t.Errorf("refund status = %s, want PROCESSED", status)
	}
	if bal, version := e.balance(t, w); bal != "100.00" || version != 3 {
		t.Errorf("wallet = %s v%d, want 100.00 v3 (credited once)", bal, version)
	}
	// OPENING + aposta + refund = 3 lançamentos
	if n := e.count(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, w.id); n != 3 {
		t.Errorf("ledger entries = %d, want 3", n)
	}
	e.assertLedgerConsistent(t, w)
}

// failingLedgerStore faz a gravação do ledger falhar DEPOIS de a transação e o
// saldo terem sido gravados, para provar que o rollback desfaz tudo.
type failingLedgerStore struct {
	app.Store
}

func (failingLedgerStore) InsertLedgerEntry(context.Context, ledger.Entry) error {
	return errBoom
}

type failingTxManager struct {
	inner *postgres.DB
}

func (f failingTxManager) WithinTx(ctx context.Context, fn func(ctx context.Context, s app.Store) error) error {
	return f.inner.WithinTx(ctx, func(ctx context.Context, s app.Store) error {
		return fn(ctx, failingLedgerStore{s})
	})
}

func TestIntegration_TechnicalFailure_RollsBackEverything(t *testing.T) {
	e := newEnv(t)
	w := e.newWallet(t, "100.00")
	cmd := w.cmd(t, wager.KindBet, "80.00", "ext-1", "key-1")

	broken := app.NewProcessor(failingTxManager{inner: e.dbs[0]}, app.DefaultConfig(), nil, nil)
	if _, err := broken.Process(context.Background(), cmd); !errors.Is(err, errBoom) {
		t.Fatalf("error = %v, want %v", err, errBoom)
	}

	if bal, version := e.balance(t, w); bal != "100.00" || version != 1 {
		t.Errorf("wallet = %s v%d, want 100.00 v1 after the rollback", bal, version)
	}
	if n := e.count(t, `SELECT count(*) FROM wager_transactions WHERE provider_id = $1`, w.provider); n != 0 {
		t.Errorf("the failed operation left %d transaction rows behind", n)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1`, w.id); n != 0 {
		t.Errorf("the failed operation left %d outbox events behind", n)
	}

	// Sem a falha, o mesmo comando é processado normalmente.
	res := e.process(t, 0, cmd)
	if res.Status != wager.StatusProcessed || res.Replay {
		t.Errorf("retry after the failure = %+v", res)
	}
	e.assertLedgerConsistent(t, w)
}

func TestIntegration_InvalidRequests_PersistNothing(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	w := e.newWallet(t, "100.00")

	unknown := w.cmd(t, wager.KindBet, "10.00", "ext-1", "key-1")
	unknown.WalletID = uuid.New()
	if _, err := e.procs[0].Process(ctx, unknown); !errors.Is(err, app.ErrWalletNotFound) {
		t.Errorf("unknown wallet: error = %v, want ErrWalletNotFound", err)
	}

	otherPlayer := w.cmd(t, wager.KindBet, "10.00", "ext-2", "key-2")
	otherPlayer.PlayerID = uuid.New()
	if _, err := e.procs[0].Process(ctx, otherPlayer); !errors.Is(err, app.ErrWalletPlayerMismatch) {
		t.Errorf("another player: error = %v, want ErrWalletPlayerMismatch", err)
	}

	if n := e.count(t, `SELECT count(*) FROM wager_transactions WHERE provider_id = $1`, w.provider); n != 0 {
		t.Errorf("invalid requests stored %d transactions", n)
	}
}

// ---- garantias que o próprio banco impõe -----------------------------------

func TestIntegration_Database_EnforcesTheInvariantsByItself(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	w := e.newWallet(t, "100.00")
	e.process(t, 0, w.cmd(t, wager.KindBet, "10.00", "ext-1", "key-1"))

	tests := []struct {
		name     string
		sql      string
		args     []any
		wantCode string
	}{
		{"negative balance", `UPDATE wallets SET balance_minor = -1, version = version + 1 WHERE id = $1`, []any{w.id}, "23514"},
		{"balance change without a version bump", `UPDATE wallets SET balance_minor = balance_minor - 1 WHERE id = $1`, []any{w.id}, "23000"},
		{"ledger update", `UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE wallet_id = $1`, []any{w.id}, "23000"},
		{"ledger delete", `DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, []any{w.id}, "23000"},
		{"ledger truncate", `TRUNCATE wallet_ledger_entries`, nil, "23000"},
		{"outbox payload rewrite", `UPDATE outbox_events SET payload = '{}'::jsonb WHERE aggregate_id = $1`, []any{w.id}, "23000"},
		{"terminal transaction change", `UPDATE wager_transactions SET status = 'PENDING' WHERE provider_id = $1`, []any{w.provider}, "23000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := e.pool.Exec(ctx, tc.sql, tc.args...)
			if err == nil {
				t.Fatalf("the database accepted: %s", tc.sql)
			}
			if code := pgCode(err); code != tc.wantCode {
				t.Errorf("SQLSTATE = %q, want %q (%v)", code, tc.wantCode, err)
			}
		})
	}

	if bal, version := e.balance(t, w); bal != "90.00" || version != 2 {
		t.Errorf("rejected statements changed the wallet: %s v%d", bal, version)
	}
	e.assertLedgerConsistent(t, w)
}

func TestIntegration_Store_ReportsNotFound(t *testing.T) {
	e := newEnv(t)
	err := e.dbs[0].WithinTx(context.Background(), func(ctx context.Context, s app.Store) error {
		if _, err := s.FindTransactionByKey(ctx, "nobody", "nothing"); !errors.Is(err, app.ErrNotFound) {
			t.Errorf("FindTransactionByKey: error = %v, want ErrNotFound", err)
		}
		if _, err := s.GetTransaction(ctx, uuid.New()); !errors.Is(err, app.ErrNotFound) {
			t.Errorf("GetTransaction: error = %v, want ErrNotFound", err)
		}
		if _, err := s.LockWallet(ctx, uuid.New()); !errors.Is(err, app.ErrNotFound) {
			t.Errorf("LockWallet: error = %v, want ErrNotFound", err)
		}
		ref, err := s.ResolveReference(ctx, "nobody", "nothing")
		if err != nil || ref != nil {
			t.Errorf("ResolveReference = %v, %v; want nil, nil", ref, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
