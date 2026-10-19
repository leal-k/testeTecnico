package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/leal-k/testeTecnico/internal/adapters/postgres"
	"github.com/leal-k/testeTecnico/internal/app"
	"github.com/leal-k/testeTecnico/internal/domain/event"
	"github.com/leal-k/testeTecnico/internal/domain/wager"
)

var internalCaller = app.Caller{Internal: true}

func (e *env) walletService(t *testing.T) *app.WalletService {
	t.Helper()
	return app.NewWalletService(e.dbs[0], postgres.NewQueries(e.pool), nil, nil)
}

func (e *env) reader() *app.TransactionReader {
	return app.NewTransactionReader(postgres.NewQueries(e.pool))
}

func TestIntegration_OpenWallet_WithBalance_WritesOpeningLedgerAndEventsTogether(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	svc := e.walletService(t)

	w, err := svc.Open(ctx, internalCaller, app.OpenWalletCommand{
		PlayerID:       uuid.New(),
		InitialBalance: mustMoney(t, "1000.00"),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if w.Balance().Amount() != "1000.00" || w.Version() != 1 {
		t.Errorf("wallet = %s v%d, want 1000.00 v1", w.Balance().Amount(), w.Version())
	}

	if n := e.count(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING' AND status = 'PROCESSED' AND origin = 'INTERNAL'`, w.ID()); n != 1 {
		t.Errorf("OPENING transactions = %d, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'CREDIT' AND balance_before_minor = 0 AND balance_after_minor = 100000`, w.ID()); n != 1 {
		t.Errorf("opening credit entries = %d, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = $2`, w.ID(), string(event.TypeWalletBalanceChanged)); n != 1 {
		t.Errorf("WalletBalanceChanged events = %d, want 1", n)
	}
	if n := e.count(t, `
		SELECT count(*) FROM outbox_events o
		JOIN wager_transactions t ON t.id = o.aggregate_id
		WHERE t.wallet_id = $1 AND o.event_type = $2`, w.ID(), string(event.TypeWagerTransactionProcessed)); n != 1 {
		t.Errorf("WagerTransactionProcessed events = %d, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox_events WHERE payload->>'walletVersion' = '1' AND aggregate_id = $1`, w.ID()); n != 1 {
		t.Errorf("the opening event must carry walletVersion 1")
	}
}

func TestIntegration_OpenWallet_ZeroBalance_CreatesOnlyTheWallet(t *testing.T) {
	e := newEnv(t)
	w, err := e.walletService(t).Open(context.Background(), internalCaller, app.OpenWalletCommand{
		PlayerID:       uuid.New(),
		InitialBalance: mustMoney(t, "0.00"),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if n := e.count(t, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1`, w.ID()); n != 0 {
		t.Errorf("zero balance created %d transactions", n)
	}
	if n := e.count(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, w.ID()); n != 0 {
		t.Errorf("zero balance created %d ledger entries", n)
	}
	if n := e.count(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1`, w.ID()); n != 0 {
		t.Errorf("zero balance created %d events", n)
	}
}

func TestIntegration_OpenWallet_SamePlayerAndCurrency_Conflicts(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	svc := e.walletService(t)
	player := uuid.New()
	cmd := app.OpenWalletCommand{PlayerID: player, InitialBalance: mustMoney(t, "10.00")}

	// várias aberturas simultâneas: só uma pode vencer
	results := make([]error, 10)
	runParallel(len(results), func(i int) {
		_, results[i] = svc.Open(ctx, internalCaller, cmd)
	})

	created, conflicts := 0, 0
	for i, err := range results {
		switch {
		case err == nil:
			created++
		case errors.Is(err, app.ErrWalletAlreadyExists):
			conflicts++
		default:
			t.Fatalf("open %d: %v", i, err)
		}
	}
	if created != 1 || conflicts != 9 {
		t.Errorf("created=%d conflicts=%d, want 1 and 9", created, conflicts)
	}
	if n := e.count(t, `SELECT count(*) FROM wallets WHERE player_id = $1`, player); n != 1 {
		t.Errorf("wallets for the player = %d, want 1", n)
	}
	if n := e.count(t, `SELECT count(*) FROM wager_transactions WHERE player_id = $1 AND kind = 'OPENING'`, player); n != 1 {
		t.Errorf("opening transactions = %d, want 1 (the losers must roll back)", n)
	}
}

func TestIntegration_WalletOperations_AreInternalOnly(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	svc := e.walletService(t)
	provider := app.Caller{ProviderID: "provider-a"}
	w := e.newWallet(t, "100.00")

	if _, err := svc.Open(ctx, provider, app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: mustMoney(t, "1.00")}); !errors.Is(err, app.ErrForbidden) {
		t.Errorf("Open by a provider: error = %v, want ErrForbidden", err)
	}
	if _, err := svc.Get(ctx, provider, w.id); !errors.Is(err, app.ErrForbidden) {
		t.Errorf("Get by a provider: error = %v, want ErrForbidden", err)
	}
	if _, err := svc.Ledger(ctx, provider, w.id, "", 10); !errors.Is(err, app.ErrForbidden) {
		t.Errorf("Ledger by a provider: error = %v, want ErrForbidden", err)
	}
	if _, err := svc.Reconcile(ctx, provider, w.id); !errors.Is(err, app.ErrForbidden) {
		t.Errorf("Reconcile by a provider: error = %v, want ErrForbidden", err)
	}
}

func TestIntegration_Ledger_PaginatesWithAStableCursor(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	svc := e.walletService(t)
	w := e.newWallet(t, "100.00")
	for i := 0; i < 6; i++ {
		e.process(t, i, w.cmd(t, wager.KindBet, "1.00", fmt.Sprintf("ext-%d", i), fmt.Sprintf("key-%d", i)))
	}
	// OPENING + 6 apostas = 7 lançamentos; páginas de 3: 3 + 3 + 1

	var all []string
	cursor := ""
	pages := 0
	for {
		page, err := svc.Ledger(ctx, internalCaller, w.id, cursor, 3)
		if err != nil {
			t.Fatalf("Ledger page %d: %v", pages, err)
		}
		pages++
		for _, entry := range page.Entries {
			all = append(all, entry.BalanceAfter().Amount())
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
		if pages > 10 {
			t.Fatal("pagination does not end")
		}
	}

	want := []string{"100.00", "99.00", "98.00", "97.00", "96.00", "95.00", "94.00"}
	if pages != 3 || fmt.Sprint(all) != fmt.Sprint(want) {
		t.Errorf("pages=%d balances=%v, want 3 pages and %v", pages, all, want)
	}

	other := e.newWallet(t, "1.00")
	if _, err := svc.Ledger(ctx, internalCaller, other.id, cursor, 3); !errors.Is(err, app.ErrInvalidCursor) {
		t.Errorf("cursor from another wallet: error = %v, want ErrInvalidCursor", err)
	}
	if _, err := svc.Ledger(ctx, internalCaller, w.id, "not-a-cursor!", 3); !errors.Is(err, app.ErrInvalidCursor) {
		t.Errorf("garbage cursor: error = %v, want ErrInvalidCursor", err)
	}
	if _, err := svc.Ledger(ctx, internalCaller, w.id, "", app.MaxLedgerLimit+1); !errors.Is(err, app.ErrInvalidCommand) {
		t.Errorf("limit too big: error = %v, want ErrInvalidCommand", err)
	}
}

// Cenário do README: abertura de 1000.00 e uma aposta de 25.00.
func TestIntegration_Reconciliation_Consistent(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	svc := e.walletService(t)

	opened, err := svc.Open(ctx, internalCaller, app.OpenWalletCommand{PlayerID: uuid.New(), InitialBalance: mustMoney(t, "1000.00")})
	if err != nil {
		t.Fatal(err)
	}
	w := testWallet{id: opened.ID(), player: opened.PlayerID(), provider: "provider-" + uuid.NewString()}
	e.process(t, 0, w.cmd(t, wager.KindBet, "25.00", "transaction-123", "key-123"))

	r, err := svc.Reconcile(ctx, internalCaller, w.id)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if r.Stored.Amount() != "975.00" || r.Calculated.Amount() != "975.00" ||
		r.Difference.Amount() != "0.00" || !r.Consistent || r.CheckedEntries != 2 {
		t.Errorf("reconciliation = %+v", r)
	}
}

func TestIntegration_Reconciliation_ReportsDivergenceWithoutFixingIt(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	svc := e.walletService(t)
	var reported []app.Reconciliation
	svc.OnDivergence(func(r app.Reconciliation) { reported = append(reported, r) })

	w := e.newWallet(t, "100.00")
	// Simula corrupção: um crédito de 1.00 no ledger sem o saldo correspondente.
	// (O banco permite INSERT no ledger; só proíbe alterar ou apagar.)
	lossID := uuid.New()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, `
		INSERT INTO wager_transactions (id, origin, kind, status, provider_id, external_transaction_id,
			idempotency_key, payload_hash, round_id, game_id, wallet_id, player_id, currency,
			amount_minor, result_balance_minor)
		VALUES ($1, 'EXTERNAL', 'LOSS', 'PROCESSED', $2, 'x', 'x', 'x', 'r', 'g', $3, $4, 'BRL', 0, 10000)`,
		lossID, w.provider, w.id, w.player)
	mustExec(t, tx, `
		INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before_minor, balance_after_minor)
		VALUES ($1, $2, $3, 'CREDIT', 100, 'BRL', 10000, 10100)`, uuid.New(), w.id, lossID)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	r, err := svc.Reconcile(ctx, internalCaller, w.id)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if r.Consistent || r.Stored.Amount() != "100.00" || r.Calculated.Amount() != "101.00" || r.Difference.Amount() != "-1.00" {
		t.Errorf("reconciliation = %+v, want stored 100.00, calculated 101.00, difference -1.00", r)
	}
	if len(reported) != 1 {
		t.Errorf("divergence callback called %d times, want 1", len(reported))
	}
	if bal, version := e.balance(t, w); bal != "100.00" || version != 1 {
		t.Errorf("reconciliation changed the wallet: %s v%d", bal, version)
	}
}

func TestIntegration_TransactionQueries_IsolateProviders(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	reader := e.reader()
	w := e.newWallet(t, "100.00")
	res := e.process(t, 0, w.cmd(t, wager.KindBet, "10.00", "ext-1", "key-1"))

	owner := app.Caller{ProviderID: w.provider}
	stranger := app.Caller{ProviderID: "provider-someone-else"}

	if tx, err := reader.ByID(ctx, owner, res.TransactionID); err != nil || tx.ID() != res.TransactionID {
		t.Errorf("owner ByID = %v, %v", tx, err)
	}
	if tx, err := reader.ByExternalID(ctx, owner, w.provider, "ext-1"); err != nil || tx.ID() != res.TransactionID {
		t.Errorf("owner ByExternalID = %v, %v", tx, err)
	}
	if _, err := reader.ByID(ctx, internalCaller, res.TransactionID); err != nil {
		t.Errorf("internal ByID: %v", err)
	}

	// Outro provedor: por ID responde como inexistente (não revela que existe).
	if _, err := reader.ByID(ctx, stranger, res.TransactionID); !errors.Is(err, app.ErrTransactionNotFound) {
		t.Errorf("stranger ByID: error = %v, want ErrTransactionNotFound", err)
	}
	// Pedir pelo providerId de outro é proibido antes de qualquer leitura.
	if _, err := reader.ByExternalID(ctx, stranger, w.provider, "ext-1"); !errors.Is(err, app.ErrForbidden) {
		t.Errorf("stranger ByExternalID: error = %v, want ErrForbidden", err)
	}
	// A transação OPENING (interna) também não aparece para provedores.
	var openingID uuid.UUID
	if err := e.pool.QueryRow(ctx, `SELECT id FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING'`, w.id).Scan(&openingID); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ByID(ctx, owner, openingID); !errors.Is(err, app.ErrTransactionNotFound) {
		t.Errorf("provider reading an OPENING: error = %v, want ErrTransactionNotFound", err)
	}
}
