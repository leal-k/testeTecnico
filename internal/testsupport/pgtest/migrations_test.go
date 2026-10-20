package pgtest_test

// Verifica as migrations nos dois sentidos, contra um PostgreSQL real: todas
// sobem, todas descem (cada Down desfaz o seu Up, inclusive a 00006, que roda
// fora de transação) e sobem de novo sobre o banco revertido. Exige Docker;
// sem Docker, ou com `go test -short`, é ignorado.

import (
	"context"
	"database/sql"
	"io/fs"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	testetecnico "github.com/leal-k/testeTecnico"
	"github.com/leal-k/testeTecnico/internal/testsupport/pgtest"
)

// Tabelas criadas pelas migrations.
var tables = []string{"wallets", "wager_transactions", "wallet_ledger_entries", "inbox_messages", "outbox_events"}

func TestMigrations_UpDownUp(t *testing.T) {
	if testing.Short() {
		t.Skip("integração: precisa de Docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	dsn, stop, err := pgtest.Start(ctx) // já aplica todas as migrations
	if err != nil {
		t.Skipf("PostgreSQL em container indisponível (Docker?): %v", err)
	}
	defer stop()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	migrations, err := fs.Sub(testetecnico.MigrationsFS, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations)
	if err != nil {
		t.Fatal(err)
	}
	latest := int64(len(provider.ListSources()))

	existing := func() int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, `
			SELECT count(*) FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = ANY($1)`, tables).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	version := func() int64 {
		t.Helper()
		v, err := provider.GetDBVersion(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}

	// 1. Depois do Start: todas aplicadas.
	if v := version(); v != latest {
		t.Fatalf("version after up = %d, want %d", v, latest)
	}
	if n := existing(); n != len(tables) {
		t.Fatalf("tables after up = %d, want %d", n, len(tables))
	}

	// 2. Reverte todas: cada Down precisa funcionar e o schema volta a vazio.
	if _, err := provider.DownTo(ctx, 0); err != nil {
		t.Fatalf("reverting every migration: %v", err)
	}
	if v := version(); v != 0 {
		t.Errorf("version after down = %d, want 0", v)
	}
	if n := existing(); n != 0 {
		t.Errorf("tables after down = %d, want 0 (some Down left objects behind)", n)
	}

	// 3. Sobe de novo sobre o banco revertido: os Down não deixaram resíduos
	// que impeçam reaplicar.
	if _, err := provider.Up(ctx); err != nil {
		t.Fatalf("reapplying after down: %v", err)
	}
	if v := version(); v != latest {
		t.Errorf("version after re-up = %d, want %d", v, latest)
	}
	if n := existing(); n != len(tables) {
		t.Errorf("tables after re-up = %d, want %d", n, len(tables))
	}
}
