// Comando app: a aplicação (API HTTP e workers) e utilitários operacionais.
//
//	app                      sobe a aplicação
//	app migrate up|down|reset|status   gerencia o schema do banco
//	app healthcheck          consulta /health/live (usado pelo Docker)
package main

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
	"go.uber.org/fx"

	testetecnico "github.com/leal-k/testeTecnico"
	"github.com/leal-k/testeTecnico/internal/platform/config"
	"github.com/leal-k/testeTecnico/internal/platform/fxapp"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "testetecnico")

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "migrate":
			os.Exit(runMigrate(log, os.Args[2:]))
		case "healthcheck":
			os.Exit(runHealthcheck())
		default:
			fmt.Fprintf(os.Stderr, "unknown command %q (use: migrate, healthcheck)\n", os.Args[1])
			os.Exit(2)
		}
	}

	// A configuração é validada antes de qualquer conexão.
	cfg, err := config.Load()
	if err != nil {
		log.Error("invalid configuration", "error", err.Error())
		os.Exit(1)
	}
	// Run inicia tudo, espera SIGINT/SIGTERM e encerra na ordem certa.
	fx.New(fxapp.Options(cfg)).Run()
}

// runMigrate aplica ou reverte as migrations embutidas no binário. Um lock de
// sessão no PostgreSQL impede que duas instâncias migrem ao mesmo tempo.
func runMigrate(log *slog.Logger, args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: app migrate up|down|reset|status")
		return 2
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Error("DATABASE_URL is required")
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Error("opening database", "error", err.Error())
		return 1
	}
	defer db.Close()

	migrations, err := fs.Sub(testetecnico.MigrationsFS, "migrations")
	if err != nil {
		log.Error("loading migrations", "error", err.Error())
		return 1
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		log.Error("creating migration lock", "error", err.Error())
		return 1
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations, goose.WithSessionLocker(locker))
	if err != nil {
		log.Error("creating migration provider", "error", err.Error())
		return 1
	}

	switch args[0] {
	case "up":
		results, err := provider.Up(ctx)
		for _, r := range results {
			log.Info("migration applied", "migration", r.String())
		}
		if err != nil {
			log.Error("migrate up failed", "error", err.Error())
			return 1
		}
		version, _ := provider.GetDBVersion(ctx)
		log.Info("database is up to date", "version", version)
	case "down":
		r, err := provider.Down(ctx)
		if err != nil {
			log.Error("migrate down failed", "error", err.Error())
			return 1
		}
		log.Info("migration reverted", "migration", r.String())
	case "reset":
		results, err := provider.DownTo(ctx, 0)
		for _, r := range results {
			log.Info("migration reverted", "migration", r.String())
		}
		if err != nil {
			log.Error("migrate reset failed", "error", err.Error())
			return 1
		}
	case "status":
		statuses, err := provider.Status(ctx)
		if err != nil {
			log.Error("migrate status failed", "error", err.Error())
			return 1
		}
		for _, s := range statuses {
			fmt.Printf("%-8s %s\n", s.State, s.Source.Path)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown migrate command %q (use: up, down, reset, status)\n", args[0])
		return 2
	}
	return 0
}

// runHealthcheck consulta o liveness do próprio processo. A imagem final não
// tem curl nem shell, então o Docker usa este subcomando.
func runHealthcheck() int {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return 1
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/health/live")
	if err != nil {
		fmt.Fprintln(os.Stderr, strings.TrimSpace(err.Error()))
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
