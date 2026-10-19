// Package pgtest sobe um PostgreSQL real em container, com as migrations do
// projeto aplicadas, para os testes de integração.
package pgtest

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	testetecnico "github.com/leal-k/testeTecnico"
)

// Start sobe o container e devolve a connection string e uma função para
// encerrá-lo. Um erro aqui normalmente significa Docker indisponível.
func Start(ctx context.Context) (dsn string, stop func(), err error) {
	container, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithDatabase("testetecnico_test"),
		tcpostgres.WithUsername("app"),
		tcpostgres.WithPassword("app"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second)),
	)
	stop = func() {
		if container != nil {
			_ = container.Terminate(context.Background())
		}
	}
	if err != nil {
		stop()
		return "", func() {}, fmt.Errorf("starting PostgreSQL container: %w", err)
	}
	dsn, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		stop()
		return "", func() {}, err
	}
	if err := Migrate(ctx, dsn); err != nil {
		stop()
		return "", func() {}, fmt.Errorf("applying migrations: %w", err)
	}
	return dsn, stop, nil
}

// Migrate aplica todas as migrations embutidas.
func Migrate(ctx context.Context, dsn string) error {
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
