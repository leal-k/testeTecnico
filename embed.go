// Package testetecnico expõe os recursos embutidos do projeto.
package testetecnico

import "embed"

// MigrationsFS contém as migrations SQL (diretório "migrations/"), para serem
// aplicadas por código nos testes de integração e na inicialização da aplicação.
//
//go:embed migrations/*.sql
var MigrationsFS embed.FS
