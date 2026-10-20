DATABASE_URL ?= postgres://app:app@localhost:5432/testetecnico?sslmode=disable
GOOSE = goose -dir migrations postgres "$(DATABASE_URL)"

.PHONY: up down logs ps infra demo loadtest loadtest-hot migrate-up migrate-down migrate-reset migrate-status test test-short test-race vet fmt

# ---- ambiente completo ----
up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f app

ps:
	docker compose ps

# demonstração com as 3 instâncias (ambiente no ar)
demo:
	./scripts/demo-concurrency.sh

# teste de carga (ambiente no ar): 100 carteiras, 50 requisições simultâneas, 60s
loadtest:
	mkdir -p docs
	go run ./cmd/loadtest -duration 60s -concurrency 50 -wallets 100 -report docs/load-test-results.md

# disputa máxima: todas as requisições na mesma carteira
loadtest-hot:
	mkdir -p docs
	go run ./cmd/loadtest -duration 30s -concurrency 50 -wallets 1 -report docs/load-test-hot-wallet.md

# só a infraestrutura (Postgres, Keycloak, MiniStack e filas)
infra:
	docker compose up -d postgres keycloak ministack queue-init

# ---- migrations (CLI goose, contra o Postgres do Compose) ----
migrate-up:
	$(GOOSE) up

# reverte apenas a ultima migration
migrate-down:
	$(GOOSE) down

# reverte todas
migrate-reset:
	$(GOOSE) reset

migrate-status:
	$(GOOSE) status

# ---- qualidade ----
test:
	go test ./...

# sem os testes que sobem containers
test-short:
	go test -short ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .
