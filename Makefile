DATABASE_URL ?= postgres://app:app@localhost:5432/testetecnico?sslmode=disable
GOOSE = goose -dir migrations postgres "$(DATABASE_URL)"

.PHONY: up down logs ps infra demo migrate-up migrate-down migrate-reset migrate-status test test-short test-race vet fmt

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
