DATABASE_URL ?= postgres://app:app@localhost:5432/testetecnico?sslmode=disable
GOOSE = goose -dir migrations postgres "$(DATABASE_URL)"

.PHONY: infra migrate-up migrate-down migrate-reset migrate-status test test-race vet

infra:
	docker compose up -d

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

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...
