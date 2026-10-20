# testeTecnico: processamento distribuído de apostas em Go

Serviço em Go (Uber Fx) que movimenta carteiras de jogadores a partir de operações de provedores de jogos (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`), recebidas por **API HTTP** e por **fila SQS**, com as mesmas garantias nos dois caminhos: dinheiro exato, ledger imutável, idempotência persistente, coordenação por carteira entre várias instâncias, inbox, transactional outbox e autenticação com Keycloak.

As decisões técnicas, limitações e interpretações estão em **[ARCHITECTURE.md](ARCHITECTURE.md)**.

## Sumário
1. [Pré-requisitos](#1-pré-requisitos)
2. [Subir o ambiente](#2-subir-o-ambiente)
3. [Configuração](#3-configuração)
4. [Identidades e tokens](#4-identidades-e-tokens)
5. [Exemplos de chamadas](#5-exemplos-de-chamadas)
6. [Filas SQS](#6-filas-sqs)
7. [Migrations](#7-migrations)
8. [Testes](#8-testes)
9. [Várias instâncias e simulações de falha](#9-várias-instâncias-e-simulações-de-falha)
10. [Observabilidade e teste de carga](#10-observabilidade-e-teste-de-carga)
11. [Problemas comuns](#11-problemas-comuns)
12. [Estrutura do projeto](#12-estrutura-do-projeto)

## 1. Pré-requisitos

| Ferramenta | Para quê |
|---|---|
| Docker com Docker Compose v2 | ambiente completo e testes de integração |
| Go 1.26 | testes, teste de carga e a aplicação fora do Docker (o `go.mod` pede 1.26.9; com uma 1.26 anterior, o Go baixa essa versão sozinho) |
| gcc | `go test -race` (o detector de corridas usa cgo) |
| `curl`, `jq`, `uuidgen` | exemplos e script de demonstração |
| AWS CLI v2 (opcional) | enviar mensagens e ler filas manualmente |

Portas usadas no host: `5432` (PostgreSQL), `8080` (Keycloak), `4566` (MiniStack/SQS), `8081`–`8083` (as três instâncias da aplicação), `9090` (Prometheus) e `3000` (Grafana).

> Linux e macOS funcionam direto. No Windows, use o WSL2 (o `Makefile` e os scripts usam bash). No Fedora (SELinux), os volumes do Compose já usam o sufixo `:z`.

## 2. Subir o ambiente

```bash
docker compose up --build
```

Isso sobe, nesta ordem:

| Serviço | O que faz |
|---|---|
| `postgres` | banco de dados |
| `keycloak` | IdP; importa o realm `deploy/keycloak/testetecnico-realm.json` na primeira subida |
| `ministack` | SQS local |
| `queue-init` | cria `wager-transactions.fifo`, `wager-transactions-dlq.fifo` (com redrive) e `wager-events.fifo`, e termina |
| `migrate` | aplica as migrations e termina |
| `app` (3 réplicas) | API HTTP, consumidor SQS, worker de referências pendentes e publisher da outbox |
| `prometheus` | coleta `/metrics` de todas as instâncias |
| `grafana` | dashboard provisionado (http://localhost:3000) |

Para rodar em segundo plano, use `docker compose up --build -d` (ou `make up`). A primeira subida baixa as imagens e compila a aplicação, o que leva alguns minutos.

O ambiente está pronto quando `docker compose ps` mostra `postgres`, `keycloak` e as três instâncias `app` como `healthy`. As instâncias só sobem depois de o Keycloak terminar de importar o realm (de 30 s a 1 min numa subida do zero), então daí em diante já dá para pedir tokens. Para acompanhar: `docker compose logs -f app`.

Conferência rápida:
```bash
curl -s localhost:8081/health/ready | jq .
# {"status":"ready","checks":{"postgres":"ok","sqs":"ok","events-queue":"ok"}}
```

Para encerrar: `docker compose down`. Para apagar também os dados do banco: `docker compose down -v`.

## 3. Configuração

O Compose já define tudo; não é preciso criar `.env`. Para mudar valores locais (credenciais de desenvolvimento, quantidade de réplicas, nível de log), copie o exemplo:
```bash
cp .env.example .env
```

Variáveis da aplicação (todas com padrão, exceto as obrigatórias):

| Variável | Padrão | Descrição |
|---|---|---|
| `DATABASE_URL` | **obrigatória** | conexão com o PostgreSQL |
| `AUTH_ISSUER_URL` | **obrigatória** | emissor esperado nos tokens (`iss`) |
| `AUTH_JWKS_URL` | **obrigatória** | onde buscar as chaves públicas do IdP |
| `AUTH_AUDIENCE` | `testetecnico-api` | audiência exigida (`aud`) |
| `HTTP_ADDR` | `:8080` | endereço da API |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `SHUTDOWN_TIMEOUT` | `20s` | prazo do encerramento organizado |
| `REFERENCE_TTL` | `5m` | prazo para uma referência chegar |
| `SQS_ENABLED` | `true` | liga o consumidor e o publisher |
| `SQS_ENDPOINT_URL` | vazio (AWS) | no Compose, `http://ministack:4566` |
| `SQS_VISIBILITY_TIMEOUT` | `60s` | precisa ser maior que `SQS_HANDLE_TIMEOUT` (`15s`) |
| `SQS_MAX_RECEIVES` | `5` | tentativas antes da DLQ |
| `OUTBOX_ENABLED` | `true` | liga o publisher da outbox |
| `OUTBOX_LEASE` | `30s` | validade da reserva de um lote de eventos |

A lista completa, com todos os ajustes finos, está em `.env.example` e em `internal/platform/config/config.go`. A configuração é validada na subida: se algo estiver errado, a aplicação lista todos os problemas e não inicia.

## 4. Identidades e tokens

O realm `testetecnico` é provisionado automaticamente, com estas identidades de teste (`client_credentials`):

| client_id | client_secret | Papel | `providerId` |
|---|---|---|---|
| `provider-a` | `provider-a-secret` | `provider` | `provider-a` |
| `provider-b` | `provider-b-secret` | `provider` | `provider-b` |
| `internal-service` | `internal-service-secret` | `internal` | — |

Provedores enviam e consultam as próprias operações. Abrir e ler carteiras, ledger e reconciliação é exclusivo do serviço interno. São credenciais **de desenvolvimento**, sem valor real.

Para obter um token:
```bash
token() {
  curl -s -X POST http://localhost:8080/realms/testetecnico/protocol/openid-connect/token \
    -d grant_type=client_credentials -d client_id=$1 -d client_secret=$1-secret | jq -r .access_token
}
INTERNAL=$(token internal-service)
PROVIDER=$(token provider-a)
```
Os tokens valem 5 minutos. O console do Keycloak fica em http://localhost:8080 (`admin`/`admin`).

## 5. Exemplos de chamadas

As três instâncias são equivalentes; os exemplos usam portas diferentes de propósito.

**Abrir carteira** (serviço interno):
```bash
PLAYER=$(uuidgen)
WALLET=$(curl -s -X POST localhost:8081/wallets \
  -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}" \
  | tee /dev/stderr | jq -r .id)
```

**Enviar uma aposta** (provedor):
```bash
BET="{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"

curl -s -X POST localhost:8082/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' -d "$BET" | jq .
# {"transactionId":"...","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}
```

**Reenviar a mesma aposta** (em outra instância): devolve o mesmo resultado, com `idempotentReplay: true`, sem novo débito.
```bash
curl -s -X POST localhost:8083/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' -d "$BET" | jq .
```

**Reversão** (acrescente `referenceExternalTransactionId`):
```bash
curl -s -X POST localhost:8081/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:refund-123' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"refund-123\",\"referenceExternalTransactionId\":\"transaction-123\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"REFUND\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}" | jq .
```

**Consultas:**
```bash
curl -s localhost:8081/wallets/$WALLET -H "Authorization: Bearer $INTERNAL" | jq .
curl -s "localhost:8081/wallets/$WALLET/ledger?limit=50" -H "Authorization: Bearer $INTERNAL" | jq .
curl -s localhost:8081/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PROVIDER" | jq .
```
O ledger pagina por cursor opaco: use o `nextCursor` da resposta em `?cursor=...` (ele é `null` na última página).

**Reconciliação:**
```bash
curl -s -X POST localhost:8081/wallets/$WALLET/reconciliation -H "Authorization: Bearer $INTERNAL" | jq .
```

**Saúde e métricas** (públicos):
```bash
curl -s localhost:8081/health/live
curl -s localhost:8081/health/ready | jq .
curl -s localhost:8081/metrics | grep '^testetecnico_'
```

Os códigos HTTP e corpos de erro estão em [ARCHITECTURE.md, seção 12](ARCHITECTURE.md#12-contrato-http-códigos-e-corpos).

## 6. Filas SQS

As filas são criadas pelo serviço `queue-init` (`deploy/aws/init-queues.sh`). Para usá-las do host:
```bash
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
Q=http://localhost:4566/000000000000
aws --endpoint-url http://localhost:4566 sqs list-queues
```

**Enviar uma operação pela fila** (o `messageId` precisa ser único; aqui, um UUID novo):
```bash
MSG="{\"messageId\":\"$(uuidgen)\",\"type\":\"WagerTransactionRequested\",\"occurredAt\":\"$(date -u +%FT%TZ)\",\"data\":{\"providerId\":\"provider-a\",\"externalTransactionId\":\"sqs-1\",\"idempotencyKey\":\"provider-a:sqs-1\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"r1\",\"gameId\":\"g1\",\"kind\":\"BET\",\"money\":{\"amount\":\"10.00\",\"currency\":\"BRL\"}}}"
aws --endpoint-url http://localhost:4566 sqs send-message --queue-url $Q/wager-transactions.fifo \
  --message-body "$MSG" --message-group-id "$WALLET" --message-deduplication-id "$(uuidgen)"
docker compose logs app | grep '"message handled"' | tail -3
```
Produtores devem usar `MessageGroupId` = ID da carteira e `MessageDeduplicationId` = `messageId`.

**Ler os eventos publicados pela outbox:**
```bash
aws --endpoint-url http://localhost:4566 sqs receive-message --queue-url $Q/wager-events.fifo \
  --max-number-of-messages 5 | jq -r '.Messages[].Body' | jq .
```

**Ver a DLQ:**
```bash
aws --endpoint-url http://localhost:4566 sqs receive-message --queue-url $Q/wager-transactions-dlq.fifo \
  --message-attribute-names All | jq .
```

## 7. Migrations

As migrations ficam em `migrations/` (goose, uma por arquivo, com `Up` e `Down`) e são embutidas no binário.

**Dentro do Compose** (o serviço `migrate` já aplica na subida):
```bash
docker compose run --rm migrate migrate status   # situação
docker compose run --rm migrate migrate up       # aplicar todas
docker compose run --rm migrate migrate down     # reverter a última
docker compose run --rm migrate migrate reset    # reverter todas
```

**Do host**, com o CLI do goose (`go install github.com/pressly/goose/v3/cmd/goose@latest`):
```bash
make migrate-status
make migrate-up
make migrate-down     # reverte a última
make migrate-reset    # reverte todas
```
O `Makefile` usa `DATABASE_URL=postgres://app:app@localhost:5432/testetecnico?sslmode=disable`; sobrescreva com `make migrate-up DATABASE_URL=...`.

## 8. Testes

```bash
go vet ./...
go test ./...          # unitários + integração (precisa do Docker)
go test -race ./...    # o mesmo, com detector de corridas
go test -short ./...   # só os unitários (sem containers)
```

**Dependências dos testes:** os testes de integração sobem os próprios containers com [testcontainers](https://golang.testcontainers.org/) (PostgreSQL 17, MiniStack e Keycloak com o realm do projeto), independentes do Compose. Basta o Docker estar rodando e o usuário ter acesso a ele. Sem Docker, ou com `-short`, esses testes são ignorados (`SKIP`) e os unitários rodam normalmente. Não há build tags.

| Pacote | O que cobre |
|---|---|
| `internal/domain/...` | dinheiro, carteira, transações, ledger, eventos |
| `internal/app` | casos de uso (orquestração, idempotência, inbox, pendências, outbox) |
| `internal/adapters/postgres` | SQL real: locks, concorrência com 3 instâncias, constraints, outbox |
| `internal/adapters/sqs` | consumidor e publisher contra SQS real (MiniStack) + PostgreSQL |
| `internal/adapters/httpapi` | contrato HTTP, permissões, códigos de erro, logs |
| `internal/adapters/auth` | validação de tokens (IdP falso com RSA real) |
| `internal/platform/fxapp` | composição Fx, início, encerramento e retomada entre instâncias |
| `internal/e2e` | ponta a ponta com Keycloak, PostgreSQL e a aplicação HTTP reais: credenciais ausentes, inválidas, adulteradas e expiradas; isolamento entre provedores em consultas e replays; restrição das operações internas; nenhum efeito financeiro em acessos recusados |

**Teste extra contra o Keycloak do Compose** (opcional, com o ambiente de pé):
```bash
KEYCLOAK_URL=http://localhost:8080 go test -count=1 ./internal/adapters/auth/ -run Keycloak -v
```

A correspondência entre os cenários obrigatórios do enunciado e os testes está em [ARCHITECTURE.md, seção 15](ARCHITECTURE.md#15-estratégia-de-testes).

## 9. Várias instâncias e simulações de falha

**Três processos independentes.** Com o ambiente no ar:
```bash
./scripts/demo-concurrency.sh
```
O script roda, contra as três instâncias do Compose: duas apostas simultâneas de 80.00 sobre 100.00 em instâncias diferentes (uma processada, uma rejeitada, saldo 20.00, um único débito; reenvios não mudam nada); a mesma aposta 50 vezes em paralelo nas três instâncias (um único débito); e a reconciliação das carteiras. Termina com o total de verificações.

Para mudar a quantidade de instâncias: `APP_REPLICAS=1 docker compose up -d` (as portas vão de 8081 a 8083, então o máximo é 3).

**Simulações de falha** (com o ambiente no ar):

| Cenário | Como simular | O que observar |
|---|---|---|
| Encerramento organizado | `docker compose stop app` | nos logs, a ordem: HTTP, consumidor, publisher, worker, banco |
| Queda abrupta de uma instância | `docker kill testetecnico-app-1` durante o script | as outras seguem e reenvios são replays; `docker compose up -d` traz a instância de volta |
| Reinício de tudo | `docker compose restart app` e reenviar uma aposta já feita | `idempotentReplay: true` (idempotência persistente) |
| Banco fora do ar | `docker compose stop postgres` | `/health/ready` em 503, operações em 503 com `Retry-After`; `docker compose start postgres` volta ao normal |
| SQS fora do ar | `docker compose stop ministack` | readiness em 503, consumidor tentando com backoff, `testetecnico_outbox_pending_events` subindo; `docker compose start ministack` e a outbox esvazia |
| Referência atrasada | enviar um `REFUND` antes da `BET` | `202 PENDING_REFERENCE`; ao enviar a aposta, o worker conclui o estorno em segundos |
| Mensagem inválida | enviar `{"messageId":"x"}` para a fila | a mensagem vai para a DLQ com `failureReason=invalid_message` |

As falhas entre etapas (consumidor caindo depois do commit e antes de apagar a mensagem, publisher caindo entre publicar e confirmar, broker fora do ar) são reproduzidas de forma determinística nos testes de integração do pacote `internal/adapters/sqs`.

## 10. Observabilidade e teste de carga

**Dashboard:** com o ambiente no ar, abra **http://localhost:3000**. O dashboard *testeTecnico: visão geral* é a página inicial e abre sem login (somente leitura; para explorar consultas no Grafana, entre com `admin`/`admin`). Painéis:

| Grupo | O que mostra |
|---|---|
| Resumo | instâncias no ar, operações/s, latência p99, atraso da outbox, mensagens na DLQ no período e divergências de saldo |
| Operações | vazão por origem e status, latência p50/p95/p99, erros por motivo, duplicatas (replays e mensagens repetidas) e conflitos de concorrência |
| Mensageria | mensagens SQS por desfecho (processed, duplicate, retry, dlq), publicação da outbox (publicados e falhas), atraso e pendentes da outbox, referências pendentes e reconciliação |
| Instâncias | disponibilidade, goroutines e memória por instância |

O Prometheus (http://localhost:9090) descobre as instâncias pelo DNS do Compose (`dns_sd_configs` com o nome do serviço `app`), então funciona com qualquer número de réplicas. As métricas expostas estão em [ARCHITECTURE.md, seção 14](ARCHITECTURE.md#14-observabilidade).

**Teste de carga** (ambiente no ar; gera relatórios em `docs/`):
```bash
make loadtest       # 60s, 50 requisições simultâneas, 100 carteiras
make loadtest-hot   # 30s, 50 requisições simultâneas, 1 carteira (disputa máxima)
```
Para acompanhar ao vivo, deixe o dashboard aberto durante a carga. Metodologia, resultados e a análise do gargalo encontrado na outbox estão em [docs/LOAD_TEST.md](docs/LOAD_TEST.md).

## 11. Problemas comuns

| Sintoma | Causa e solução |
|---|---|
| `port is already allocated` na subida | outra coisa usa uma das portas (`5432`, `8080`, `4566`, `8081`–`8083`, `9090`, `3000`). Pare o outro serviço, por exemplo um PostgreSQL local (`sudo systemctl stop postgresql`), ou mude o lado do host da porta no `docker-compose.yml` |
| `permission denied` em `/var/run/docker.sock` | o usuário não está no grupo `docker`: `sudo usermod -aG docker $USER` e entrar de novo na sessão |
| `connection reset by peer` ao pedir token | o Keycloak ainda está subindo; espere ele ficar `healthy` em `docker compose ps` |
| instâncias `app` não sobem (`dependency failed`) | veja `docker compose logs migrate queue-init keycloak`; para recomeçar do zero, `docker compose down -v` e subir de novo |
| testes de integração aparecem como `SKIP` | o Docker não está acessível para o usuário que roda o `go test` |
| o atraso da outbox só cresce durante o teste de carga | esperado: o MiniStack publica ~130 mensagens/s; veja [docs/LOAD_TEST.md](docs/LOAD_TEST.md) |

## 12. Estrutura do projeto

```
cmd/app/                  ponto de entrada (app, app migrate, app healthcheck)
cmd/loadtest/             gerador do teste de carga
internal/domain/          money, wallet, wager, ledger, event (sem dependências externas)
internal/app/             casos de uso e portas
internal/adapters/        postgres, httpapi, auth, sqs
internal/platform/        config, fxapp (composição Fx), metrics
internal/e2e/             testes ponta a ponta de autenticação e autorização
internal/testsupport/     PostgreSQL em container para os testes
migrations/               schema (goose)
deploy/                   realm do Keycloak, criação das filas, Prometheus e Grafana
docs/                     teste de carga: metodologia, análise e relatórios
scripts/                  demonstração com 3 instâncias
```
