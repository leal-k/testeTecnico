# Teste de carga: 2026-10-19 22:03

## Parâmetros

| Parâmetro | Valor |
|---|---|
| Duração | 30s |
| Requisições simultâneas | 50 |
| Instâncias | 3 (http://localhost:8081, http://localhost:8082, http://localhost:8083) |
| Carteiras disputadas | 1 |
| Mistura | 20% WIN, 80% BET nas operações novas; 10% das requisições são reenvios |
| Comando | `go run ./cmd/loadtest -duration 30s -concurrency 50 -wallets 1 -dup-ratio 0.10` |

## Ambiente

| Item | Valor |
|---|---|
| CPU | AMD Ryzen 7 5700X 8-Core Processor (16 núcleos lógicos) |
| Sistema | linux/amd64 |
| Gerador | este programa, na mesma máquina que o ambiente (disputa CPU com ele) |

## Resultados

| Métrica | Valor |
|---|---|
| Requisições | 7857 em 30.188s |
| Vazão | 260 req/s |
| Latência p50 | 135.9 ms |
| Latência p95 | 592.0 ms |
| Latência p99 | 942.4 ms |
| Latência máxima | 2616.1 ms |
| Erros 5xx | 0 |
| Erros de rede | 0 |
| Conflitos de concorrência (métrica) | 0 |
| Atraso máximo da outbox | 379.65 s |
| Pico de eventos pendentes na outbox | 391648 |
| Tempo para a outbox esvaziar após a carga | não esvaziou em 2 min |

### Respostas

| Código HTTP | Quantidade |
|---|---|
| 200 | 7857 |

| Resultado | Quantidade |
|---|---|
| PROCESSED | 7128 |
| PROCESSED (replay) | 729 |

## Correção sob carga

| Verificação | Resultado |
|---|---|
| Reenvios respondidos como replay | 729 de 729 |
| Reenvios aplicados de novo (deve ser 0) | 0 |
| Carteiras consistentes na reconciliação | 1 de 1 |
