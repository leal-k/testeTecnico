# Teste de carga: 2026-10-19 22:00

## Parâmetros

| Parâmetro | Valor |
|---|---|
| Duração | 1m0s |
| Requisições simultâneas | 50 |
| Instâncias | 3 (http://localhost:8081, http://localhost:8082, http://localhost:8083) |
| Carteiras disputadas | 100 |
| Mistura | 20% WIN, 80% BET nas operações novas; 10% das requisições são reenvios |
| Comando | `go run ./cmd/loadtest -duration 1m0s -concurrency 50 -wallets 100 -dup-ratio 0.10` |

## Ambiente

| Item | Valor |
|---|---|
| CPU | AMD Ryzen 7 5700X 8-Core Processor (16 núcleos lógicos) |
| Sistema | linux/amd64 |
| Gerador | este programa, na mesma máquina que o ambiente (disputa CPU com ele) |

## Resultados

| Métrica | Valor |
|---|---|
| Requisições | 234276 em 1m0.014s |
| Vazão | 3904 req/s |
| Latência p50 | 10.7 ms |
| Latência p95 | 24.4 ms |
| Latência p99 | 41.0 ms |
| Latência máxima | 256.5 ms |
| Erros 5xx | 0 |
| Erros de rede | 0 |
| Conflitos de concorrência (métrica) | 0 |
| Atraso máximo da outbox | 174.63 s |
| Pico de eventos pendentes na outbox | 402690 |
| Tempo para a outbox esvaziar após a carga | não esvaziou em 2 min |

### Respostas

| Código HTTP | Quantidade |
|---|---|
| 200 | 234276 |

| Resultado | Quantidade |
|---|---|
| PROCESSED | 210809 |
| PROCESSED (replay) | 23467 |

## Correção sob carga

| Verificação | Resultado |
|---|---|
| Reenvios respondidos como replay | 23467 de 23467 |
| Reenvios aplicados de novo (deve ser 0) | 0 |
| Carteiras consistentes na reconciliação | 100 de 100 |
