# Teste de carga

Este documento descreve como o teste de carga é feito. Os números de cada
execução ficam nos relatórios gerados pelo próprio programa:

- [`load-test-results.md`](load-test-results.md): carga distribuída (100 carteiras)
- [`load-test-hot-wallet.md`](load-test-hot-wallet.md): disputa máxima (1 carteira)

## Como reproduzir

```bash
docker compose up --build -d     # ambiente completo, 3 instâncias da aplicação
make loadtest                    # 60s, 50 requisições simultâneas, 100 carteiras
make loadtest-hot                # 30s, 50 requisições simultâneas, 1 carteira
```

Parâmetros ajustáveis (`go run ./cmd/loadtest -h`): `-duration`,
`-concurrency`, `-wallets`, `-dup-ratio`, `-win-ratio`, `-targets`, `-report`.

O programa termina com código diferente de zero se alguma carteira estiver
inconsistente na reconciliação final.

## Metodologia

1. **Preparação.** Abre N carteiras novas com saldo alto (padrão: 1.000.000,00 BRL),
   para que o resultado meça o processamento, não rejeições por saldo.
2. **Carga.** Um número fixo de trabalhadores (`-concurrency`) envia requisições
   em laço fechado durante `-duration`. Cada requisição vai para uma das três
   instâncias, escolhida ao acaso. A mistura é:
   - 90% operações novas (80% BET, 20% WIN), em carteira sorteada;
   - 10% reenvios de uma operação já enviada, com a mesma `Idempotency-Key`
     e o mesmo corpo, simulando um provedor que repete após timeout.
3. **Tokens reais.** Os tokens vêm do Keycloak (`client_credentials`, clients
   `provider-a` e `internal-service`) e são renovados antes de expirar. A
   validação do JWT faz parte do custo medido.
4. **Medição.** A latência é medida no cliente, do envio até a leitura completa
   da resposta. Os percentis são calculados sobre todas as amostras (sem
   amostragem). Conflitos de concorrência e atraso da outbox vêm do `/metrics`
   das instâncias: conflitos pela diferença entre antes e depois; o atraso da
   outbox é lido a cada 500 ms durante a carga.
5. **Esvaziamento da outbox.** Depois da carga, o programa mede quanto tempo a
   outbox leva para publicar todos os eventos pendentes no SQS.
6. **Correção.** Confere que todo reenvio foi respondido como replay (nenhum
   débito duplicado) e reconcilia todas as carteiras usadas (saldo armazenado ×
   soma do ledger).

## Os dois cenários

| Cenário | O que exercita |
|---|---|
| 100 carteiras | Vazão com pouca disputa: cada carteira recebe ~1% das requisições, então os locks raramente se encontram. |
| 1 carteira | Pior caso: todas as transações serializam no `SELECT ... FOR UPDATE` da mesma carteira. A vazão cai para o limite de uma fila serial e a latência sobe, mas o saldo deve continuar exato. |

O cenário de uma carteira não representa uso real (um jogador não faz centenas
de apostas por segundo); ele mostra que o lock pessimista mantém a correção sob
disputa extrema e quanto ela custa.

## O que o teste encontrou

A correção passou em todas as execuções: nenhum erro 5xx, nenhum reenvio
aplicado duas vezes e todas as carteiras consistentes na reconciliação. A API
sustentou ~4 mil req/s com p99 de algumas dezenas de ms (100 carteiras).

O problema encontrado foi na **outbox**: a API gera ~2 eventos por transação
(~7.800 eventos/s no cenário de 100 carteiras), e a publicação ficou muito
abaixo disso. O atraso cresceu durante toda a carga (pico de ~450 mil eventos
pendentes). Nenhum evento se perde, porque estão confirmados no banco e saem
depois, mas o atraso para os consumidores cresce sem limite enquanto durar a
carga. A investigação, passo a passo:

| Passo | Medição | Conclusão |
|---|---|---|
| 1. Publicação um por um | ~200 eventos/s durante a carga | cada evento custava duas idas e voltas em série (SQS + `UPDATE`) |
| 2. Publicação em lote (`SendMessageBatch` de até 10 + um `UPDATE ... ANY($1)` por lote) | ~315 eventos/s durante a carga | ajudou, mas pouco: o gargalo principal estava em outro lugar |
| 3. `docker stats` sem carga | MiniStack 99% de CPU, Postgres 44%, aplicação < 1% | as instâncias estavam esperando, não trabalhando |
| 4. `EXPLAIN ANALYZE` da reserva | `Parallel Seq Scan` + `Sort` sobre ~250 mil pendentes: **54 ms** para reservar 50 | faltava índice na ordem da consulta (`ORDER BY created_at, id`) |
| 5. Migration `00006` (índice parcial em `(created_at, id)` onde `published_at IS NULL`) | `Index Scan`: **0,34 ms**; CPU do Postgres sem carga: 44% → **1%** | gargalo do banco resolvido |
| 6. Esvaziar a fila de eventos do MiniStack (`purge-queue`) e medir de novo | ~130 eventos/s antes e depois, MiniStack sempre em 99% | o teto restante é o simulador (um processo Python em um núcleo), não o código |

**Em produção:** uma fila FIFO do SQS aceita 300 chamadas/s sem o modo de alta
vazão. Publicando um evento por chamada, o teto seria 300 eventos/s; em lotes
de 10, 3.000 eventos/s (e mais com o modo de alta vazão). Por isso a publicação
em lote é a correção certa para produção, ainda que o MiniStack não deixe o
ganho aparecer aqui. Se a geração ainda superasse a publicação, os próximos
passos seriam publicar lotes em paralelo dentro de cada instância e
particionar a reserva por agregado.

## Limitações conhecidas

- **Gerador e sistema na mesma máquina.** O gerador disputa CPU com Postgres,
  Keycloak, MiniStack e as três instâncias. Os números são do ambiente local
  inteiro, não de uma instância isolada.
- **Laço fechado.** Cada trabalhador só envia a próxima requisição depois da
  resposta anterior. Quando o sistema fica lento, a taxa de envio também cai
  (*coordinated omission*), então os percentis altos podem sair otimistas em
  relação a um teste com taxa fixa.
- **Pool de conexões.** Cada instância usa `DB_MAX_CONNS=20` (padrão), ou seja,
  até 60 conexões no total, abaixo do limite padrão do Postgres (100). Não foi
  feito ajuste fino desse valor.
- **Teto do MiniStack.** O simulador de SQS publica ~130 mensagens/s neste
  ambiente, então o atraso da outbox medido aqui reflete o simulador, não o
  SQS real. Por isso o relatório mostra "não esvaziou em 2 min" mesmo depois
  das correções.
- **Atraso da outbox no cenário de 1 carteira.** Ele roda logo depois do cenário
  de 100 carteiras, sem limpar o banco, então o atraso e o pico de pendentes
  do relatório incluem o acúmulo deixado pelo primeiro cenário. Latência,
  vazão e correção não são afetadas.
- **Conflitos de concorrência.** Como o processamento trava a carteira antes de
  qualquer INSERT, os conflitos de inserção (que geram retry) só acontecem
  quando duas requisições com a mesma chave chegam ao mesmo tempo. Esperar um
  número baixo, mesmo sob muita carga, é o comportamento correto.
