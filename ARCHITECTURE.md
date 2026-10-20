# Arquitetura

Este documento registra as decisões técnicas da solução, com o motivo de cada uma, as interpretações adotadas onde o enunciado deixava margem, as limitações conhecidas e o que não foi feito.

## 1. Visão geral

```
                 ┌──────────────────────── Keycloak (OIDC) ───────────────────────┐
                 │  emite tokens client_credentials; a API valida pelo JWKS       │
                 └────────────────────────────────────────────────────────────────┘
 provedores ──HTTP──▶ ┌──────────────┐
                      │  httpapi     │──┐
 wager-transactions   ├──────────────┤  │     ┌───────────────────┐      ┌──────────────┐
 .fifo ──SQS────────▶ │  sqs consumer│──┼───▶ │ app.Processor     │────▶ │ PostgreSQL   │
                      └──────────────┘  │     │ (mesmo caso de uso│      │ carteiras,   │
                                        │     │  para HTTP e SQS) │      │ transações,  │
              worker de pendências ─────┘     └───────────────────┘      │ ledger,      │
                                                                         │ inbox, outbox│
              publisher da outbox ◀── reserva com lease (SKIP LOCKED) ───┤              │
                     │                                                   └──────────────┘
                     └──SQS──▶ wager-events.fifo
```

Cada instância da aplicação roda, no mesmo processo, a API HTTP, o consumidor SQS, o worker de referências pendentes e o publisher da outbox. **Nada do que importa fica em memória:** idempotência, pendências, reservas da outbox e deduplicação de mensagens vivem no PostgreSQL. Por isso qualquer número de instâncias pode rodar ao mesmo tempo e qualquer uma retoma o trabalho de outra. O `docker-compose.yml` sobe três.

## 2. Organização do código

| Pacote | Responsabilidade | Depende de |
|---|---|---|
| `internal/domain/money` | Value object `Money` | biblioteca padrão |
| `internal/domain/wallet` | Agregado `Wallet` (débito, crédito, versão) | `money` |
| `internal/domain/wager` | `WagerTransaction`: tipos, estados, hash, regras (`Apply`) | `money`, `wallet` |
| `internal/domain/ledger` | Lançamento imutável | `money`, `wallet` |
| `internal/domain/event` | Eventos de integração (snapshot do payload) | domínio |
| `internal/app` | Casos de uso e portas (interfaces): `Processor`, `WalletService`, `TransactionReader`, `PendingWorker`, `OutboxRelay` | domínio |
| `internal/adapters/postgres` | Implementação das portas com `pgx` e SQL explícito | `app` |
| `internal/adapters/httpapi` | Contrato HTTP | `app` |
| `internal/adapters/auth` | Validação dos tokens do Keycloak | `app` |
| `internal/adapters/sqs` | Consumidor e publisher SQS | `app` |
| `internal/platform/config`, `fxapp`, `metrics` | Configuração, composição com Fx, métricas | tudo acima |
| `cmd/app` | Ponto de entrada (`app`, `app migrate ...`, `app healthcheck`) | `platform` |

O domínio não conhece Fx, HTTP, SQS nem a biblioteca do banco. Os casos de uso dependem só de interfaces declaradas no próprio pacote `app` (`Store`, `TxManager`, `OutboxStore`, `EventPublisher`...), e os adaptadores as implementam.

## 3. Dinheiro

- **Representação:** `int64` em unidades mínimas (centavos), com escala fixa de 2 casas e a moeda junto (código ISO 4217). Nunca há `float32`/`float64`: o parsing é feito dígito a dígito e a serialização formata inteiros.
- **Limite:** de `-92233720368547758.08` a `92233720368547758.07`. Soma, subtração, negação e parsing **detectam overflow antes** de calcular e retornam `ErrOverflow`, em vez de dar a volta silenciosamente.
- **Entrada externa** (`money.Parse`): aceita `25`, `25.5` e `25.50`. Rejeita vazio, sinal (`-1.00`, `+1.00`), espaços, notação científica, `NaN`, `Infinity`, hexadecimal, zeros à esquerda (`01.00`) e mais de 2 casas. Nada é arredondado: uma entrada inválida é recusada.
- **Normalização antes do hash:** o valor entra no hash de idempotência na forma canônica de duas casas (`Money.Amount()`), então `25` e `25.00` produzem o mesmo hash.
- **No contrato JSON**, `amount` é sempre string: um número JSON (`25.00` sem aspas) é recusado na decodificação, então o valor nunca passa por `float` nem no `encoding/json`.
- **No banco:** colunas `BIGINT` de unidades mínimas mais uma coluna `currency`. A conversão `Money ↔ (minor, currency)` fica só no adaptador PostgreSQL.
- Valores negativos existem em cálculos internos (`Sub`, a diferença da reconciliação), mas nunca no saldo: a carteira impede, e o banco também (`CHECK`).
- Operações entre moedas diferentes retornam `ErrCurrencyMismatch`. Os cenários usam BRL, mas o tipo carrega a moeda e há testes de incompatibilidade.

## 4. Modelo de domínio

### Carteira
- Criação (`wallet.Open`) e reidratação (`wallet.Rehydrate`) são separadas: reidratar valida, mas não reaplica movimentos nem gera efeitos.
- `Debit` e `Credit` validam tudo antes de alterar o estado; numa falha, a carteira fica intacta. Devolvem um `Movement` (direção, valor, saldo anterior e posterior), que o caso de uso transforma em lançamento do ledger.
- A versão começa em 1 e sobe exatamente +1 a cada mudança de saldo. `LOSS` não altera a versão.

### Transação de aposta: máquina de estados

```
            NewExternal
                │
                ▼
            PENDING ──────────────┬──────────────┬─────────────┐
                │                 │              │             │
                ▼                 ▼              ▼             ▼
        PENDING_REFERENCE ──▶ PROCESSED      REJECTED       FAILED
           (se repete com       (terminal)   (terminal)    (terminal)
            backoff)  ──────────────────────▶ REJECTED
```

- Estados terminais não mudam mais; a regra está no domínio e também em um trigger do banco.
- **`PENDING` nunca é gravado.** Uma operação sem dependência é aplicada na mesma transação SQL em que é registrada. O único estado intermediário gravado é `PENDING_REFERENCE`, retomado por qualquer instância (seção 7). Por isso não existe "aceite assíncrono" a recuperar.
- **`REJECTED`** é decisão de negócio definitiva (com `failureCode`). **`FAILED`** fica reservado para falha técnica permanente registrada para auditoria. Na implementação atual, falhas técnicas fazem a transação SQL inteira ser desfeita e o cliente repetir (HTTP 503 ou nova entrega SQS), então nada fica em `FAILED`; o estado existe no modelo e no schema.
- **Falha transitória x permanente:** transitória é tudo que pode dar certo numa nova tentativa sem mudar a requisição (banco ou rede indisponível, tempo esgotado, disputa de concorrência). Permanente é o que daria o mesmo resultado em qualquer tentativa (entrada inválida, conflito de idempotência, carteira inexistente).

### Regras por tipo

| Tipo | Efeito | Regras |
|---|---|---|
| `BET` | débito | valor > 0; sem saldo → `INSUFFICIENT_FUNDS`; não aceita referência |
| `WIN` | crédito | valor > 0; referência opcional, que precisa ser uma `BET` |
| `LOSS` | nenhum | valor `0.00`; referência opcional (uma `BET`); sem ledger; não muda a versão; gera `WagerTransactionProcessed` e não `WalletBalanceChanged` |
| `REFUND` | crédito | referência obrigatória a uma `BET` processada; valor igual ao dela |
| `ROLLBACK` | movimento contrário | referência obrigatória a `BET` (vira crédito), `WIN` ou `REFUND` (viram débito); valor igual |
| `OPENING` | crédito inicial | só interno; recusado por HTTP e SQS |

A referência é resolvida por `(providerId, referenceExternalTransactionId)` e precisa concordar em provedor, jogador, carteira, moeda e rodada.

### Códigos de falha (`failureCode`)

| Código | Significado | Corrigível pelo cliente? |
|---|---|---|
| `INSUFFICIENT_FUNDS` | aposta sem saldo | não (resultado definitivo) |
| `REVERSAL_INSUFFICIENT_FUNDS` | `ROLLBACK` de um crédito que deixaria o saldo negativo (código distinto do anterior, como pedido) | não |
| `REFERENCE_NOT_FOUND` | a referência não apareceu dentro do prazo | sim, reenviando com outra chave depois que a referência existir |
| `REFERENCE_NOT_PROCESSED` | a referência existe, mas terminou sem sucesso ou continuou pendente até o prazo | não |
| `REFERENCE_MISMATCH` | a referência é de outro provedor, jogador, carteira, moeda ou rodada | sim (requisição errada) |
| `INVALID_REFERENCE_KIND` | o tipo referenciado não admite a operação | sim |
| `REFERENCE_AMOUNT_MISMATCH` | o valor da reversão difere do referenciado | sim |
| `ALREADY_REVERSED` | a referência já tem uma reversão bem-sucedida | não |
| `CURRENCY_MISMATCH` | moeda diferente da carteira | sim |
| `BALANCE_LIMIT_EXCEEDED` | o crédito ultrapassaria o limite do saldo | não |

"Corrigível" significa que uma nova requisição, com outra chave de idempotência, pode dar certo. A mesma chave sempre devolve o mesmo resultado.

## 5. Transações SQL, locks e concorrência

- **Uma operação = uma transação SQL** (`READ COMMITTED`): registro da inbox (se veio do SQS), transação, saldo novo, lançamento do ledger e eventos da outbox são confirmados juntos ou nenhum é. A delimitação fica no caso de uso (`TxManager.WithinTx`), e os repositórios recebem o `Store` daquela transação; nenhum repositório abre transação própria.
- **Coordenação por carteira, com lock pessimista:** `SELECT ... FROM wallets WHERE id = $1 FOR UPDATE`. Operações da mesma carteira entram em fila; carteiras diferentes seguem em paralelo. Não há lock global.
- **Ordem dos locks:** a inbox é gravada primeiro, depois a carteira é travada, e só então há `INSERT` em `wager_transactions`. A carteira é travada antes de qualquer `INSERT` que a referencie porque a chave estrangeira pega um lock compartilhado na linha da carteira; inserir antes e travar depois faria duas requisições se bloquearem mutuamente (deadlock).
- **Sem lost update:** além do lock, o `UPDATE` confere a versão lida (`WHERE id = $1 AND version = $4`), e um trigger exige que a versão suba exatamente +1 junto com o saldo.
- **Disputa pela mesma operação:** se duas requisições idênticas chegam juntas, a segunda espera o lock da carteira e então encontra a transação da primeira (replay). Se ainda assim houver corrida no `INSERT` (por exemplo, a mesma chave em carteiras diferentes), o `INSERT ... ON CONFLICT DO NOTHING` informa que não inseriu; o caso de uso desfaz e refaz a operação (até 3 vezes) e responde como replay. Essas disputas são contadas na métrica `concurrency_conflicts_total`.
- **Por que `READ COMMITTED` + lock e não `SERIALIZABLE`:** o comportamento é previsível, não há retries de serialização, e o lock dura só uma transação curta.

### Invariantes impostas pelo banco

Independentemente do código da aplicação:

| Invariante | Mecanismo |
|---|---|
| saldo ≥ 0 | `CHECK (balance_minor >= 0)` |
| versão sobe +1 a cada mudança de saldo | trigger `fn_wallets_guard_update` |
| uma carteira por (jogador, moeda) | `UNIQUE (player_id, currency)` |
| idempotência por provedor | `UNIQUE (provider_id, idempotency_key)` e `UNIQUE (provider_id, external_transaction_id)` |
| um único crédito inicial | índice único parcial `WHERE kind = 'OPENING'` |
| no máximo uma reversão bem-sucedida por referência | índice único parcial em `reference_transaction_id` `WHERE kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED'`, mais um `CHECK` que exige a referência resolvida |
| transação coerente com a carteira | chave estrangeira composta `(wallet_id, player_id, currency)` |
| `LOSS` = 0; demais > 0 | `CHECK` |
| interna x externa | `CHECK` de forma: `OPENING` sem provedor/chave/hash/rodada/jogo; externas com todos |
| estado terminal imutável | trigger `fn_wager_transactions_guard_update` |
| ledger: `depois = antes ± valor` | `CHECK` |
| ledger: um lançamento por transação | `UNIQUE (wallet_id, transaction_id)` |
| ledger append-only | triggers que recusam `UPDATE`, `DELETE` e `TRUNCATE` |
| snapshot da outbox imutável | trigger que só permite mudar os campos de publicação |

Os testes de integração tentam violar cada uma diretamente por SQL e conferem o código de erro do Postgres.

## 6. Idempotência

- **Persistente:** a chave fica em `wager_transactions`, com unicidade no banco; sobrevive ao reinício de todos os processos.
- **Escopo:** por provedor. `providerId` vem do token (HTTP) ou do corpo da mensagem (SQS) e faz parte da chave única, então provedores diferentes podem usar a mesma chave sem colisão e nunca veem o resultado um do outro.
- **A chave é usada exatamente como recebida** (`Idempotency-Key` no HTTP, `data.idempotencyKey` no SQS); o servidor nunca a recalcula.
- **Hash do payload:** SHA-256, em hexadecimal, de um JSON canônico (chaves em ordem alfabética, gerado por `encoding/json` a partir de um `map`) com os campos de negócio:
  `amount` (forma canônica de 2 casas), `currency`, `externalId`, `gameId`, `kind`, `playerId`, `providerId`, `referenceExternalId`, `roundId`, `walletId`.
  Ficam de fora: a chave de idempotência, o ID interno e metadados de transporte (headers, `messageId`, `occurredAt`, `correlationId`). O cálculo é a mesma função (`wager.ComputePayloadHash`) para HTTP e SQS.
- **Comportamento:**
  - mesma chave e mesmo hash → devolve o resultado gravado, com `idempotentReplay: true`, inclusive o **saldo observado no processamento original** (coluna `result_balance_minor`) e as recusas de negócio;
  - mesma chave e hash diferente → `409 IDEMPOTENCY_KEY_CONFLICT`;
  - mesmo `(providerId, externalTransactionId)` com outra chave → `409 EXTERNAL_TRANSACTION_ID_CONFLICT` (a operação não pode ser reaplicada com outra chave).
- **HTTP e SQS:** como usam o mesmo caso de uso, a mesma operação chegando pelos dois caminhos é aplicada uma única vez (há teste para isso).

## 7. Referências ainda indisponíveis

- Se a referência não existe ou ainda não terminou, a operação é gravada como `PENDING_REFERENCE`, com `attempts` e `next_attempt_at`, e é emitido `WagerTransactionPendingReference`. A resposta HTTP é `202`.
- O **worker de referências pendentes** (um por instância) busca as pendências vencidas a cada segundo e reavalia cada uma em uma transação. Como o lock da carteira serializa tudo, várias instâncias podem reavaliar a mesma pendência ao mesmo tempo sem duplicar efeito (testado com três). O estado fica no banco, então a retomada funciona depois de reiniciar qualquer instância (também testado: uma instância grava a pendência, para, e outra conclui).
- **Backoff exponencial:** 1 s, 2 s, 4 s... até 1 min (`REFERENCE_BACKOFF_BASE`, `REFERENCE_BACKOFF_MAX`).
- **Prazo (TTL):** 5 minutos desde o registro (`REFERENCE_TTL`). Vencido o prazo, a transação vira `REJECTED` com:
  - `REFERENCE_NOT_FOUND`, se a referência nunca apareceu;
  - `REFERENCE_NOT_PROCESSED`, se ela existe mas continuou pendente.
- Se a referência existe e **terminou sem sucesso** (`REJECTED`/`FAILED`), a rejeição é imediata, com `REFERENCE_NOT_PROCESSED`.
- Na entrada por SQS, a mensagem é concluída e removida assim que a pendência está gravada; a continuidade fica com o worker.

## 8. Reversões

- **Política adotada:** uma transação referenciada pode ter **no máximo uma reversão bem-sucedida, de qualquer tipo**. Uma segunda (`REFUND` depois de `REFUND`, `ROLLBACK` depois de `REFUND` ou o contrário) é rejeitada com `ALREADY_REVERSED`.
- **Interpretação:** o enunciado pede que uma referência não receba duas reversões do mesmo tipo e que a combinação `REFUND` + `ROLLBACK` sobre a mesma aposta não devolva o débito duas vezes. A regra adotada é mais rígida e cobre os dois casos com uma única garantia simples, imposta por um índice único no banco.
- **`ROLLBACK` de um `REFUND`:** é permitido. Ele desfaz o estorno (débito), e não a aposta. Como a aposta já foi revertida pelo `REFUND`, ela não aceita mais nenhuma reversão.
- Reversões são integrais: o valor precisa ser igual ao da referência.
- Um `ROLLBACK` que precisaria debitar mais que o saldo é rejeitado com `REVERSAL_INSUFFICIENT_FUNDS` e fica gravado (auditável), com o evento de rejeição.

## 9. Entrada por SQS (inbox)

- **Fila:** `wager-transactions.fifo`, com DLQ `wager-transactions-dlq.fifo` e redrive após 5 recebimentos (`deploy/aws/init-queues.sh`).
- **Envelope:** o do enunciado (`messageId`, `type: WagerTransactionRequested`, `occurredAt`, `data`). Campos extras são ignorados, para compatibilidade com produtores mais novos; os obrigatórios são validados. `OPENING` é recusado.
- **Inbox:** `(consumer_name, message_id)` com o hash da mensagem (SHA-256 de tipo + chave de idempotência + hash de negócio; não depende de espaços nem da ordem dos campos). O registro entra **na mesma transação SQL** da operação.
  - Reentrega do mesmo conteúdo: duplicata, nada é reaplicado, a mensagem é removida.
  - Mesmo `messageId` com outro conteúdo: conflito, vai para a DLQ. **O `messageId` precisa ser único de forma global e permanente** para o consumidor (por exemplo, um UUID gerado pelo produtor).
- **Remoção só depois do commit.** Se o processo morrer entre o commit e a remoção, a mensagem reaparece quando a visibilidade expira e é reconhecida como duplicata (testado).
- **Desfechos:**
  - processada, rejeição de negócio ou pendência gravada → removida;
  - erro permanente (mensagem inválida, conflito de chave ou de `messageId`, carteira inexistente ou de outro jogador, moeda diferente) → copiada para a DLQ com o atributo `failureReason` e removida;
  - erro transitório → permanece, com a visibilidade ajustada por backoff exponencial (2 s, 4 s, 8 s... até 1 min) via `ChangeMessageVisibility`;
  - 5º recebimento com erro transitório (`SQS_MAX_RECEIVES`) → DLQ com `failureReason=retries_exhausted`. O redrive da fila faz o mesmo como rede de segurança.
  - O consumidor move para a DLQ ele mesmo (e não só pelo redrive) para uma mensagem inválida não gastar cinco tentativas e para registrar o motivo.
- **Visibility timeout:** 60 s, maior que o limite de tratamento de uma mensagem (15 s), para ela nunca reaparecer para outra instância enquanto é tratada. A configuração recusa valores que quebrem essa relação.
- **Produtores:** `MessageGroupId` = ID da carteira (ordem por carteira, paralelismo entre carteiras); `MessageDeduplicationId` = `messageId` (o SQS descarta reenvios idênticos em até 5 minutos; a garantia permanente é a inbox).
- **SIGTERM:** o consumidor para de buscar mensagens, termina a que está tratando (com prazo próprio, que cabe no prazo de encerramento) e devolve as demais do lote à fila na hora (visibilidade zero).
- **SQS indisponível:** o consumidor tenta de novo com backoff, sem derrubar o processo, e a readiness responde 503.

## 10. Saída por eventos (transactional outbox)

- Os eventos são gravados em `outbox_events` na mesma transação da operação. **Nada é publicado antes do commit.**
- **Publisher** (um por instância, `OutboxRelay`):
  - reserva um lote em uma única instrução: `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED)`, marcando `locked_by` e `locked_until`. Publishers concorrentes **pulam** as linhas que outro está reservando, então dividem o trabalho sem publicar o mesmo evento ao mesmo tempo (testado com quatro);
  - **lease de 30 s:** se o publisher morrer, a reserva vence e outro assume os mesmos eventos (recuperação de trabalho abandonado). Os prazos usam o relógio do banco (`now()`), igual para todas as instâncias;
  - antes de cada publicação, confere se a reserva ainda cobre uma publicação inteira; senão, devolve o resto do lote;
  - **queda entre publicar e confirmar:** o evento é republicado quando a reserva vence, **com o mesmo `eventId`** (entrega pelo menos uma vez; testado);
  - **broker indisponível:** a falha é registrada (`attempts`, `last_error`) e o evento volta com backoff exponencial, sem limite de tentativas. Um evento confirmado no banco nunca é descartado;
  - no encerramento, termina a publicação em andamento e devolve o resto do lote;
  - **publica em lotes de até 10** (`SendMessageBatch`, o limite do SQS) e marca os entregues com um único `UPDATE ... WHERE id = ANY($1)`. Cada evento do lote tem resultado próprio: os recusados voltam com backoff, os outros são confirmados. Encontrado pelo teste de carga ([docs/LOAD_TEST.md](docs/LOAD_TEST.md)): um evento por chamada limita a fila FIFO do SQS a 300 eventos/s, e lotes elevam o teto para 3.000;
  - a reserva usa o índice parcial `(created_at, id) WHERE published_at IS NULL` (migration `00006`), na mesma ordem da consulta: lê só o começo do índice, em vez de ordenar todos os pendentes (54 ms → 0,34 ms com ~250 mil pendentes).
- **Destino:** `wager-events.fifo`.
- **Roteamento:** `MessageGroupId` = `aggregateId` (eventos do mesmo agregado saem em ordem entre si); `MessageDeduplicationId` = `eventId` (republicações em até 5 minutos são descartadas pelo próprio SQS); atributos `eventType` e `aggregateType` para filtrar sem abrir o corpo.
- **Envelope:**
  ```json
  {
    "eventId": "uuid", "eventType": "WalletBalanceChanged", "aggregateType": "wallet",
    "aggregateId": "uuid", "correlationId": "...", "causationId": "... (opcional)",
    "occurredAt": "RFC 3339 UTC", "version": 1, "data": { ... }
  }
  ```
  Tipo e versão são definidos pelo construtor de cada evento (`internal/domain/event`). O `data` é o snapshot gravado, imutável (trigger). Valores monetários são strings decimais.
- **Contrato de consumo:** quem consome deve deduplicar pelo `eventId` e, para saldo, ordenar pelo `walletVersion` do `WalletBalanceChanged`.

## 11. Autenticação e autorização

- **IdP: Keycloak**, provisionado automaticamente pelo realm em `deploy/keycloak/testetecnico-realm.json`. É o IdP sugerido, implementa OAuth 2.0/OIDC por completo e roda localmente sem conta externa. Não há cadastro de senhas nem emissão própria de tokens.
- **Fluxo:** `client_credentials` (comunicação entre serviços). Identidades de teste: `provider-a`, `provider-b` (papel `provider`, com claim `providerId`) e `internal-service` (papel `internal`).
- **Validação na API** (`internal/adapters/auth`), local, sem chamar o Keycloak a cada requisição:
  - assinatura conferida com as chaves públicas do JWKS (em cache, renovadas quando aparece um `kid` novo);
  - só RS256 (recusa `alg: none` e HS256);
  - `iss` igual ao emissor configurado, `aud` contendo `testetecnico-api`, `exp` no futuro;
  - só tokens de acesso (`typ: Bearer`), nunca ID tokens.
- **Emissor fixo:** o Keycloak usa `KC_HOSTNAME=http://localhost:8080`, então o `iss` é o mesmo para tokens pedidos de dentro ou de fora do Docker. A aplicação busca as chaves pela rede interna (`AUTH_JWKS_URL`), separada do emissor esperado (`AUTH_ISSUER_URL`).
- **Modelo de permissões:**

| Operação | `provider` | `internal` |
|---|---|---|
| `POST /wagering/transactions` | sim, só com o próprio `providerId` | não |
| `GET /wagering/transactions/:id` | só transações próprias (as demais respondem 404) | todas |
| `GET /providers/:providerId/wagering/transactions/:ext` | só o próprio `providerId` (os demais respondem 403) | todos |
| `POST /wallets`, `GET /wallets/...`, ledger, reconciliação | não | sim |
| `/health/*`, `/metrics` | públicos | públicos |

- **A identidade determina o `providerId`:** o valor vem do token. O corpo pode repeti-lo, mas, se divergir, a resposta é 403 e o caso de uso nem é chamado. A idempotência é por provedor, então um replay também nunca expõe dado de outro provedor.
- **Consulta por ID de transação de outro provedor responde 404 (e não 403)**, para não confirmar que ela existe. A consulta pela rota com `providerId` alheio responde 403, porque o próprio pedido já é não autorizado.
- Token válido sem papel reconhecido, sem `providerId` ou com os dois papéis → 403.
- **Mensageria:** o acesso às filas é dado por credenciais AWS (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`). As validações de domínio continuam no consumidor (provedor, jogador, carteira, valores), independentemente de quem publicou a mensagem.

## 12. Contrato HTTP: códigos e corpos

Erros têm sempre o corpo `{"error": {"code": "...", "message": "..."}}`, com códigos estáveis.

| Situação | HTTP | Corpo |
|---|---|---|
| Operação processada ou replay | 200 | `{transactionId, status: PROCESSED, balance, idempotentReplay}` |
| Aguardando referência | 202 | `{..., status: PENDING_REFERENCE}`, com `Location` |
| Rejeição de negócio | 422 | `{..., status: REJECTED, failureCode}` |
| Carteira de outro jogador / moeda diferente | 422 | `WALLET_PLAYER_MISMATCH`, `CURRENCY_MISMATCH` |
| Entrada inválida | 400 | `INVALID_REQUEST`, `MISSING_IDEMPOTENCY_KEY`, `INVALID_CURSOR` |
| Credencial ausente, inválida ou expirada | 401 | `UNAUTHENTICATED`, com `WWW-Authenticate` |
| Sem permissão | 403 | `FORBIDDEN` |
| Não encontrado | 404 | `WALLET_NOT_FOUND`, `TRANSACTION_NOT_FOUND` |
| Conflito | 409 | `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_ID_CONFLICT`, `WALLET_ALREADY_EXISTS` |
| Indisponibilidade temporária | 503 | `TEMPORARILY_UNAVAILABLE`, com `Retry-After`; repetir com a mesma chave é seguro |
| Erro inesperado | 500 | `INTERNAL_ERROR` (detalhes só no log) |

**Interpretação:** 422 para rejeição de negócio porque a requisição é válida, mas não pode ser aplicada. Assim, as cinco situações pedidas (entrada inválida, conflito, rejeição de negócio, pendente e indisponibilidade) são distinguíveis só pelo código HTTP.

Outras regras: o corpo é decodificado de forma estrita (campos desconhecidos, mais de um objeto e corpo acima de 1 MiB são recusados); cada requisição tem prazo de 15 s; um pânico vira 500 sem derrubar o processo.

## 13. Composição com Fx e ciclo de vida

- Um `fx.Module` por área (`database`, `auth`, `usecases`, `workers`, `aws`, `outbox`, `sqs`, `http`), com injeção por construtores (`fx.Provide`) e ativação por `fx.Invoke`. As verificações de saúde são coletadas por um grupo do Fx (`group:"health"`): cada módulo contribui com a sua.
- **Inicialização:** a configuração é lida e validada antes do Fx (todos os problemas são listados de uma vez). No `OnStart`, o pool confere que o banco responde e o HTTP abre a porta antes de a aplicação se declarar pronta; banco fora do ar ou porta ocupada impedem a subida.
- **Workers** (`StartWorker`): cada um roda com um contexto próprio, cancelado no `OnStop`, que espera o término até o prazo de encerramento. O registro `Workers` torna o término observável (logs e testes conferem que nenhum ficou rodando).
- **Ordem de encerramento** (o Fx executa os `OnStop` na ordem inversa dos `OnStart`; os módulos foram ordenados para isso):
  1. HTTP para de aceitar conexões e espera as requisições em curso;
  2. consumidor SQS termina a mensagem atual e devolve as outras;
  3. publisher da outbox termina a publicação atual e devolve o resto do lote;
  4. worker de pendências termina a transação atual;
  5. o pool do PostgreSQL é fechado, depois de todos que o usam.
- **Prazos:** encerramento em 20 s (`SHUTDOWN_TIMEOUT`), maior que o tratamento de uma mensagem (15 s) e de uma publicação (5 s); `stop_grace_period` do Compose em 30 s, para o Docker não matar o processo no meio.
- **Migrations:** embutidas no binário (`app migrate up|down|reset|status`), com lock de sessão no Postgres para duas instâncias nunca migrarem ao mesmo tempo. No Compose, o serviço `migrate` roda antes e a aplicação só sobe se ele terminar com sucesso.

## 14. Observabilidade

- **Logs** JSON (`log/slog`), uma linha por requisição e por mensagem, com `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId` e `instanceId`. Não são registrados tokens, segredos nem payloads financeiros. O `X-Correlation-Id` do cliente é aceito e devolvido; sem ele, um é gerado. Health checks e coletas de métricas bem-sucedidos ficam em `debug`.
- **Métricas** Prometheus em `GET /metrics` (prefixo `testetecnico_`):

| Pedido | Métrica |
|---|---|
| resultados por status | `wager_transactions_total{source,status}` |
| erros | `wager_transaction_errors_total{source,reason}` |
| duplicatas | `wager_duplicates_total{source,kind}` |
| retries | `sqs_messages_total{outcome="retry"}`, `pending_reference_retries_total`, `outbox_publish_failures_total` |
| DLQ | `sqs_messages_total{outcome="dlq"}` |
| conflitos de concorrência | `concurrency_conflicts_total` |
| atraso da outbox | `outbox_lag_seconds`, `outbox_pending_events` (medidos no banco a cada coleta: valor global) |
| latência | `wager_processing_duration_seconds{source}` (histograma) |
| divergências de reconciliação | `reconciliation_divergences_total` |

  As métricas são coletadas por decoradores em volta dos casos de uso e por ganchos dos workers; o domínio não conhece o Prometheus. Rótulos têm poucos valores possíveis (nunca IDs).
- **Health:** `/health/live` (processo) e `/health/ready` (PostgreSQL, fila de entrada, DLQ e fila de eventos; 503 se qualquer uma falhar).
- **Reconciliação:** reconstrói o saldo pelo ledger (incluindo a abertura) e compara com o armazenado em **uma única consulta SQL**, que enxerga um único snapshot dos dados. Divergências aparecem na resposta, no log (`level: ERROR`) e na métrica. O saldo nunca é alterado.

## 15. Estratégia de testes

- **Unitários:** `Money` (parsing, escala, limites, overflow, moedas), carteira (incluindo 5000 operações aleatórias conferindo as invariantes), máquina de estados, regras dos cinco tipos, hash, eventos, configuração, contrato HTTP, validação de tokens (com um IdP falso de chaves RSA reais), consumidor e publisher (com dublês do SQS).
- **Integração com infraestrutura real** (containers via testcontainers): PostgreSQL com as migrations reais, MiniStack (SQS) e Keycloak com o realm do projeto. Nada da infraestrutura é substituído por mocks nesses testes.
- **Ponta a ponta** (`internal/e2e`): Keycloak, PostgreSQL e a aplicação montada pelo Fx atendendo HTTP de verdade, com tokens emitidos pelo Keycloak. O realm de teste é o do projeto com a validade do token reduzida a 4 s, para testar um token expirado de verdade.
- **Concorrência:** os testes de integração usam **três instâncias independentes** (pool de conexões e caso de uso próprios, compartilhando só o banco); `scripts/demo-concurrency.sh` repete os cenários principais contra os **três processos** do Compose.
- **Ao final dos cenários financeiros**, os testes conferem que o saldo armazenado é igual à soma dos lançamentos do ledger.

Correspondência com a lista do enunciado:

| Cenário | Onde |
|---|---|
| 1. mesma aposta 50× em paralelo | `postgres_test.go: TestIntegration_FiftyIdenticalRequests_DebitExactlyOnce`; `scripts/demo-concurrency.sh` |
| 2. duas apostas de 80.00 sobre 100.00 | `TestIntegration_TwoSimultaneousBetsOfEightyOnHundred` (15 rodadas); script |
| 3. carteiras distintas em paralelo | `TestIntegration_DifferentWalletsDoNotInterfere` |
| 4. três instâncias | todos os testes de `postgres_test.go`; `TestIntegration_SQS_RedeliveredMessageDebitsOnce` (3 consumidores); script com 3 processos |
| 5. consumidor cai após o commit e antes da remoção | `TestIntegration_SQS_CrashAfterCommitBeforeDelete_RedeliveryIsDeduplicated` |
| 6. dois publishers e recuperação | `TestIntegration_Outbox_TwoPublishersShareTheWork`, `..._RecoveryAfterPublishingButBeforeConfirming`, `TestOutbox_AbandonedClaimIsRecoveredAfterTheLease` |
| 7. reversão antes da referência | `TestIntegration_PendingReference_ResolvedOnceEvenWithConcurrentRetries`, `TestIntegration_SQS_ReversalBeforeReference`, testes de expiração no `processor_test.go` |
| 8. reinício preserva idempotência e pendências | `fxapp_test.go: TestLifecycle_AnotherInstanceResumesPendingWork`; replay entre instâncias |
| HTTP e SQS para a mesma operação | `TestIntegration_SQS_SameOperationFromHTTPAndSQS` |
| credenciais ausentes, inválidas e expiradas com o IdP real | `e2e/auth_test.go: TestE2E_MissingInvalidTamperedAndExpiredCredentialsAreRejected` |
| isolamento entre provedores em consultas e replays | `TestE2E_ProvidersAreIsolatedInQueriesAndReplays`; `wallets_test.go: TestIntegration_TransactionQueries_IsolateProviders` |
| restrição das operações internas, sem efeito financeiro em acesso recusado | `TestE2E_InternalOperationsAreRestricted` |
| composição Fx e encerramento | `fxapp_test.go` |

## 16. Limitações, interpretações e trabalho não concluído

**Limitações conhecidas**
- **Ordem dos eventos publicados:** eventos de agregados diferentes saem em qualquer ordem, e dois eventos do mesmo agregado podem, em casos raros, sair fora de ordem se forem reservados por publishers diferentes. Por isso o contrato de consumo usa `eventId` e `walletVersion`. Ordem total exigiria um publisher por agregado (menos paralelismo).
- **Imutabilidade do ledger por trigger:** o dono da tabela pode desabilitar triggers, e no Compose o usuário `app` é superusuário do banco. O endurecimento seria separar o dono do schema de um papel de execução sem `ALTER`/`TRUNCATE`.
- **Políticas do broker:** o MiniStack aceita qualquer credencial, então o controle de acesso à fila é só configuração. Na AWS real, uma política de IAM daria a este serviço apenas `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility` e `GetQueueUrl` na fila de entrada e `SendMessage` na DLQ e na fila de eventos.
- **Keycloak em modo `start-dev`** (HTTP, banco embutido) e **segredos de desenvolvimento versionados** no realm e no `.env.example`. Valem só para o ambiente local.
- **`/metrics` é público**, como os health checks (expõe só contagens e tempos). Em produção, ficaria numa porta interna.
- **Abertura de carteira não é idempotente por chave:** uma segunda abertura para o mesmo jogador e moeda é recusada (409), que já evita duplicidade, mas o cliente não recebe a carteira original como replay.
- **Testes de integração "com três instâncias"** usam três pools e três casos de uso no mesmo processo de teste (o que prova que nada depende de memória); a demonstração com três processos de verdade é o `scripts/demo-concurrency.sh`.
- **Retenção da outbox:** eventos publicados ficam na tabela (com `published_at`), sem limpeza. O índice da reserva só contém os pendentes, então isso não afeta a publicação, mas a tabela cresce sem limite. Em produção, um job apagaria (ou particionaria por data e descartaria partições) os publicados há mais de N dias.
- **Vazão da outbox no ambiente local:** o MiniStack publica ~130 mensagens/s (processo Python em um núcleo), abaixo do que a API gera sob carga. O atraso da outbox medido localmente reflete o simulador; a análise está em [docs/LOAD_TEST.md](docs/LOAD_TEST.md).
- **Redrive para a DLQ:** o consumidor move as mensagens explicitamente; o redrive configurado na fila não é exercitado por teste automatizado.

**Interpretações adotadas**
- No máximo uma reversão bem-sucedida por referência, de qualquer tipo (seção 8).
- 422 para rejeição de negócio e para carteira/moeda incompatíveis (seção 12).
- `PENDING` não é gravado; operações sem dependência são síncronas (seção 4).
- `WIN` e `LOSS` aceitam referência opcional a uma `BET`, validada quando informada.
- O serviço interno não envia operações de aposta; só provedores.

**Opcionais do enunciado**
- **Teste de carga:** implementado (`cmd/loadtest`, `make loadtest`, `make loadtest-hot`); metodologia, resultados e análise em [docs/LOAD_TEST.md](docs/LOAD_TEST.md).
- **Não implementados:** ledger de partidas dobradas; tracing com OpenTelemetry e dashboards.
