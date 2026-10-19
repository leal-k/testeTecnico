-- +goose Up
CREATE TABLE wager_transactions (
    id                                uuid        PRIMARY KEY,
    origin                            text        NOT NULL,
    kind                              text        NOT NULL,
    status                            text        NOT NULL DEFAULT 'PENDING',

    -- campos externos (nulos em OPENING)
    provider_id                       text,
    external_transaction_id           text,
    idempotency_key                   text,
    payload_hash                      text,
    round_id                          text,
    game_id                           text,
    reference_external_transaction_id text,

    wallet_id                         uuid        NOT NULL,
    player_id                         uuid        NOT NULL,
    currency                          text        NOT NULL,
    amount_minor                      bigint      NOT NULL,

    -- referência interna resolvida (REFUND/ROLLBACK e WIN com referência)
    reference_transaction_id          uuid        REFERENCES wager_transactions (id),

    failure_code                      text,
    -- saldo observado ao concluir; é o que o replay devolve
    result_balance_minor              bigint,

    -- retomada de PENDING / PENDING_REFERENCE
    attempts                          integer     NOT NULL DEFAULT 0,
    next_attempt_at                   timestamptz,

    created_at                        timestamptz NOT NULL DEFAULT now(),
    updated_at                        timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT wager_transactions_origin_check   CHECK (origin IN ('EXTERNAL', 'INTERNAL')),
    CONSTRAINT wager_transactions_kind_check     CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    CONSTRAINT wager_transactions_status_check   CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    CONSTRAINT wager_transactions_currency_check CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT wager_transactions_attempts_check CHECK (attempts >= 0),

    -- LOSS exige zero; todos os demais exigem valor positivo
    CONSTRAINT wager_transactions_amount_check CHECK (
        (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)
    ),

    -- operação interna (OPENING) x externa (HTTP/SQS)
    CONSTRAINT wager_transactions_origin_shape CHECK (
        (origin = 'INTERNAL'
            AND kind = 'OPENING'
            AND provider_id IS NULL
            AND external_transaction_id IS NULL
            AND idempotency_key IS NULL
            AND payload_hash IS NULL
            AND round_id IS NULL
            AND game_id IS NULL
            AND reference_external_transaction_id IS NULL
            AND reference_transaction_id IS NULL)
        OR
        (origin = 'EXTERNAL'
            AND kind <> 'OPENING'
            AND provider_id IS NOT NULL
            AND external_transaction_id IS NOT NULL
            AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL
            AND round_id IS NOT NULL
            AND game_id IS NOT NULL)
    ),

    CONSTRAINT wager_transactions_reference_required CHECK (
        kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL
    ),
    CONSTRAINT wager_transactions_failure_code_required CHECK (
        status NOT IN ('REJECTED', 'FAILED') OR failure_code IS NOT NULL
    ),
    CONSTRAINT wager_transactions_result_balance_check CHECK (
        result_balance_minor IS NULL OR result_balance_minor >= 0
    ),
    CONSTRAINT wager_transactions_processed_has_result CHECK (
        status <> 'PROCESSED' OR result_balance_minor IS NOT NULL
    ),

    -- jogador e moeda precisam coincidir com os da carteira
    CONSTRAINT wager_transactions_wallet_fk
        FOREIGN KEY (wallet_id, player_id, currency)
        REFERENCES wallets (id, player_id, currency),

    -- idempotência persistente (NULLs não colidem, então OPENING fica de fora)
    CONSTRAINT wager_transactions_provider_external_key    UNIQUE (provider_id, external_transaction_id),
    CONSTRAINT wager_transactions_provider_idempotency_key UNIQUE (provider_id, idempotency_key)
);

-- No máximo um crédito inicial por carteira.
CREATE UNIQUE INDEX wager_transactions_one_opening_per_wallet
    ON wager_transactions (wallet_id)
    WHERE kind = 'OPENING';

-- Uma referência só pode ter UMA reversão bem-sucedida (REFUND ou ROLLBACK).
-- Barra REFUND duplicado, ROLLBACK duplicado e REFUND + ROLLBACK da mesma aposta.
CREATE UNIQUE INDEX wager_transactions_one_reversal_per_reference
    ON wager_transactions (reference_transaction_id)
    WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED';

-- Fila de trabalho: o que ainda precisa ser (re)processado.
CREATE INDEX wager_transactions_pending_idx
    ON wager_transactions (next_attempt_at)
    WHERE status IN ('PENDING', 'PENDING_REFERENCE');

-- Estado terminal não muda; não volta para PENDING; campos de negócio são imutáveis.
-- +goose StatementBegin
CREATE FUNCTION fn_wager_transactions_guard_update() RETURNS trigger AS $$
BEGIN
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'transaction % is in terminal state % and cannot change', OLD.id, OLD.status
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;

    IF NEW.status = 'PENDING' AND OLD.status <> 'PENDING' THEN
        RAISE EXCEPTION 'transaction % cannot go back to PENDING from %', OLD.id, OLD.status
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;

    IF NEW.id <> OLD.id
       OR NEW.origin <> OLD.origin
       OR NEW.kind <> OLD.kind
       OR NEW.wallet_id <> OLD.wallet_id
       OR NEW.player_id <> OLD.player_id
       OR NEW.currency <> OLD.currency
       OR NEW.amount_minor <> OLD.amount_minor
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.external_transaction_id IS DISTINCT FROM OLD.external_transaction_id
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash THEN
        RAISE EXCEPTION 'business fields of transaction % are immutable', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;

    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER wager_transactions_guard_update
    BEFORE UPDATE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION fn_wager_transactions_guard_update();

-- +goose Down
DROP TABLE wager_transactions;
DROP FUNCTION fn_wager_transactions_guard_update();
