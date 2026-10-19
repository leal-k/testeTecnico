-- +goose Up
CREATE TABLE wallet_ledger_entries (
    id                   uuid        PRIMARY KEY,
    -- ordem estável para paginação por cursor (sequência crescente)
    seq                  bigint      GENERATED ALWAYS AS IDENTITY,
    wallet_id            uuid        NOT NULL REFERENCES wallets (id),
    transaction_id       uuid        NOT NULL REFERENCES wager_transactions (id),
    direction            text        NOT NULL,
    amount_minor         bigint      NOT NULL,
    currency             text        NOT NULL,
    balance_before_minor bigint      NOT NULL,
    balance_after_minor  bigint      NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT ledger_direction_check  CHECK (direction IN ('DEBIT', 'CREDIT')),
    CONSTRAINT ledger_currency_check   CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ledger_amount_positive  CHECK (amount_minor > 0),
    CONSTRAINT ledger_balances_non_negative CHECK (balance_before_minor >= 0 AND balance_after_minor >= 0),

    -- balance_after = balance_before ± valor, conforme a direção
    CONSTRAINT ledger_balance_arithmetic CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR
        (direction = 'DEBIT'  AND balance_after_minor = balance_before_minor - amount_minor)
    ),

    -- no máximo um lançamento por transação em cada carteira
    CONSTRAINT ledger_wallet_transaction_key UNIQUE (wallet_id, transaction_id)
);

CREATE INDEX ledger_wallet_seq_idx ON wallet_ledger_entries (wallet_id, seq);

-- Append-only: bloqueia UPDATE, DELETE e TRUNCATE.
-- +goose StatementBegin
CREATE FUNCTION fn_ledger_forbid_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only (% is not allowed)', TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER ledger_forbid_update_delete
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION fn_ledger_forbid_mutation();

CREATE TRIGGER ledger_forbid_truncate
    BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION fn_ledger_forbid_mutation();

-- +goose Down
DROP TABLE wallet_ledger_entries;
DROP FUNCTION fn_ledger_forbid_mutation();
