-- +goose Up
-- Dinheiro em unidades mínimas (centavos) como BIGINT; a moeda vai ao lado (ISO 4217).
CREATE TABLE wallets (
    id            uuid        PRIMARY KEY,
    player_id     uuid        NOT NULL,
    currency      text        NOT NULL,
    balance_minor bigint      NOT NULL,
    version       bigint      NOT NULL DEFAULT 1,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT wallets_currency_format      CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT wallets_balance_non_negative CHECK (balance_minor >= 0),
    CONSTRAINT wallets_version_positive     CHECK (version >= 1),
    -- uma carteira por (jogador, moeda)
    CONSTRAINT wallets_player_currency_key  UNIQUE (player_id, currency),
    -- alvo da FK composta das transações (garante jogador e moeda coerentes)
    CONSTRAINT wallets_identity_key         UNIQUE (id, player_id, currency)
);

-- Identidade imutável; a versão só sobe (+1) junto com uma mudança de saldo.
-- +goose StatementBegin
CREATE FUNCTION fn_wallets_guard_update() RETURNS trigger AS $$
BEGIN
    IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id OR NEW.currency <> OLD.currency THEN
        RAISE EXCEPTION 'wallet identity is immutable'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;

    IF NEW.balance_minor <> OLD.balance_minor THEN
        IF NEW.version <> OLD.version + 1 THEN
            RAISE EXCEPTION 'wallet version must increase by 1 when the balance changes (old=%, new=%)',
                OLD.version, NEW.version
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    ELSIF NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'wallet version can only change together with the balance'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;

    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER wallets_guard_update
    BEFORE UPDATE ON wallets
    FOR EACH ROW EXECUTE FUNCTION fn_wallets_guard_update();

-- +goose Down
DROP TABLE wallets;
DROP FUNCTION fn_wallets_guard_update();
