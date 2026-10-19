-- +goose Up
-- Inbox: deduplicação durável das mensagens recebidas do SQS, por consumidor.
CREATE TABLE inbox_messages (
    consumer_name text        NOT NULL,
    message_id    text        NOT NULL,
    payload_hash  text        NOT NULL,
    received_at   timestamptz NOT NULL DEFAULT now(),
    completed_at  timestamptz,
    PRIMARY KEY (consumer_name, message_id)
);

-- Outbox: eventos gravados na mesma transação do negócio e publicados depois.
CREATE TABLE outbox_events (
    id              uuid        PRIMARY KEY,   -- eventId estável (não muda em republicações)
    aggregate_type  text        NOT NULL,
    aggregate_id    uuid        NOT NULL,
    event_type      text        NOT NULL,
    event_version   integer     NOT NULL DEFAULT 1,
    correlation_id  text        NOT NULL,
    causation_id    text,
    payload         jsonb       NOT NULL,
    occurred_at     timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),

    -- controle de publicação (retry com backoff e "lease" para vários publishers)
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    locked_by       text,
    locked_until    timestamptz,
    last_error      text,
    published_at    timestamptz,

    CONSTRAINT outbox_event_type_check CHECK (event_type IN (
        'WagerTransactionProcessed',
        'WagerTransactionRejected',
        'WalletBalanceChanged',
        'WagerTransactionPendingReference'
    )),
    CONSTRAINT outbox_attempts_check CHECK (attempts >= 0)
);

-- Só o que ainda não foi publicado entra no índice.
CREATE INDEX outbox_events_pending_idx
    ON outbox_events (next_attempt_at)
    WHERE published_at IS NULL;

-- O snapshot do evento é imutável; só os campos de publicação podem mudar.
-- +goose StatementBegin
CREATE FUNCTION fn_outbox_guard_update() RETURNS trigger AS $$
BEGIN
    IF NEW.id <> OLD.id
       OR NEW.aggregate_type <> OLD.aggregate_type
       OR NEW.aggregate_id <> OLD.aggregate_id
       OR NEW.event_type <> OLD.event_type
       OR NEW.event_version <> OLD.event_version
       OR NEW.correlation_id <> OLD.correlation_id
       OR NEW.causation_id IS DISTINCT FROM OLD.causation_id
       OR NEW.payload <> OLD.payload
       OR NEW.occurred_at <> OLD.occurred_at THEN
        RAISE EXCEPTION 'outbox event % snapshot is immutable', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER outbox_guard_update
    BEFORE UPDATE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION fn_outbox_guard_update();

-- +goose Down
DROP TABLE outbox_events;
DROP FUNCTION fn_outbox_guard_update();
DROP TABLE inbox_messages;
