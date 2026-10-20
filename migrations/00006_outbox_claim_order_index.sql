-- O publisher reserva eventos com ORDER BY created_at, id LIMIT n. O índice
-- antigo (next_attempt_at) não serve essa ordem: o Postgres lia e ordenava
-- todos os pendentes a cada reserva (Parallel Seq Scan + Sort; ~54 ms com
-- ~250 mil pendentes, crescendo com o acúmulo). Com o índice na mesma ordem da
-- consulta, a reserva lê só o começo do índice e para no LIMIT.
-- Também atende MIN(created_at) da métrica de atraso da outbox.
--
-- CONCURRENTLY não bloqueia escritas durante a criação, mas não pode rodar
-- dentro de transação; por isso NO TRANSACTION.

-- +goose NO TRANSACTION

-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS outbox_events_pending_order_idx
    ON outbox_events (created_at, id)
    WHERE published_at IS NULL;

-- O índice antigo deixa de ser usado pela reserva.
DROP INDEX CONCURRENTLY IF EXISTS outbox_events_pending_idx;

-- +goose Down
CREATE INDEX CONCURRENTLY IF NOT EXISTS outbox_events_pending_idx
    ON outbox_events (next_attempt_at)
    WHERE published_at IS NULL;

DROP INDEX CONCURRENTLY IF EXISTS outbox_events_pending_order_idx;
