-- +goose Up
-- O índice único de reversões usa reference_transaction_id. Como NULLs não
-- colidem em índices únicos, uma reversão PROCESSED sem referência resolvida
-- furaria a garantia. Esta constraint fecha essa brecha.
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_reversal_has_reference CHECK (
        kind NOT IN ('REFUND', 'ROLLBACK')
        OR status <> 'PROCESSED'
        OR reference_transaction_id IS NOT NULL
    );

-- +goose Down
ALTER TABLE wager_transactions
    DROP CONSTRAINT wager_transactions_reversal_has_reference;
