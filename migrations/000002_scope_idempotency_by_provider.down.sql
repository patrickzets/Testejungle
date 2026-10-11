ALTER TABLE wallets DROP CONSTRAINT chk_wallets_version_positive;
ALTER TABLE wallets ALTER COLUMN version SET DEFAULT 0;

ALTER TABLE wager_transactions DROP CONSTRAINT uq_wager_transactions_provider_idempotency_key;
ALTER TABLE wager_transactions ADD CONSTRAINT uq_wager_transactions_idempotency_key UNIQUE (idempotency_key);
