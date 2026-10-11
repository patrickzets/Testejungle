ALTER TABLE wager_transactions DROP CONSTRAINT uq_wager_transactions_idempotency_key;
ALTER TABLE wager_transactions ADD CONSTRAINT uq_wager_transactions_provider_idempotency_key UNIQUE (provider_id, idempotency_key);

ALTER TABLE wallets ALTER COLUMN version SET DEFAULT 1;
ALTER TABLE wallets ADD CONSTRAINT chk_wallets_version_positive CHECK (version >= 1);
