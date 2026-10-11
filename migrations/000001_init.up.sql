-- 000001_init.up.sql

CREATE TABLE wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id UUID NOT NULL,
    currency CHAR(3) NOT NULL,
    balance_minor BIGINT NOT NULL CHECK (balance_minor >= 0),
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wallets_player_currency UNIQUE (player_id, currency)
);

CREATE TABLE wager_transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    origin VARCHAR(16) NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    provider_id VARCHAR(64),
    external_transaction_id VARCHAR(128),
    idempotency_key VARCHAR(128),
    payload_hash VARCHAR(64),
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    player_id UUID NOT NULL,
    round_id VARCHAR(128),
    game_id VARCHAR(128),
    kind VARCHAR(16) NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor >= 0),
    currency CHAR(3) NOT NULL,
    reference_external_transaction_id VARCHAR(128),
    reference_transaction_id UUID REFERENCES wager_transactions(id),
    status VARCHAR(32) NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    failure_code VARCHAR(64),
    result_balance_minor BIGINT,
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    first_pending_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wager_transactions_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT uq_wager_transactions_provider_external UNIQUE (provider_id, external_transaction_id),
    CONSTRAINT chk_wager_transactions_origin_shape CHECK (
        (
            origin = 'INTERNAL' AND
            kind = 'OPENING' AND
            provider_id IS NULL AND
            external_transaction_id IS NULL AND
            idempotency_key IS NULL AND
            payload_hash IS NULL AND
            round_id IS NULL AND
            game_id IS NULL AND
            reference_external_transaction_id IS NULL AND
            reference_transaction_id IS NULL
        ) OR (
            origin = 'EXTERNAL' AND
            kind != 'OPENING' AND
            provider_id IS NOT NULL AND
            external_transaction_id IS NOT NULL AND
            idempotency_key IS NOT NULL AND
            payload_hash IS NOT NULL AND
            round_id IS NOT NULL AND
            game_id IS NOT NULL
        )
    )
);

CREATE UNIQUE INDEX uq_wager_transactions_one_opening_per_wallet
    ON wager_transactions (wallet_id)
    WHERE kind = 'OPENING';

CREATE UNIQUE INDEX uq_wager_transactions_reversal_ref
    ON wager_transactions (reference_transaction_id)
    WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED';

CREATE INDEX idx_wager_transactions_resolver
    ON wager_transactions (next_attempt_at)
    WHERE status IN ('PENDING', 'PENDING_REFERENCE');

CREATE TABLE wallet_ledger_entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    seq BIGSERIAL NOT NULL,
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    transaction_id UUID NOT NULL REFERENCES wager_transactions(id),
    direction VARCHAR(16) NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor >= 0),
    balance_before BIGINT NOT NULL CHECK (balance_before >= 0),
    balance_after BIGINT NOT NULL CHECK (balance_after >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_wallet_ledger_wallet_tx UNIQUE (wallet_id, transaction_id),
    CONSTRAINT chk_wallet_ledger_balance_direction CHECK (
        (direction = 'CREDIT' AND balance_after = balance_before + amount_minor) OR
        (direction = 'DEBIT'  AND balance_after = balance_before - amount_minor)
    )
);

CREATE INDEX idx_wallet_ledger_wallet_seq ON wallet_ledger_entries (wallet_id, seq);

CREATE OR REPLACE FUNCTION prevent_ledger_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only: % is not allowed', TG_OP
        USING ERRCODE = '0A000';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ledger_no_update_delete
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW
    EXECUTE FUNCTION prevent_ledger_mutation();

CREATE TRIGGER trg_ledger_no_truncate
    BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT
    EXECUTE FUNCTION prevent_ledger_mutation();

CREATE TABLE inbox (
    consumer_name VARCHAR(64) NOT NULL,
    message_id VARCHAR(128) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE outbox (
    event_id UUID PRIMARY KEY,
    aggregate_id VARCHAR(128) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    locked_until TIMESTAMPTZ,
    locked_by VARCHAR(128)
);

CREATE INDEX idx_outbox_claimable
    ON outbox (next_attempt_at, locked_until)
    WHERE published_at IS NULL;
