-- Periodic snapshots of the broker's own account figures.
--
-- This table is the account's history as Deriv reports it. It is never
-- derived from the local trade journal: balance comes from the broker's
-- balance call, and equity from that balance plus the live profit of each
-- open contract. Where a value is not reported it stays NULL.

CREATE TABLE IF NOT EXISTS balance_snapshots (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- When Watch Tower read the figures from Deriv.
    taken_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Exactly what the broker reported.
    balance         DOUBLE PRECISION NOT NULL,
    currency        TEXT,

    -- Balance plus the unrealised profit of open contracts. NULL when the
    -- open positions could not be valued.
    equity          DOUBLE PRECISION,

    -- How many contracts were open when the snapshot was taken, and their
    -- combined unrealised profit. Kept so equity can be explained rather
    -- than merely asserted.
    open_contracts  INTEGER,
    open_profit     DOUBLE PRECISION,

    -- Which account the figures belong to, so a change of account is
    -- visible in the history instead of silently shifting the numbers.
    account_id      TEXT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS balance_snapshots_time_idx
    ON balance_snapshots (taken_at DESC);

CREATE INDEX IF NOT EXISTS balance_snapshots_account_idx
    ON balance_snapshots (account_id, taken_at DESC);
