-- Watch Tower persistence, first migration.
--
-- Mirrors the two CSV journals the live trader already writes. Every
-- strategy-derived column is nullable, because the pipeline does not
-- always produce every value and nothing here is invented to fill a gap.
--
-- Applied by migrate.py, which records what it has run and never drops or
-- rewrites an existing object.

CREATE TABLE IF NOT EXISTS signals (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Row index in confluence_history.csv, so an update lands on the same
    -- row in both places.
    csv_row         INTEGER UNIQUE NOT NULL,

    signal_time     TEXT,
    symbol          TEXT,
    timeframe       TEXT,
    direction       TEXT,
    score           DOUBLE PRECISION,

    -- Confluence components, exactly as the engine reports them.
    h1_bias         TEXT,
    liquidity_sweep TEXT,
    m15_structure   TEXT,
    m5_fvg          TEXT,
    m5_order_block  TEXT,

    -- Setup / zone.
    ob_lower        DOUBLE PRECISION,
    ob_upper        DOUBLE PRECISION,
    setup_id        TEXT,

    -- Lifecycle.
    status          TEXT,
    rejection_reason TEXT,

    -- Trade information when the signal became a trade.
    entry           DOUBLE PRECISION,
    stop_loss       DOUBLE PRECISION,
    take_profit     DOUBLE PRECISION,
    contract_id     TEXT,
    outcome         TEXT,
    profit          DOUBLE PRECISION,

    entry_time      TEXT,
    exit_time       TEXT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS signals_symbol_idx     ON signals (symbol);
CREATE INDEX IF NOT EXISTS signals_status_idx     ON signals (status);
CREATE INDEX IF NOT EXISTS signals_timeframe_idx  ON signals (timeframe);
CREATE INDEX IF NOT EXISTS signals_time_idx       ON signals (signal_time DESC);

CREATE TABLE IF NOT EXISTS trades (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Row index in trade_history.csv.
    csv_row           INTEGER UNIQUE NOT NULL,

    signal_time       TEXT,
    symbol            TEXT,
    direction         TEXT,
    score             DOUBLE PRECISION,

    entry             DOUBLE PRECISION,
    stop_loss_price   DOUBLE PRECISION,
    take_profit_price DOUBLE PRECISION,
    stop_distance_pct DOUBLE PRECISION,

    stake             DOUBLE PRECISION,
    multiplier        DOUBLE PRECISION,
    risk_amount       DOUBLE PRECISION,
    reward_amount     DOUBLE PRECISION,

    contract_id       TEXT,
    transaction_id    TEXT,

    -- ORDER_SUBMITTED / OPEN / WIN / LOSS / BREAKEVEN / error states, as the
    -- live trader already writes them.
    status            TEXT,
    exit_time         TEXT,
    profit            DOUBLE PRECISION,
    error             TEXT,

    -- Deriv reports profit, not an exit price, so this stays empty until the
    -- pipeline produces one. It is never inferred.
    exit_price        DOUBLE PRECISION,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS trades_symbol_idx    ON trades (symbol);
CREATE INDEX IF NOT EXISTS trades_status_idx    ON trades (status);
CREATE INDEX IF NOT EXISTS trades_direction_idx ON trades (direction);
CREATE INDEX IF NOT EXISTS trades_time_idx      ON trades (signal_time DESC);

-- R multiple is not stored by the pipeline. It is derived here from two
-- values that do exist, so the dashboard reads one consistent definition
-- rather than each caller inventing its own.
CREATE OR REPLACE VIEW trades_with_r AS
SELECT
    t.*,
    CASE
        WHEN t.risk_amount IS NULL OR t.risk_amount = 0 OR t.profit IS NULL THEN NULL
        ELSE round((t.profit / t.risk_amount)::numeric, 4)
    END AS r_multiple
FROM trades t;
