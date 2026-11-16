-- Passive WatchTower health snapshots and structured event timeline.
-- Bounded by the application writer; no trading tables are changed.
CREATE TABLE IF NOT EXISTS operational_events (
    event_id UUID PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL,
    instance_id TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('INFO', 'WARN', 'ERROR')),
    component TEXT NOT NULL,
    event_type TEXT NOT NULL,
    symbol TEXT,
    timeframe TEXT,
    signal_id TEXT,
    trade_id TEXT,
    contract_id TEXT,
    message TEXT NOT NULL DEFAULT '',
    reason TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS operational_events_time_idx
    ON operational_events (occurred_at DESC, event_id);
CREATE INDEX IF NOT EXISTS operational_events_filter_idx
    ON operational_events (instance_id, severity, component, event_type, occurred_at DESC);
CREATE INDEX IF NOT EXISTS operational_events_symbol_idx
    ON operational_events (symbol, timeframe, occurred_at DESC);

CREATE TABLE IF NOT EXISTS runtime_status (
    instance_id TEXT NOT NULL,
    component TEXT NOT NULL,
    state JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instance_id, component)
);
