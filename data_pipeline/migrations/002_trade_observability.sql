-- Add explicit technical, Deriv monetary, broker value, and contract timing
-- fields while keeping all legacy trade columns and API keys unchanged.
ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS entry_price DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS technical_sl_price DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS technical_tp_price DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS technical_stop_distance DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS technical_reward_distance DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS technical_rr DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS deriv_stop_loss_amount DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS deriv_take_profit_amount DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS broker_stop_loss_value DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS broker_take_profit_value DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS contract_created_at TEXT,
    ADD COLUMN IF NOT EXISTS contract_updated_at TEXT,
    ADD COLUMN IF NOT EXISTS final_pl DOUBLE PRECISION;

-- Preserve the established trades_with_r column order (including r_multiple)
-- and append the new fields so existing API consumers remain compatible.
CREATE OR REPLACE VIEW trades_with_r AS
SELECT
    t.id,
    t.csv_row,
    t.signal_time,
    t.symbol,
    t.direction,
    t.score,
    t.entry,
    t.stop_loss_price,
    t.take_profit_price,
    t.stop_distance_pct,
    t.stake,
    t.multiplier,
    t.risk_amount,
    t.reward_amount,
    t.contract_id,
    t.transaction_id,
    t.status,
    t.exit_time,
    t.profit,
    t.error,
    t.exit_price,
    t.created_at,
    t.updated_at,
    CASE
        WHEN t.risk_amount IS NULL OR t.risk_amount = 0 OR t.profit IS NULL THEN NULL
        ELSE round((t.profit / t.risk_amount)::numeric, 4)
    END AS r_multiple,
    t.entry_price,
    t.technical_sl_price,
    t.technical_tp_price,
    t.technical_stop_distance,
    t.technical_reward_distance,
    t.technical_rr,
    t.deriv_stop_loss_amount,
    t.deriv_take_profit_amount,
    t.broker_stop_loss_value,
    t.broker_take_profit_value,
    t.contract_created_at,
    t.contract_updated_at,
    t.final_pl
FROM trades t;
