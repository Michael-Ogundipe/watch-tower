ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS quality_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS quality_category TEXT,
    ADD COLUMN IF NOT EXISTS quality_components TEXT,
    ADD COLUMN IF NOT EXISTS displacement_assessment TEXT,
    ADD COLUMN IF NOT EXISTS premium_discount_state TEXT,
    ADD COLUMN IF NOT EXISTS tp_source TEXT,
    ADD COLUMN IF NOT EXISTS structural_target DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS potential_rr DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS setup_invalidation_reason TEXT;

ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS quality_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS quality_category TEXT,
    ADD COLUMN IF NOT EXISTS quality_components TEXT,
    ADD COLUMN IF NOT EXISTS displacement_assessment TEXT,
    ADD COLUMN IF NOT EXISTS premium_discount_state TEXT,
    ADD COLUMN IF NOT EXISTS tp_source TEXT,
    ADD COLUMN IF NOT EXISTS structural_target DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS potential_rr DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS setup_invalidation_reason TEXT;

CREATE OR REPLACE VIEW trades_with_r AS
SELECT
    t.id, t.csv_row, t.signal_time, t.symbol, t.direction, t.score, t.entry,
    t.stop_loss_price, t.take_profit_price, t.stop_distance_pct, t.stake,
    t.multiplier, t.risk_amount, t.reward_amount, t.contract_id, t.transaction_id,
    t.status, t.exit_time, t.profit, t.error, t.exit_price, t.created_at, t.updated_at,
    CASE WHEN t.risk_amount IS NULL OR t.risk_amount = 0 OR t.profit IS NULL THEN NULL
         ELSE round((t.profit / t.risk_amount)::numeric, 4) END AS r_multiple,
    t.entry_price, t.technical_sl_price, t.technical_tp_price,
    t.technical_stop_distance, t.technical_reward_distance, t.technical_rr,
    t.deriv_stop_loss_amount, t.deriv_take_profit_amount,
    t.broker_stop_loss_value, t.broker_take_profit_value,
    t.contract_created_at, t.contract_updated_at, t.final_pl,
    t.quality_score, t.quality_category, t.quality_components,
    t.displacement_assessment, t.premium_discount_state, t.tp_source,
    t.structural_target, t.potential_rr, t.setup_invalidation_reason
FROM trades t;
