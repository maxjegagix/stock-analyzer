CREATE OR REPLACE VIEW stock.v_screening_result_rule_detail AS
SELECT
    r.run_id,
    x.formula_code,
    x.as_of_date AS trade_date,
    r.stock_code,
    r.rank_no,
    r.passed,
    r.passed_required_rules,
    r.total_score,
    r.max_score,
    r.score_pct,
    (d->>'rule_id')::BIGINT AS rule_id,
    d->>'rule_code' AS rule_code,
    d->>'metric_key' AS metric_key,
    d->>'operator' AS operator,
    NULLIF(d->>'threshold_min', '')::NUMERIC AS threshold_min,
    NULLIF(d->>'threshold_max', '')::NUMERIC AS threshold_max,
    NULLIF(d->>'metric_value', '')::NUMERIC AS metric_value,
    (d->>'is_required')::BOOLEAN AS is_required,
    (d->>'is_hit')::BOOLEAN AS is_hit,
    NULLIF(d->>'weight', '')::NUMERIC AS weight,
    NULLIF(d->>'score_if_true', '')::NUMERIC AS score_if_true,
    NULLIF(d->>'score_obtained', '')::NUMERIC AS score_obtained
FROM stock.screening_result_tx r
JOIN stock.screening_run_tx x
    ON x.run_id = r.run_id
CROSS JOIN LATERAL jsonb_array_elements(r.rule_detail) AS d;

CREATE INDEX IF NOT EXISTS idx_screening_result_tx_run_rank
    ON stock.screening_result_tx (run_id, rank_no);
