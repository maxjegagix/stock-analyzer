CREATE TABLE IF NOT EXISTS stock.screening_formula_master
(
    formula_id       BIGSERIAL PRIMARY KEY,
    formula_code     VARCHAR(64) NOT NULL UNIQUE,
    formula_name     TEXT        NOT NULL,
    description      TEXT,
    min_total_score  NUMERIC(18, 6) NOT NULL DEFAULT 0,
    is_active        BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stock.screening_formula_rule
(
    rule_id          BIGSERIAL PRIMARY KEY,
    formula_id       BIGINT      NOT NULL REFERENCES stock.screening_formula_master (formula_id) ON DELETE CASCADE,
    rule_code        VARCHAR(64) NOT NULL,
    metric_key       VARCHAR(64) NOT NULL,
    operator         VARCHAR(16) NOT NULL CHECK (operator IN ('>', '>=', '<', '<=', '=', 'between')),
    threshold_min    NUMERIC(28, 10),
    threshold_max    NUMERIC(28, 10),
    weight           NUMERIC(18, 6) NOT NULL DEFAULT 1,
    score_if_true    NUMERIC(18, 6) NOT NULL DEFAULT 1,
    is_required      BOOLEAN     NOT NULL DEFAULT FALSE,
    is_active        BOOLEAN     NOT NULL DEFAULT TRUE,
    rule_order       INTEGER     NOT NULL DEFAULT 100,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (formula_id, rule_code),
    CHECK (
        (operator = 'between' AND threshold_min IS NOT NULL AND threshold_max IS NOT NULL)
        OR (operator <> 'between' AND threshold_min IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS stock.screening_run_tx
(
    run_id              BIGSERIAL PRIMARY KEY,
    formula_id          BIGINT      NOT NULL REFERENCES stock.screening_formula_master (formula_id),
    formula_code        VARCHAR(64) NOT NULL,
    as_of_date          DATE        NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    status              VARCHAR(16) NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'failed')),
    total_candidates    INTEGER,
    total_passed        INTEGER,
    notes               TEXT,
    error_message       TEXT
);

CREATE TABLE IF NOT EXISTS stock.screening_result_tx
(
    result_id             BIGSERIAL PRIMARY KEY,
    run_id                BIGINT      NOT NULL REFERENCES stock.screening_run_tx (run_id) ON DELETE CASCADE,
    formula_id            BIGINT      NOT NULL REFERENCES stock.screening_formula_master (formula_id),
    trade_date            DATE        NOT NULL,
    stock_code            VARCHAR(10) NOT NULL,
    total_score           NUMERIC(18, 6) NOT NULL,
    max_score             NUMERIC(18, 6) NOT NULL,
    score_pct             NUMERIC(18, 6) NOT NULL,
    passed                BOOLEAN     NOT NULL,
    passed_required_rules BOOLEAN     NOT NULL,
    rule_hit_count        INTEGER     NOT NULL,
    required_hit_count    INTEGER     NOT NULL,
    rank_no               INTEGER,
    rule_detail           JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, stock_code)
);

CREATE INDEX IF NOT EXISTS idx_screening_rule_formula_active
    ON stock.screening_formula_rule (formula_id, is_active, rule_order);

CREATE INDEX IF NOT EXISTS idx_screening_result_trade_date
    ON stock.screening_result_tx (trade_date, stock_code);

CREATE INDEX IF NOT EXISTS idx_screening_run_formula_date
    ON stock.screening_run_tx (formula_code, as_of_date);

CREATE OR REPLACE FUNCTION stock.run_stock_screening(
    p_formula_code VARCHAR,
    p_trade_date DATE DEFAULT CURRENT_DATE
)
RETURNS BIGINT
LANGUAGE plpgsql
AS
$$
DECLARE
    v_run_id BIGINT;
    v_formula_id BIGINT;
    v_min_total_score NUMERIC(18, 6);
BEGIN
    SELECT formula_id, min_total_score
    INTO v_formula_id, v_min_total_score
    FROM stock.screening_formula_master
    WHERE formula_code = p_formula_code
      AND is_active = TRUE;

    IF v_formula_id IS NULL THEN
        RAISE EXCEPTION 'Formula % tidak ditemukan atau tidak aktif', p_formula_code;
    END IF;

    INSERT INTO stock.screening_run_tx (
        formula_id,
        formula_code,
        as_of_date,
        status
    ) VALUES (
        v_formula_id,
        p_formula_code,
        p_trade_date,
        'running'
    )
    RETURNING run_id INTO v_run_id;

    WITH rules AS (
        SELECT
            r.rule_id,
            r.rule_code,
            r.metric_key,
            r.operator,
            r.threshold_min,
            r.threshold_max,
            r.weight,
            r.score_if_true,
            r.is_required,
            r.rule_order
        FROM stock.screening_formula_rule r
        WHERE r.formula_id = v_formula_id
          AND r.is_active = TRUE
    ),
    base AS (
        SELECT *
        FROM stock.v_summary_features_daily
        WHERE trade_date = p_trade_date
    ),
    evaluated AS (
        SELECT
            b.trade_date,
            b.stock_code,
            r.rule_id,
            r.rule_code,
            r.metric_key,
            r.operator,
            r.threshold_min,
            r.threshold_max,
            r.weight,
            r.score_if_true,
            r.is_required,
            r.rule_order,
            CASE r.metric_key
                WHEN 'ret_1d_pct' THEN b.ret_1d_pct
                WHEN 'gap_open_pct' THEN b.gap_open_pct
                WHEN 'intraday_return_pct' THEN b.intraday_return_pct
                WHEN 'range_pct' THEN b.range_pct
                WHEN 'mom_5d_pct' THEN b.mom_5d_pct
                WHEN 'mom_20d_pct' THEN b.mom_20d_pct
                WHEN 'mom_60d_pct' THEN b.mom_60d_pct
                WHEN 'ret_vol_20d' THEN b.ret_vol_20d
                WHEN 'avg_trade_value' THEN b.avg_trade_value
                WHEN 'turnover_float_pct' THEN b.turnover_float_pct
                WHEN 'turnover_listed_pct' THEN b.turnover_listed_pct
                WHEN 'volume_spike_ratio_20d' THEN b.volume_spike_ratio_20d
                WHEN 'value_spike_ratio_20d' THEN b.value_spike_ratio_20d
                WHEN 'spread_pct' THEN b.spread_pct
                WHEN 'queue_imbalance' THEN b.queue_imbalance
                WHEN 'microprice_proxy' THEN b.microprice_proxy
                WHEN 'net_foreign' THEN b.net_foreign
                WHEN 'foreign_participation_pct' THEN b.foreign_participation_pct
                WHEN 'net_foreign_intensity_pct' THEN b.net_foreign_intensity_pct
                WHEN 'non_regular_value_ratio_pct' THEN b.non_regular_value_ratio_pct
                WHEN 'non_regular_volume_ratio_pct' THEN b.non_regular_volume_ratio_pct
                WHEN 'non_regular_frequency_ratio_pct' THEN b.non_regular_frequency_ratio_pct
                WHEN 'index_contribution_proxy' THEN b.index_contribution_proxy
                WHEN 'check_change_diff' THEN b.check_change_diff
                WHEN 'check_percentage_diff' THEN b.check_percentage_diff
                ELSE NULL
            END AS metric_value
        FROM base b
        CROSS JOIN rules r
    ),
    scored AS (
        SELECT
            e.*,
            CASE
                WHEN e.metric_value IS NULL THEN FALSE
                WHEN e.operator = '>' THEN e.metric_value > e.threshold_min
                WHEN e.operator = '>=' THEN e.metric_value >= e.threshold_min
                WHEN e.operator = '<' THEN e.metric_value < e.threshold_min
                WHEN e.operator = '<=' THEN e.metric_value <= e.threshold_min
                WHEN e.operator = '=' THEN e.metric_value = e.threshold_min
                WHEN e.operator = 'between' THEN e.metric_value BETWEEN e.threshold_min AND e.threshold_max
                ELSE FALSE
            END AS is_hit
        FROM evaluated e
    ),
    agg AS (
        SELECT
            s.trade_date,
            s.stock_code,
            SUM(CASE WHEN s.is_hit THEN s.weight * s.score_if_true ELSE 0 END) AS total_score,
            SUM(s.weight * s.score_if_true) AS max_score,
            COUNT(*) FILTER (WHERE s.is_hit) AS rule_hit_count,
            COUNT(*) FILTER (WHERE s.is_required AND s.is_hit) AS required_hit_count,
            COALESCE(BOOL_AND(CASE WHEN s.is_required THEN s.is_hit ELSE TRUE END), TRUE) AS passed_required_rules,
            JSONB_AGG(
                JSONB_BUILD_OBJECT(
                    'rule_id', s.rule_id,
                    'rule_code', s.rule_code,
                    'metric_key', s.metric_key,
                    'operator', s.operator,
                    'threshold_min', s.threshold_min,
                    'threshold_max', s.threshold_max,
                    'metric_value', s.metric_value,
                    'is_required', s.is_required,
                    'is_hit', s.is_hit,
                    'weight', s.weight,
                    'score_if_true', s.score_if_true,
                    'score_obtained', CASE WHEN s.is_hit THEN s.weight * s.score_if_true ELSE 0 END
                )
                ORDER BY s.rule_order, s.rule_id
            ) AS rule_detail
        FROM scored s
        GROUP BY s.trade_date, s.stock_code
    ),
    inserted AS (
        INSERT INTO stock.screening_result_tx (
            run_id,
            formula_id,
            trade_date,
            stock_code,
            total_score,
            max_score,
            score_pct,
            passed,
            passed_required_rules,
            rule_hit_count,
            required_hit_count,
            rule_detail
        )
        SELECT
            v_run_id AS run_id,
            v_formula_id AS formula_id,
            a.trade_date,
            a.stock_code,
            a.total_score,
            a.max_score,
            CASE WHEN a.max_score = 0 THEN 0 ELSE a.total_score / a.max_score * 100 END AS score_pct,
            (a.passed_required_rules AND a.total_score >= v_min_total_score) AS passed,
            a.passed_required_rules,
            a.rule_hit_count,
            a.required_hit_count,
            a.rule_detail
        FROM agg a
        RETURNING result_id, stock_code, total_score
    ),
    ranked AS (
        SELECT
            i.result_id,
            ROW_NUMBER() OVER (ORDER BY i.total_score DESC, i.stock_code) AS rn
        FROM inserted i
    )
    UPDATE stock.screening_result_tx t
    SET rank_no = r.rn
    FROM ranked r
    WHERE t.result_id = r.result_id;

    UPDATE stock.screening_run_tx x
    SET
        finished_at = now(),
        status = 'success',
        total_candidates = q.total_candidates,
        total_passed = q.total_passed
    FROM (
        SELECT
            COUNT(*) AS total_candidates,
            COUNT(*) FILTER (WHERE passed) AS total_passed
        FROM stock.screening_result_tx
        WHERE run_id = v_run_id
    ) q
    WHERE x.run_id = v_run_id;

    RETURN v_run_id;
EXCEPTION
    WHEN OTHERS THEN
        UPDATE stock.screening_run_tx
        SET
            finished_at = now(),
            status = 'failed',
            error_message = SQLERRM
        WHERE run_id = v_run_id;
        RAISE;
END;
$$;

INSERT INTO stock.screening_formula_master (
    formula_code,
    formula_name,
    description,
    min_total_score,
    is_active
)
SELECT
    'MOMO_FLOW_V1',
    'Momentum + Flow + Liquidity v1',
    'Formula default untuk screening kombinasi momentum, likuiditas, dan foreign/order-flow',
    3.5,
    TRUE
WHERE NOT EXISTS (
    SELECT 1
    FROM stock.screening_formula_master
    WHERE formula_code = 'MOMO_FLOW_V1'
);

WITH f AS (
    SELECT formula_id
    FROM stock.screening_formula_master
    WHERE formula_code = 'MOMO_FLOW_V1'
)
INSERT INTO stock.screening_formula_rule (
    formula_id,
    rule_code,
    metric_key,
    operator,
    threshold_min,
    threshold_max,
    weight,
    score_if_true,
    is_required,
    is_active,
    rule_order
)
SELECT
    f.formula_id,
    v.rule_code,
    v.metric_key,
    v.operator,
    v.threshold_min,
    v.threshold_max,
    v.weight,
    v.score_if_true,
    v.is_required,
    TRUE,
    v.rule_order
FROM f
CROSS JOIN (
    VALUES
        ('R01_MOM_20D_POS', 'mom_20d_pct', '>', 5::numeric, NULL::numeric, 1.20::numeric, 1.0::numeric, TRUE, 10),
        ('R02_RET_1D_POS', 'ret_1d_pct', '>', 0.5::numeric, NULL::numeric, 0.80::numeric, 1.0::numeric, FALSE, 20),
        ('R03_LIQ_VALUE', 'avg_trade_value', '>', 20000000::numeric, NULL::numeric, 0.60::numeric, 1.0::numeric, FALSE, 30),
        ('R04_VOL_SPIKE', 'volume_spike_ratio_20d', '>', 1.2::numeric, NULL::numeric, 0.70::numeric, 1.0::numeric, FALSE, 40),
        ('R05_QUEUE_BUY', 'queue_imbalance', '>', 0::numeric, NULL::numeric, 0.60::numeric, 1.0::numeric, FALSE, 50),
        ('R06_FOREIGN_POS', 'net_foreign_intensity_pct', '>', 0::numeric, NULL::numeric, 0.90::numeric, 1.0::numeric, TRUE, 60),
        ('R07_SPREAD_HEALTHY', 'spread_pct', '<', 1.5::numeric, NULL::numeric, 0.50::numeric, 1.0::numeric, FALSE, 70),
        ('R08_NONREG_NOT_EXTREME', 'non_regular_value_ratio_pct', '<', 50::numeric, NULL::numeric, 0.30::numeric, 1.0::numeric, FALSE, 80)
) AS v(
    rule_code,
    metric_key,
    operator,
    threshold_min,
    threshold_max,
    weight,
    score_if_true,
    is_required,
    rule_order
)
WHERE NOT EXISTS (
    SELECT 1
    FROM stock.screening_formula_rule r
    WHERE r.formula_id = f.formula_id
      AND r.rule_code = v.rule_code
);

SELECT stock.run_stock_screening('MOMO_FLOW_V1', CURRENT_DATE-1);
