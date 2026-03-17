## Screener IDX (Intraday-Style via Daily Summary)

Catatan: data yang tersedia adalah ringkasan harian (`stock.summary` dan view `stock.v_summary_features_daily`). Jadi “intraday” di sini diaproksimasi menggunakan:
- `intraday_return_pct` (close vs open)
- `range_pct`, `body_pct`, `upper_shadow_pct`, `lower_shadow_pct`
- tekanan order book (`queue_imbalance`, `spread_pct`)
- spike likuiditas (`volume_spike_ratio_20d`, `value_spike_ratio_20d`)

### Tujuan
Menangkap saham IDX yang:
- bergerak kuat intraday (momentum intraday + range sehat),
- likuiditas tinggi (mudah exit cepat),
- spread tidak melebar,
- ada dukungan order book / foreign flow,
- bukan sekadar non-regular dominance.

### Formula: `IDX_INTRADAY_MOMO_V1`
Skor berbasis rule; pakai mekanisme `stock.run_stock_screening`.

**Rule utama (required)**
- `intraday_return_pct > 0.8`
- `value_spike_ratio_20d > 1.2`
- `spread_pct < 1.2`
- `avg_trade_value > 20000000`

**Rule pendukung**
- `range_pct > 2.0`
- `volume_spike_ratio_20d > 1.2`
- `queue_imbalance > 0`
- `net_foreign_intensity_pct > 0`
- `turnover_float_pct > 0.3`
- `non_regular_value_ratio_pct < 40`
- `value > 5000000000`

### SQL Insert Formula & Rules
```sql
INSERT INTO stock.screening_formula_master (
    formula_code,
    formula_name,
    description,
    min_total_score,
    is_active
)
SELECT
    'IDX_INTRADAY_MOMO_V1',
    'IDX Intraday Momentum v1',
    'Intraday-style screener (daily summary) untuk momentum + likuiditas + order-flow',
    4.0,
    TRUE
WHERE NOT EXISTS (
    SELECT 1
    FROM stock.screening_formula_master
    WHERE formula_code = 'IDX_INTRADAY_MOMO_V1'
);

WITH f AS (
    SELECT formula_id
    FROM stock.screening_formula_master
    WHERE formula_code = 'IDX_INTRADAY_MOMO_V1'
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
        -- Required core momentum + liquidity + tight spread
        ('R01_INTRADAY_POS',        'intraday_return_pct',     '>', 0.8::numeric,  NULL::numeric, 1.20::numeric, 1.0::numeric, TRUE,  10),
        ('R02_VALUE_SPIKE',         'value_spike_ratio_20d',    '>', 1.2::numeric,  NULL::numeric, 1.00::numeric, 1.0::numeric, TRUE,  20),
        ('R03_SPREAD_TIGHT',        'spread_pct',              '<', 1.2::numeric,  NULL::numeric, 0.80::numeric, 1.0::numeric, TRUE,  30),
        ('R04_LIQ_AVG_TRADE',       'avg_trade_value',         '>', 20000000::numeric, NULL::numeric, 0.90::numeric, 1.0::numeric, TRUE,  40),

        -- Supportive strength/flow
        ('R05_RANGE_HEALTHY',       'range_pct',               '>', 2.0::numeric,  NULL::numeric, 0.60::numeric, 1.0::numeric, FALSE, 50),
        ('R06_VOLUME_SPIKE',        'volume_spike_ratio_20d',   '>', 1.2::numeric,  NULL::numeric, 0.70::numeric, 1.0::numeric, FALSE, 60),
        ('R07_QUEUE_BUY',           'queue_imbalance',          '>', 0.0::numeric,  NULL::numeric, 0.60::numeric, 1.0::numeric, FALSE, 70),
        ('R08_FOREIGN_POS',         'net_foreign_intensity_pct','>', 0.0::numeric,  NULL::numeric, 0.70::numeric, 1.0::numeric, FALSE, 80),
        ('R09_TURNOVER_FLOAT',      'turnover_float_pct',       '>', 0.3::numeric,  NULL::numeric, 0.50::numeric, 1.0::numeric, FALSE, 90),
        ('R10_NONREG_NOT_EXTREME',  'non_regular_value_ratio_pct','<',40.0::numeric, NULL::numeric, 0.40::numeric, 1.0::numeric, FALSE, 100),
        ('R11_VALUE_BIG',           'value',                   '>', 5000000000::numeric, NULL::numeric, 0.50::numeric, 1.0::numeric, FALSE, 110)
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
```

### Jalankan Screening
```sql
SELECT stock.run_stock_screening('IDX_INTRADAY_MOMO_V1', CURRENT_DATE);
```

### Ambil Hasil Top Kandidat
```sql
SELECT
    run_id,
    trade_date,
    stock_code,
    total_score,
    score_pct,
    rank_no
FROM stock.screening_result_tx
WHERE run_id = (
    SELECT MAX(run_id)
    FROM stock.screening_run_tx
    WHERE formula_code = 'IDX_INTRADAY_MOMO_V1'
      AND status = 'success'
)
  AND passed = TRUE
ORDER BY rank_no;
```

### Drill-down Alasan Lolos
```sql
SELECT
    run_id,
    stock_code,
    rank_no,
    rule_code,
    metric_key,
    operator,
    threshold_min,
    threshold_max,
    metric_value,
    is_required,
    is_hit,
    score_obtained
FROM stock.v_screening_result_rule_detail
WHERE run_id = (
    SELECT MAX(run_id)
    FROM stock.screening_run_tx
    WHERE formula_code = 'IDX_INTRADAY_MOMO_V1'
      AND status = 'success'
)
ORDER BY rank_no, is_required DESC, rule_code;
```

## Screener IDX (Open-Continuation via Daily Summary)

Catatan: targetnya saham yang cenderung lanjut naik di hari berikutnya atau baru mulai naik, sehingga cocok untuk entry antri buy di open dan exit 5-10%.

### Tujuan
Menangkap saham IDX yang:
- naik sehat (tidak terlalu ekstrem),
- close dekat high (indikasi continuation),
- likuiditas cukup untuk eksekusi cepat,
- spread ketat,
- ada dukungan order-flow.

### Formula: `IDX_OPEN_MOMO_V1`
Skor berbasis rule; pakai mekanisme `stock.run_stock_screening`.

**Rule utama (required)**
- `intraday_return_pct BETWEEN 0.8 AND 3.5`
- `body_pct > 50`
- `upper_shadow_pct < 25`
- `spread_pct < 1.0`
- `avg_trade_value > 20000000`

**Rule pendukung**
- `range_pct > 3.0`
- `value_spike_ratio_20d > 1.3`
- `volume_spike_ratio_20d > 1.3`
- `queue_imbalance > 0`
- `net_foreign_intensity_pct > 0`
- `non_regular_value_ratio_pct < 30`
- `value > 5000000000`

### SQL Insert Formula & Rules
```sql
INSERT INTO stock.screening_formula_master (
    formula_code,
    formula_name,
    description,
    min_total_score,
    is_active
)
SELECT
    'IDX_OPEN_MOMO_V1',
    'IDX Open Momentum v1',
    'Continuation/open-momo screener (daily summary) untuk peluang lanjut naik H+1',
    4.0,
    TRUE
WHERE NOT EXISTS (
    SELECT 1
    FROM stock.screening_formula_master
    WHERE formula_code = 'IDX_OPEN_MOMO_V1'
);

WITH f AS (
    SELECT formula_id
    FROM stock.screening_formula_master
    WHERE formula_code = 'IDX_OPEN_MOMO_V1'
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
        -- Required: sehat, close near high, spread ketat, likuid
        ('R01_INTRADAY_MID',        'intraday_return_pct',     'between', 0.8::numeric,  3.5::numeric, 1.20::numeric, 1.0::numeric, TRUE,  10),
        ('R02_BODY_STRONG',         'body_pct',                '>', 50.0::numeric, NULL::numeric, 1.00::numeric, 1.0::numeric, TRUE,  20),
        ('R03_UPPER_SHADOW_LOW',    'upper_shadow_pct',        '<', 25.0::numeric, NULL::numeric, 0.90::numeric, 1.0::numeric, TRUE,  30),
        ('R04_SPREAD_TIGHT',        'spread_pct',              '<', 1.0::numeric,  NULL::numeric, 0.80::numeric, 1.0::numeric, TRUE,  40),
        ('R05_LIQ_AVG_TRADE',       'avg_trade_value',         '>', 20000000::numeric, NULL::numeric, 0.90::numeric, 1.0::numeric, TRUE,  50),

        -- Support: range sehat + likuiditas + order flow
        ('R06_RANGE_HEALTHY',       'range_pct',               '>', 3.0::numeric,  NULL::numeric, 0.60::numeric, 1.0::numeric, FALSE, 60),
        ('R07_VALUE_SPIKE',         'value_spike_ratio_20d',    '>', 1.3::numeric,  NULL::numeric, 0.70::numeric, 1.0::numeric, FALSE, 70),
        ('R08_VOLUME_SPIKE',        'volume_spike_ratio_20d',   '>', 1.3::numeric,  NULL::numeric, 0.70::numeric, 1.0::numeric, FALSE, 80),
        ('R09_QUEUE_BUY',           'queue_imbalance',          '>', 0.0::numeric,  NULL::numeric, 0.60::numeric, 1.0::numeric, FALSE, 90),
        ('R10_FOREIGN_POS',         'net_foreign_intensity_pct','>', 0.0::numeric,  NULL::numeric, 0.70::numeric, 1.0::numeric, FALSE, 100),
        ('R11_NONREG_LOW',          'non_regular_value_ratio_pct','<',30.0::numeric, NULL::numeric, 0.40::numeric, 1.0::numeric, FALSE, 110),
        ('R12_VALUE_BIG',           'value',                   '>', 5000000000::numeric, NULL::numeric, 0.50::numeric, 1.0::numeric, FALSE, 120)
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
```

### Jalankan Screening
```sql
SELECT stock.run_stock_screening('IDX_OPEN_MOMO_V1', CURRENT_DATE);
```

### Ambil Hasil Top Kandidat
```sql
SELECT
    run_id,
    trade_date,
    stock_code,
    total_score,
    score_pct,
    rank_no
FROM stock.screening_result_tx
WHERE run_id = (
    SELECT MAX(run_id)
    FROM stock.screening_run_tx
    WHERE formula_code = 'IDX_OPEN_MOMO_V1'
      AND status = 'success'
)
  AND passed = TRUE
ORDER BY rank_no;
```

### Drill-down Alasan Lolos
```sql
SELECT
    run_id,
    stock_code,
    rank_no,
    rule_code,
    metric_key,
    operator,
    threshold_min,
    threshold_max,
    metric_value,
    is_required,
    is_hit,
    score_obtained
FROM stock.v_screening_result_rule_detail
WHERE run_id = (
    SELECT MAX(run_id)
    FROM stock.screening_run_tx
    WHERE formula_code = 'IDX_OPEN_MOMO_V1'
      AND status = 'success'
)
ORDER BY rank_no, is_required DESC, rule_code;
```
