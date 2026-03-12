CREATE OR REPLACE VIEW stock.v_summary_features_daily AS
WITH base AS (
    SELECT
        s.trade_date,
        s.stock_code,
        s.id_stock_summary,
        s.stock_name,
        s.remarks,
        s.delisting_date,
        s.created_at,
        s.previous,
        s.open_price,
        s.first_trade,
        s.high,
        s.low,
        s.close,
        s.change,
        s.persen,
        s.percentage,
        s.volume,
        s.value,
        s.frequency,
        s.non_regular_volume,
        s.non_regular_value,
        s.non_regular_frequency,
        s.bid,
        s.bid_volume,
        s.offer,
        s.offer_volume,
        s.foreign_buy,
        s.foreign_sell,
        s.listed_shares,
        s.tradeable_shares,
        s.weight_for_index,
        s.index_individual,
        (s.close / NULLIF(s.previous, 0) - 1) * 100 AS ret_1d_pct_raw
    FROM stock.summary s
),
enriched AS (
    SELECT
        b.*,
        LAG(b.close, 5) OVER (PARTITION BY b.stock_code ORDER BY b.trade_date) AS close_lag_5d,
        LAG(b.close, 20) OVER (PARTITION BY b.stock_code ORDER BY b.trade_date) AS close_lag_20d,
        LAG(b.close, 60) OVER (PARTITION BY b.stock_code ORDER BY b.trade_date) AS close_lag_60d,
        AVG(b.volume::numeric) OVER (
            PARTITION BY b.stock_code
            ORDER BY b.trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS vol_ma_20d,
        AVG(b.value) OVER (
            PARTITION BY b.stock_code
            ORDER BY b.trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS value_ma_20d,
        STDDEV_SAMP(b.ret_1d_pct_raw) OVER (
            PARTITION BY b.stock_code
            ORDER BY b.trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ret_vol_20d
    FROM base b
)
SELECT
    e.trade_date,
    e.stock_code,
    e.id_stock_summary,
    e.stock_name,
    e.remarks,
    e.delisting_date,
    e.created_at,

    e.previous,
    e.open_price,
    e.first_trade,
    e.high,
    e.low,
    e.close,
    e.change,
    e.persen,
    e.percentage,

    ROUND((e.close / NULLIF(e.previous, 0) - 1) * 100, 4) AS ret_1d_pct,
    ROUND((e.open_price / NULLIF(e.previous, 0) - 1) * 100, 4) AS gap_open_pct,
    ROUND((e.close / NULLIF(e.open_price, 0) - 1) * 100, 4) AS intraday_return_pct,
    ROUND((e.high - e.low) / NULLIF(e.close, 0) * 100, 4) AS range_pct,
    ROUND((ABS(e.close - e.open_price)) / NULLIF(e.close, 0) * 100, 4) AS body_pct,
    ROUND((e.high - GREATEST(e.open_price, e.close)) / NULLIF(e.close, 0) * 100, 4) AS upper_shadow_pct,
    ROUND((LEAST(e.open_price, e.close) - e.low) / NULLIF(e.close, 0) * 100, 4) AS lower_shadow_pct,

    ROUND((e.close / NULLIF(e.close_lag_5d, 0) - 1) * 100, 4) AS mom_5d_pct,
    ROUND((e.close / NULLIF(e.close_lag_20d, 0) - 1) * 100, 4) AS mom_20d_pct,
    ROUND((e.close / NULLIF(e.close_lag_60d, 0) - 1) * 100, 4) AS mom_60d_pct,
    ROUND(e.ret_vol_20d, 4) AS ret_vol_20d,

    e.volume,
    e.value,
    e.frequency,
    ROUND(e.value / NULLIF(e.frequency, 0), 4) AS avg_trade_value,
    ROUND(e.value / NULLIF(e.volume, 0), 4) AS vwap_proxy,
    ROUND(e.volume::numeric / NULLIF(e.listed_shares, 0) * 100, 6) AS turnover_listed_pct,
    ROUND(e.volume::numeric / NULLIF(e.tradeable_shares, 0) * 100, 6) AS turnover_float_pct,
    ROUND(e.volume::numeric / NULLIF(e.vol_ma_20d, 0), 4) AS volume_spike_ratio_20d,
    ROUND(e.value / NULLIF(e.value_ma_20d, 0), 4) AS value_spike_ratio_20d,

    e.non_regular_volume,
    e.non_regular_value,
    e.non_regular_frequency,
    ROUND(e.non_regular_value / NULLIF(e.value, 0) * 100, 4) AS non_regular_value_ratio_pct,
    ROUND(e.non_regular_volume::numeric / NULLIF(e.volume, 0) * 100, 4) AS non_regular_volume_ratio_pct,
    ROUND(e.non_regular_frequency::numeric / NULLIF(e.frequency, 0) * 100, 4) AS non_regular_frequency_ratio_pct,

    e.bid,
    e.offer,
    e.bid_volume,
    e.offer_volume,
    ROUND((e.offer - e.bid) / NULLIF(e.close, 0) * 100, 4) AS spread_pct,
    ROUND(
        (e.bid_volume - e.offer_volume)::numeric
        / NULLIF((e.bid_volume + e.offer_volume)::numeric, 0),
        6
    ) AS queue_imbalance,
    ROUND(
        (e.offer * e.bid_volume + e.bid * e.offer_volume)
        / NULLIF((e.bid_volume + e.offer_volume)::numeric, 0),
        6
    ) AS microprice_proxy,

    e.foreign_buy,
    e.foreign_sell,
    (e.foreign_buy - e.foreign_sell) AS net_foreign,
    ROUND((e.foreign_buy + e.foreign_sell) / NULLIF(e.value, 0) * 100, 4) AS foreign_participation_pct,
    ROUND((e.foreign_buy - e.foreign_sell) / NULLIF(e.value, 0) * 100, 4) AS net_foreign_intensity_pct,

    e.listed_shares,
    e.tradeable_shares,
    e.weight_for_index,
    e.index_individual,
    ROUND(e.weight_for_index * ((e.close / NULLIF(e.previous, 0) - 1) * 100), 6) AS index_contribution_proxy,

    ROUND(((e.close - e.previous) - COALESCE(e.change, 0)), 8) AS check_change_diff,
    ROUND(((e.close / NULLIF(e.previous, 0) - 1) * 100) - COALESCE(e.percentage, e.persen), 8) AS check_percentage_diff
FROM enriched e;
