# Advanced Brainstorm Analisa `stock.summary`

Dokumen ini memperdalam pemanfaatan seluruh kolom di `stock.summary` untuk membangun analisa yang lebih detail dan tajam.

## 1) Peta Kolom dan Fungsi Analitik
### A. Identitas dan kualitas data
- `trade_date`, `stock_code`: key time-series per emiten.
- `id_stock_summary`: id dari source IDX, dipakai untuk audit ingest.
- `stock_name`, `remarks`, `created_at`: metadata, quality flag, data freshness.
- `delisting_date`: lifecycle emiten.

### B. Harga
- `previous`, `open_price`, `first_trade`, `high`, `low`, `close`, `change`, `persen`, `percentage`.

### C. Aktivitas transaksi
- `volume`, `value`, `frequency`.
- `non_regular_volume`, `non_regular_value`, `non_regular_frequency`.

### D. Mikrostruktur pasar
- `bid`, `bid_volume`, `offer`, `offer_volume`.

### E. Struktur emiten dan aliran dana
- `listed_shares`, `tradeable_shares`, `weight_for_index`.
- `foreign_buy`, `foreign_sell`.
- `index_individual`.

## 2) Feature Engineering Per Kolom (Lebih Tajam)
### A. Fitur harga
- `ret_1d_pct`: `(close/previous - 1)*100`.
- `gap_open_pct`: `(open_price/previous - 1)*100`.
- `intraday_return_pct`: `(close/open_price - 1)*100`.
- `shadow_up_pct`: `(high - GREATEST(open_price, close))/NULLIF(close,0)*100`.
- `shadow_down_pct`: `(LEAST(open_price, close) - low)/NULLIF(close,0)*100`.
- `range_pct`: `(high-low)/NULLIF(close,0)*100`.

### B. Fitur likuiditas
- `avg_trade_value`: `value/NULLIF(frequency,0)`.
- `vwap_proxy`: `value/NULLIF(volume,0)` (proxy harga rata-rata transaksi).
- `turnover_listed_pct`: `volume/NULLIF(listed_shares,0)*100`.
- `turnover_float_pct`: `volume/NULLIF(tradeable_shares,0)*100`.

### C. Fitur order-book pressure
- `spread_pct`: `(offer-bid)/NULLIF(close,0)*100`.
- `queue_imbalance`: `(bid_volume-offer_volume)/NULLIF((bid_volume+offer_volume),0)`.
- `microprice_proxy`: `(offer*bid_volume + bid*offer_volume)/NULLIF((bid_volume+offer_volume),0)`.

### D. Fitur foreign flow
- `net_foreign`: `foreign_buy - foreign_sell`.
- `foreign_participation_pct`: `(foreign_buy+foreign_sell)/NULLIF(value,0)*100`.
- `net_foreign_intensity`: `net_foreign/NULLIF(value,0)*100`.

### E. Fitur non-regular market
- `non_regular_ratio_value_pct`: `non_regular_value/NULLIF(value,0)*100`.
- `non_regular_ratio_volume_pct`: `non_regular_volume/NULLIF(volume,0)*100`.
- gunakan sebagai indikator block trade / perpindahan kepemilikan besar.

### F. Fitur validasi source
- `delta_change_check`: `close - previous` vs `change`.
- `delta_percentage_check`: return hitung ulang vs `percentage`/`persen`.
- `created_at_latency`: `created_at` dibanding jam load terakhir.

## 3) Analisa Detail yang Bisa Dihasilkan
### A. Regime harian per saham (trend vs reversal vs noisy)
Aturan contoh:
- `trend_up`: `ret_1d_pct > 2`, `intraday_return_pct > 0`, `queue_imbalance > 0`.
- `possible_reversal`: gap down besar tapi close kembali di atas open.
- `noisy_high_vol`: `range_pct` tinggi tapi `abs(ret_1d_pct)` kecil.

### B. Breakout berkualitas (bukan breakout palsu)
Syarat kombinasi:
- `close` menembus high N-hari.
- `turnover_float_pct` naik signifikan.
- `spread_pct` tetap sempit.
- `net_foreign_intensity` positif.
- `non_regular_ratio_value_pct` tidak ekstrem (hindari sinyal semu dari crossing block saja).

### C. Smart money detection
Skema indikatif:
- Harga naik moderat, `net_foreign` positif beruntun 3-5 hari.
- `avg_trade_value` meningkat.
- `queue_imbalance` cenderung positif.

### D. Supply overhang / distribusi
Skema indikatif:
- Harga stagnan/naik tipis, tapi `value` tinggi dan `foreign_sell` dominan.
- `offer_volume` besar, `spread_pct` melebar.
- `non_regular_value` tinggi berulang.

### E. Market breadth + concentration
Dari level market:
- breadth: jumlah `advancers` vs `decliners`.
- concentration: kontribusi `top 10` berdasarkan `value` terhadap total market.
- kalau breadth lemah tapi indeks naik, biasanya kenaikan ditopang saham besar tertentu.

### F. Index influence mapping
Gunakan `weight_for_index` + return:
- `index_contribution_proxy = weight_for_index * ret_1d_pct`.
- ranking emiten paling mendorong indeks setiap hari.
- cross-check dengan `index_individual`.

### G. Delisting risk watchlist
Gunakan:
- `delisting_date IS NOT NULL`.
- likuiditas menurun (value/frequency turun) menjelang tanggal.
- warning untuk sistem screening agar dikeluarkan dari shortlist.

## 4) Query SQL Siap Pakai
### A. Daily enriched features
```sql
SELECT
    trade_date,
    stock_code,
    stock_name,
    close,
    previous,
    ROUND((close / NULLIF(previous, 0) - 1) * 100, 2) AS ret_1d_pct,
    ROUND((open_price / NULLIF(previous, 0) - 1) * 100, 2) AS gap_open_pct,
    ROUND((close / NULLIF(open_price, 0) - 1) * 100, 2) AS intraday_return_pct,
    ROUND((high - low) / NULLIF(close, 0) * 100, 2) AS range_pct,
    ROUND(value / NULLIF(frequency, 0), 2) AS avg_trade_value,
    ROUND((offer - bid) / NULLIF(close, 0) * 100, 4) AS spread_pct,
    ROUND((bid_volume - offer_volume)::numeric / NULLIF((bid_volume + offer_volume)::numeric, 0), 4) AS queue_imbalance,
    (foreign_buy - foreign_sell) AS net_foreign,
    ROUND((foreign_buy - foreign_sell) / NULLIF(value, 0) * 100, 4) AS net_foreign_intensity_pct,
    ROUND(non_regular_value / NULLIF(value, 0) * 100, 2) AS non_regular_value_ratio_pct
FROM stock.summary
WHERE trade_date = CURRENT_DATE;
```

### B. Composite score (momentum + liquidity + order flow)
```sql
WITH d AS (
    SELECT
        *,
        (close / NULLIF(previous, 0) - 1) * 100 AS ret_1d_pct,
        value / NULLIF(frequency, 0) AS avg_trade_value,
        (bid_volume - offer_volume)::numeric / NULLIF((bid_volume + offer_volume)::numeric, 0) AS queue_imbalance,
        (foreign_buy - foreign_sell) / NULLIF(value, 0) * 100 AS net_foreign_intensity_pct
    FROM stock.summary
    WHERE trade_date = CURRENT_DATE
),
z AS (
    SELECT
        stock_code,
        stock_name,
        ret_1d_pct,
        avg_trade_value,
        queue_imbalance,
        net_foreign_intensity_pct,
        (ret_1d_pct - AVG(ret_1d_pct) OVER()) / NULLIF(STDDEV_SAMP(ret_1d_pct) OVER(), 0) AS z_ret,
        (avg_trade_value - AVG(avg_trade_value) OVER()) / NULLIF(STDDEV_SAMP(avg_trade_value) OVER(), 0) AS z_liq,
        (queue_imbalance - AVG(queue_imbalance) OVER()) / NULLIF(STDDEV_SAMP(queue_imbalance) OVER(), 0) AS z_queue,
        (net_foreign_intensity_pct - AVG(net_foreign_intensity_pct) OVER()) / NULLIF(STDDEV_SAMP(net_foreign_intensity_pct) OVER(), 0) AS z_foreign
    FROM d
)
SELECT
    stock_code,
    stock_name,
    ROUND(0.35*z_ret + 0.25*z_liq + 0.20*z_queue + 0.20*z_foreign, 4) AS composite_score
FROM z
ORDER BY composite_score DESC
LIMIT 30;
```

### C. Data quality audit
```sql
SELECT
    trade_date,
    stock_code,
    close,
    previous,
    change,
    percentage,
    (close - previous) AS calc_change,
    ((close / NULLIF(previous, 0) - 1) * 100) AS calc_pct
FROM stock.summary
WHERE
    high < low
    OR close < low
    OR close > high
    OR volume < 0
    OR value < 0
    OR ABS((close - previous) - COALESCE(change, 0)) > 0.0001;
```

## 5) Framework Sinyal Harian (Praktis)
Label contoh:
- `A1_STRONG_MOMENTUM`: ret tinggi, likuiditas tinggi, foreign netto positif, spread rendah.
- `A2_BREAKOUT_CONFIRMED`: breakout + turnover float + queue imbalance positif.
- `B1_MEAN_REVERT_SETUP`: koreksi tajam, range tinggi, reversal candle intraday.
- `C1_DISTRIBUTION_RISK`: ret lemah, value tinggi, foreign netto negatif, offer pressure dominan.
- `C2_BLOCK_TRADE_WARNING`: non-regular ratio sangat tinggi.

## 6) Rekomendasi Implementasi Lanjutan
1. Buat view `stock.v_summary_features_daily` berisi fitur turunan lengkap.
2. Buat tabel `stock.daily_signal` untuk menyimpan label + score per `trade_date, stock_code`.
3. Jalankan scoring setelah proses pull data harian selesai.
4. Tambahkan backtest sederhana berdasarkan label sinyal 1D/5D/20D forward return.

## 7) Arsitektur Screening Plug/Unplug Formula
Struktur yang dipakai:
- `stock.screening_formula_master`: master formula (aktif/nonaktif, min score).
- `stock.screening_formula_rule`: master rule per formula (metric, operator, threshold, weight, required).
- `stock.screening_run_tx`: transaksi eksekusi screening per tanggal.
- `stock.screening_result_tx`: transaksi hasil score/rank per saham.

Eksekusi:
```sql
SELECT stock.run_stock_screening('MOMO_FLOW_V1', CURRENT_DATE);
```

Contoh ambil shortlist:
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
    WHERE formula_code = 'MOMO_FLOW_V1'
      AND status = 'success'
)
  AND passed = TRUE
ORDER BY rank_no;
```

Drill-down alasan per rule (flatten JSON):
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
    WHERE formula_code = 'MOMO_FLOW_V1'
      AND status = 'success'
)
ORDER BY rank_no, is_required DESC, rule_code;
```
