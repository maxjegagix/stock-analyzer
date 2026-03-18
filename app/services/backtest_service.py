from collections import defaultdict
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

from app.core.config import settings
from app.services.recommendation_service import _score_row


SQL_TEMPLATE = """
WITH base AS (
    SELECT
        stock_code,
        trade_date,
        close,
        high,
        low,
        volume,
        {bid_col} AS bid,
        {bid_volume_col} AS bid_volume,
        {offer_col} AS offer,
        {offer_volume_col} AS offer_volume,
        AVG(close) OVER (
            PARTITION BY stock_code
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma20,
        AVG(close) OVER (
            PARTITION BY stock_code
            ORDER BY trade_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS ma50,
        AVG(volume) OVER (
            PARTITION BY stock_code
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_vol20,
        LAG(close, 20) OVER (
            PARTITION BY stock_code
            ORDER BY trade_date
        ) AS close_20d_ago,
        MAX(high) OVER (
            PARTITION BY stock_code
            ORDER BY trade_date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS max_high_20d,
        LEAD(trade_date, 1) OVER (
            PARTITION BY stock_code
            ORDER BY trade_date
        ) AS next_trade_date,
        LEAD(high, 1) OVER (
            PARTITION BY stock_code
            ORDER BY trade_date
        ) AS next_high,
        LEAD(low, 1) OVER (
            PARTITION BY stock_code
            ORDER BY trade_date
        ) AS next_low,
        LEAD(close, 1) OVER (
            PARTITION BY stock_code
            ORDER BY trade_date
        ) AS next_close
    FROM stock.summary
    WHERE trade_date BETWEEN %(start)s AND %(end)s
)
SELECT *
FROM base
ORDER BY trade_date, stock_code;
"""


def _connect_db():
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        dbname=settings.DB_NAME,
    )


def _has_column(conn, schema: str, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            """,
            (schema, table, column),
        )
        return cur.fetchone() is not None


def _build_sql(conn) -> str:
    schema = "stock"
    table = "summary"
    bid_col = "bid" if _has_column(conn, schema, table, "bid") else "NULL"
    bid_volume_col = "bid_volume" if _has_column(conn, schema, table, "bid_volume") else "NULL"
    offer_col = "offer" if _has_column(conn, schema, table, "offer") else "NULL"
    offer_volume_col = (
        "offer_volume" if _has_column(conn, schema, table, "offer_volume") else "NULL"
    )
    return SQL_TEMPLATE.format(
        bid_col=bid_col,
        bid_volume_col=bid_volume_col,
        offer_col=offer_col,
        offer_volume_col=offer_volume_col,
    )


def _parse_date(value: str) -> datetime.date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def backtest_screener(
    start_date: str,
    end_date: str,
    min_price: float,
    max_price: float | None,
    min_avg_volume_20d: float,
    limit_per_day: int | None,
) -> Dict[str, Any]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if start > end:
        raise ValueError("start_date must be <= end_date")

    lookback_start = start - timedelta(days=200)
    # Extend proof window to reach next trading day (e.g., Friday -> Monday)
    proof_end = end + timedelta(days=7)

    with _connect_db() as conn:
        sql = _build_sql(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, {"start": lookback_start, "end": proof_end})
            rows = cur.fetchall()

    rows_by_date = defaultdict(list)
    for row in rows:
        rows_by_date[row["trade_date"]].append(row)

    total_signals = 0
    total_entry_hits = 0
    hit_r1 = 0
    hit_r2 = 0
    hit_r3 = 0
    stop_hits = 0
    max_gain_pcts: List[float] = []
    max_drawdown_pcts: List[float] = []
    close_pcts: List[float] = []
    r_multiples: List[float] = []
    per_date: List[Dict[str, Any]] = []

    trade_dates = sorted([d for d in rows_by_date.keys() if start <= d <= end])
    effective_start = trade_dates[0].isoformat() if trade_dates else None
    effective_end = trade_dates[-1].isoformat() if trade_dates else None
    for date in trade_dates:
        day_rows = rows_by_date.get(date, [])
        recs = []
        for row in day_rows:
            if row["avg_vol20"] is None or float(row["avg_vol20"]) < min_avg_volume_20d:
                continue
            rec = _score_row(row, min_price=min_price, max_price=max_price)
            if rec is None:
                continue
            recs.append((rec, row))

        recs.sort(key=lambda t: t[0].score, reverse=True)
        if limit_per_day is not None:
            recs = recs[:limit_per_day]

        items = []
        day_signals = 0
        day_entry_hits = 0
        day_hit_r1 = 0
        day_hit_r2 = 0
        day_hit_r3 = 0
        day_stop_hits = 0
        proof_dates = []
        for rec, row in recs:
            if row["next_trade_date"] is None:
                continue
            next_high = row["next_high"]
            next_low = row["next_low"]
            next_close = row["next_close"]
            entry_hit = (
                next_high is not None
                and next_low is not None
                and float(next_low) <= rec.entry <= float(next_high)
            )
            risk = rec.entry - rec.support1
            hit_r1_now = entry_hit and next_high is not None and float(next_high) >= rec.resist1
            hit_r2_now = entry_hit and next_high is not None and float(next_high) >= rec.resist2
            hit_r3_now = entry_hit and next_high is not None and float(next_high) >= rec.resist3
            stop_now = entry_hit and next_low is not None and float(next_low) <= rec.support1
            outcome = "no_entry"
            if entry_hit:
                if hit_r3_now:
                    outcome = "hit_r3"
                elif hit_r2_now:
                    outcome = "hit_r2"
                elif hit_r1_now:
                    outcome = "hit_r1"
                elif stop_now:
                    outcome = "stop"
                else:
                    outcome = "no_target"

            if entry_hit:
                day_entry_hits += 1
                day_hit_r1 += 1 if hit_r1_now else 0
                day_hit_r2 += 1 if hit_r2_now else 0
                day_hit_r3 += 1 if hit_r3_now else 0
                day_stop_hits += 1 if stop_now else 0
                proof_dates.append(str(row["next_trade_date"]))

                if next_high is not None and rec.entry > 0:
                    max_gain_pcts.append(((float(next_high) - rec.entry) / rec.entry) * 100.0)
                if next_low is not None and rec.entry > 0:
                    max_drawdown_pcts.append(((rec.entry - float(next_low)) / rec.entry) * 100.0)
                if next_close is not None and rec.entry > 0:
                    close_pcts.append(((float(next_close) - rec.entry) / rec.entry) * 100.0)
                if next_high is not None and risk > 0:
                    r_multiples.append((float(next_high) - rec.entry) / risk)

            day_signals += 1
            price_hit = float(next_high) if next_high is not None else None
            pct_from_entry = (
                ((price_hit - rec.entry) / rec.entry) * 100.0
                if entry_hit and price_hit is not None and rec.entry > 0
                else None
            )
            items.append(
                {
                    "stock_code": rec.stock_code,
                    "trade_date": rec.trade_date,
                    "next_trade_date": str(row["next_trade_date"]),
                    "entry": rec.entry,
                    "resist1": rec.resist1,
                    "resist2": rec.resist2,
                    "resist3": rec.resist3,
                    "support1": rec.support1,
                    "next_high": float(next_high) if next_high is not None else None,
                    "next_low": float(next_low) if next_low is not None else None,
                    "next_close": float(next_close) if next_close is not None else None,
                    "entry_hit": entry_hit,
                    "price_hit": price_hit,
                    "pct_from_entry": round(pct_from_entry, 2) if pct_from_entry is not None else None,
                    "hit_r1": hit_r1_now,
                    "hit_r2": hit_r2_now,
                    "hit_r3": hit_r3_now,
                    "stop_hit": stop_now,
                    "outcome": outcome,
                }
            )

        if day_signals > 0:
            total_signals += day_signals
            total_entry_hits += day_entry_hits
            hit_r1 += day_hit_r1
            hit_r2 += day_hit_r2
            hit_r3 += day_hit_r3
            stop_hits += day_stop_hits
            per_date.append(
                {
                    "date": date.isoformat(),
                    "proof_date": min(proof_dates) if proof_dates else None,
                    "total_signals": day_signals,
                    "entry_hits": day_entry_hits,
                    "hit_r1": day_hit_r1,
                    "hit_r2": day_hit_r2,
                    "hit_r3": day_hit_r3,
                    "stop_hits": day_stop_hits,
                    "items": items,
                }
            )

        # iterate only on available trading dates within range

    hit_rate = (hit_r1 / total_entry_hits * 100.0) if total_entry_hits > 0 else 0.0
    entry_rate = (total_entry_hits / total_signals * 100.0) if total_signals > 0 else 0.0
    hit_r2_rate = (hit_r2 / total_entry_hits * 100.0) if total_entry_hits > 0 else 0.0
    hit_r3_rate = (hit_r3 / total_entry_hits * 100.0) if total_entry_hits > 0 else 0.0
    stop_rate = (stop_hits / total_entry_hits * 100.0) if total_entry_hits > 0 else 0.0

    def _median(values: List[float]) -> float:
        if not values:
            return 0.0
        vals = sorted(values)
        mid = len(vals) // 2
        if len(vals) % 2 == 1:
            return vals[mid]
        return (vals[mid - 1] + vals[mid]) / 2.0

    avg_gain = sum(max_gain_pcts) / len(max_gain_pcts) if max_gain_pcts else 0.0
    avg_drawdown = sum(max_drawdown_pcts) / len(max_drawdown_pcts) if max_drawdown_pcts else 0.0
    avg_close = sum(close_pcts) / len(close_pcts) if close_pcts else 0.0
    avg_r_mult = sum(r_multiples) / len(r_multiples) if r_multiples else 0.0
    result = {
        "effective_start_date": effective_start,
        "effective_end_date": effective_end,
        "total_signals": total_signals,
        "total_entry_hits": total_entry_hits,
        "hits_r1": hit_r1,
        "hits_r2": hit_r2,
        "hits_r3": hit_r3,
        "stop_hits": stop_hits,
        "hit_rate_pct": round(hit_rate, 2),
        "entry_rate_pct": round(entry_rate, 2),
        "hit_r2_rate_pct": round(hit_r2_rate, 2),
        "hit_r3_rate_pct": round(hit_r3_rate, 2),
        "stop_rate_pct": round(stop_rate, 2),
        "avg_max_gain_pct": round(avg_gain, 2),
        "median_max_gain_pct": round(_median(max_gain_pcts), 2),
        "avg_max_drawdown_pct": round(avg_drawdown, 2),
        "median_max_drawdown_pct": round(_median(max_drawdown_pcts), 2),
        "avg_close_pct": round(avg_close, 2),
        "median_close_pct": round(_median(close_pcts), 2),
        "avg_r_multiple": round(avg_r_mult, 2),
        "median_r_multiple": round(_median(r_multiples), 2),
        "per_date": per_date,
    }
    _append_backtest_log(
        method="momentum_ma_breakout",
        params={
            "start_date": start_date,
            "end_date": end_date,
            "min_price": min_price,
            "max_price": max_price,
            "min_avg_volume_20d": min_avg_volume_20d,
            "limit_per_day": limit_per_day,
        },
        summary={
            "total_signals": result["total_signals"],
            "total_entry_hits": result["total_entry_hits"],
            "hits_r1": result["hits_r1"],
            "hits_r2": result["hits_r2"],
            "hits_r3": result["hits_r3"],
            "stop_hits": result["stop_hits"],
            "hit_rate_pct": result["hit_rate_pct"],
            "entry_rate_pct": result["entry_rate_pct"],
            "hit_r2_rate_pct": result["hit_r2_rate_pct"],
            "hit_r3_rate_pct": result["hit_r3_rate_pct"],
            "stop_rate_pct": result["stop_rate_pct"],
            "avg_max_gain_pct": result["avg_max_gain_pct"],
            "median_max_gain_pct": result["median_max_gain_pct"],
            "avg_max_drawdown_pct": result["avg_max_drawdown_pct"],
            "median_max_drawdown_pct": result["median_max_drawdown_pct"],
            "avg_close_pct": result["avg_close_pct"],
            "median_close_pct": result["median_close_pct"],
            "avg_r_multiple": result["avg_r_multiple"],
            "median_r_multiple": result["median_r_multiple"],
        },
    )
    return result


def _log_path() -> Path:
    project_root = Path(__file__).resolve().parents[2]
    return project_root / "data" / "backtest_logs.jsonl"


def _append_backtest_log(method: str, params: Dict[str, Any], summary: Dict[str, Any]) -> None:
    path = _log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "method": method,
        "params": params,
        "summary": summary,
    }
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


def analyze_method_logs(method: str) -> Dict[str, Any]:
    path = _log_path()
    if not path.exists():
        return {
            "method": method,
            "count": 0,
            "avg_hit_rate_pct": 0.0,
            "last": None,
        }

    records = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("method") == method:
                records.append(rec)

    if not records:
        return {
            "method": method,
            "count": 0,
            "avg_hit_rate_pct": 0.0,
            "last": None,
        }

    hit_rates = [r.get("summary", {}).get("hit_rate_pct", 0.0) for r in records]
    avg_hit = sum(hit_rates) / len(hit_rates) if hit_rates else 0.0
    last = records[-1]
    return {
        "method": method,
        "count": len(records),
        "avg_hit_rate_pct": round(avg_hit, 2),
        "last": last,
    }
