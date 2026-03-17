from dataclasses import dataclass
from typing import List, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor

from app.core.config import settings


@dataclass
class Recommendation:
    stock_code: str
    trade_date: str
    close: float
    score: float
    reasons: List[str]
    entry: float
    support1: float
    support2: float
    support3: float
    resist1: float
    resist2: float
    resist3: float
    risk_reward_t1: float | None
    risk_reward_t2: float | None
    risk_reward_t3: float | None
    bid_offer_imbalance: float | None
    spread_pct: float | None


SQL_LATEST_FEATURES_TEMPLATE = """
WITH base AS (
    SELECT
        stock_code,
        trade_date,
        close,
        high,
        low,
        {bid_col} AS bid,
        {bid_volume_col} AS bid_volume,
        {offer_col} AS offer,
        {offer_volume_col} AS offer_volume,
        volume,
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
        MAX(trade_date) OVER (PARTITION BY stock_code) AS last_date
    FROM stock.summary
)
SELECT
    stock_code,
    trade_date,
    close,
    high,
    low,
    bid,
    bid_volume,
    offer,
    offer_volume,
    ma20,
    ma50,
    avg_vol20,
    close_20d_ago,
    max_high_20d,
    volume
FROM base
WHERE trade_date = last_date;
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


def _build_sql(conn, force_null_bid_offer: bool = False) -> str:
    schema = "stock"
    table = "summary"
    if force_null_bid_offer:
        bid_col = "NULL"
        bid_volume_col = "NULL"
        offer_col = "NULL"
        offer_volume_col = "NULL"
    else:
        bid_col = "bid" if _has_column(conn, schema, table, "bid") else "NULL"
        bid_volume_col = (
            "bid_volume" if _has_column(conn, schema, table, "bid_volume") else "NULL"
        )
        offer_col = "offer" if _has_column(conn, schema, table, "offer") else "NULL"
        offer_volume_col = (
            "offer_volume" if _has_column(conn, schema, table, "offer_volume") else "NULL"
        )
    return SQL_LATEST_FEATURES_TEMPLATE.format(
        bid_col=bid_col,
        bid_volume_col=bid_volume_col,
        offer_col=offer_col,
        offer_volume_col=offer_volume_col,
    )


def _score_row(
    row: Dict[str, Any],
    min_price: float,
    max_price: float | None,
) -> Recommendation | None:
    if row["close"] is None or row["avg_vol20"] is None:
        return None
    if row["close"] < min_price:
        return None
    if max_price is not None and row["close"] > max_price:
        return None
    if row["ma20"] is None or row["ma50"] is None or row["close_20d_ago"] is None:
        return None

    reasons: List[str] = []
    score = 0.0

    close = float(row["close"])
    high = float(row["high"]) if row["high"] is not None else None
    low = float(row["low"]) if row["low"] is not None else None
    bid = float(row["bid"]) if row["bid"] is not None else None
    offer = float(row["offer"]) if row["offer"] is not None else None
    bid_volume = float(row["bid_volume"]) if row["bid_volume"] is not None else None
    offer_volume = float(row["offer_volume"]) if row["offer_volume"] is not None else None
    ma20 = float(row["ma20"])
    ma50 = float(row["ma50"])
    avg_vol20 = float(row["avg_vol20"])
    volume = float(row["volume"] or 0.0)
    close_20d_ago = float(row["close_20d_ago"])
    max_high_20d = row["max_high_20d"]

    if close > ma20 and ma20 > ma50:
        score += 30
        reasons.append("trend_up_ma20_ma50")

    if close_20d_ago > 0:
        ret_20d = (close / close_20d_ago) - 1.0
        score += max(min(ret_20d, 0.30), -0.20) * 100
        if ret_20d > 0.05:
            reasons.append("momentum_20d_pos")

    if max_high_20d is not None and close >= float(max_high_20d):
        score += 25
        reasons.append("breakout_20d")

    if avg_vol20 > 0:
        vol_ratio = volume / avg_vol20
        score += min(vol_ratio, 5.0) * 5
        if vol_ratio >= 2.0:
            reasons.append("volume_spike")

    bid_offer_imbalance = None
    spread_pct = None
    if bid_volume is not None and offer_volume is not None:
        denom = bid_volume + offer_volume
        if denom > 0:
            bid_offer_imbalance = (bid_volume - offer_volume) / denom
            if bid_offer_imbalance <= -0.2:
                reasons.append("offer_dominant")
            if bid_offer_imbalance >= 0.2:
                reasons.append("bid_dominant")
    if bid is not None and offer is not None and close > 0:
        spread_pct = (offer - bid) / close
        if spread_pct >= 0.02:
            reasons.append("wide_spread")

    # Pivot-based targets (classic)
    if high is None or low is None:
        return None
    pivot = (high + low + close) / 3.0
    support1 = 2 * pivot - high
    resist1 = 2 * pivot - low
    support2 = pivot - (high - low)
    resist2 = pivot + (high - low)
    support3 = low - 2 * (high - pivot)
    resist3 = high + 2 * (pivot - low)

    risk_reward_t1 = None
    risk_reward_t2 = None
    risk_reward_t3 = None
    risk = pivot - support1
    if risk > 0:
        risk_reward_t1 = (resist1 - pivot) / risk
        risk_reward_t2 = (resist2 - pivot) / risk
        risk_reward_t3 = (resist3 - pivot) / risk

    return Recommendation(
        stock_code=row["stock_code"],
        trade_date=str(row["trade_date"]),
        close=close,
        score=round(score, 2),
        reasons=reasons,
        entry=round(pivot, 2),
        support1=round(support1, 2),
        support2=round(support2, 2),
        support3=round(support3, 2),
        resist1=round(resist1, 2),
        resist2=round(resist2, 2),
        resist3=round(resist3, 2),
        risk_reward_t1=round(risk_reward_t1, 2) if risk_reward_t1 is not None else None,
        risk_reward_t2=round(risk_reward_t2, 2) if risk_reward_t2 is not None else None,
        risk_reward_t3=round(risk_reward_t3, 2) if risk_reward_t3 is not None else None,
        bid_offer_imbalance=round(bid_offer_imbalance, 3) if bid_offer_imbalance is not None else None,
        spread_pct=round(spread_pct, 4) if spread_pct is not None else None,
    )


def recommend_stocks(
    limit: int = 30,
    min_price: float = 50.0,
    max_price: float | None = None,
    min_avg_volume_20d: float = 200000,
) -> List[Recommendation]:
    with _connect_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql = _build_sql(conn)
            try:
                cur.execute(sql)
            except psycopg2.errors.UndefinedColumn:
                conn.rollback()
                sql = _build_sql(conn, force_null_bid_offer=True)
                cur.execute(sql)
            rows = cur.fetchall()

    recs: List[Recommendation] = []
    for row in rows:
        if row["avg_vol20"] is None or float(row["avg_vol20"]) < min_avg_volume_20d:
            continue
        rec = _score_row(row, min_price=min_price, max_price=max_price)
        if rec is not None:
            recs.append(rec)

    recs.sort(key=lambda r: r.score, reverse=True)
    return recs[:limit]
