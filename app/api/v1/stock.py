from fastapi import APIRouter, Query

from app.services.backtest_service import backtest_screener, analyze_method_logs
from app.services.recommendation_service import recommend_stocks

router = APIRouter(prefix="/stocks", tags=["stocks"])

@router.get("/ping")
def ping():
    return {"message": "stocks service alive"}


@router.get("/recommendations/momentum_ma_breakout")
def recommend_momentum_ma_breakout(
    limit: int = Query(30, ge=1, le=500),
    min_price: float = Query(50.0, ge=0),
    max_price: float | None = Query(None, ge=0),
    min_avg_volume_20d: float = Query(200000, ge=0),
):
    recs = recommend_stocks(
        limit=limit,
        min_price=min_price,
        max_price=max_price,
        min_avg_volume_20d=min_avg_volume_20d,
    )
    return {
        "method": "momentum_ma_breakout",
        "count": len(recs),
        "items": [
            {
                "stock_code": r.stock_code,
                "trade_date": r.trade_date,
                "close": r.close,
                "score": r.score,
                "reasons": r.reasons,
                "entry": r.entry,
                "support1": r.support1,
                "support2": r.support2,
                "support3": r.support3,
                "resist1": r.resist1,
                "resist2": r.resist2,
                "resist3": r.resist3,
                "risk_reward_t1": r.risk_reward_t1,
                "risk_reward_t2": r.risk_reward_t2,
                "risk_reward_t3": r.risk_reward_t3,
                "bid_offer_imbalance": r.bid_offer_imbalance,
                "spread_pct": r.spread_pct,
            }
            for r in recs
        ],
    }


@router.get("/recommendations/momentum_ma_breakout/backtest")
def backtest_momentum_ma_breakout(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
    min_price: float = Query(50.0, ge=0),
    max_price: float | None = Query(None, ge=0),
    min_avg_volume_20d: float = Query(200000, ge=0),
    limit_per_day: int | None = Query(None, ge=1, le=500),
):
    result = backtest_screener(
        start_date=start_date,
        end_date=end_date,
        min_price=min_price,
        max_price=max_price,
        min_avg_volume_20d=min_avg_volume_20d,
        limit_per_day=limit_per_day,
    )
    return {
        "method": "momentum_ma_breakout",
        "params": {
            "start_date": start_date,
            "end_date": end_date,
            "min_price": min_price,
            "max_price": max_price,
            "min_avg_volume_20d": min_avg_volume_20d,
            "limit_per_day": limit_per_day,
        },
        "effective_start_date": result["effective_start_date"],
        "effective_end_date": result["effective_end_date"],
        "per_date": result["per_date"],
        "summary": {
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
        "assumptions": {
            "entry_hit_rule": "next_low <= entry <= next_high",
            "hit_rule": "next_high >= resist1 (R1)",
            "stop_rule": "next_low <= support1",
            "proof_date": "next trading day",
        },
    }


@router.get("/recommendations/momentum_ma_breakout/analysis")
def analyze_momentum_ma_breakout():
    return analyze_method_logs("momentum_ma_breakout")
