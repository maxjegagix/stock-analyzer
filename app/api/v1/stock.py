from fastapi import APIRouter, Query

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
