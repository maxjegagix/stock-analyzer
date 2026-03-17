import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.recommendation_service import recommend_stocks


def main():
    parser = argparse.ArgumentParser(description="Recommend stocks for next-day potential.")
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--min-price", type=float, default=50.0)
    parser.add_argument("--max-price", type=float, default=None)
    parser.add_argument("--min-avg-volume-20d", type=float, default=200000)
    args = parser.parse_args()

    recs = recommend_stocks(
        limit=args.limit,
        min_price=args.min_price,
        max_price=args.max_price,
        min_avg_volume_20d=args.min_avg_volume_20d,
    )

    print(
        "stock_code,trade_date,close,score,entry,support1,support2,support3,"
        "resist1,resist2,resist3,risk_reward_t1,risk_reward_t2,risk_reward_t3,"
        "bid_offer_imbalance,spread_pct,reasons"
    )
    for r in recs:
        print(
            f"{r.stock_code},{r.trade_date},{r.close},{r.score},"
            f"{r.entry},{r.support1},{r.support2},{r.support3},"
            f"{r.resist1},{r.resist2},{r.resist3},"
            f"{r.risk_reward_t1},{r.risk_reward_t2},{r.risk_reward_t3},"
            f"{r.bid_offer_imbalance},{r.spread_pct},"
            f"{'|'.join(r.reasons)}"
        )


if __name__ == "__main__":
    main()
