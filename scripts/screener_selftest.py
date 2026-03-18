import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.recommendation_service import _score_row


def main():
    row = {
        "stock_code": "SAMPLE",
        "trade_date": "2026-03-16",
        "close": 100.0,
        "high": 105.0,
        "low": 95.0,
        "bid": 99.5,
        "offer": 100.5,
        "bid_volume": 3000,
        "offer_volume": 1000,
        "ma20": 95.0,
        "ma50": 90.0,
        "avg_vol20": 1000.0,
        "close_20d_ago": 90.0,
        "max_high_20d": 99.0,
        "volume": 3000,
    }

    rec = _score_row(row, min_price=50.0, max_price=None)
    if rec is None:
        print("Selftest failed: no recommendation produced.")
        raise SystemExit(1)

    print("Selftest OK")
    print(
        "entry/support/resist:",
        rec.entry,
        rec.support1,
        rec.support2,
        rec.support3,
        rec.resist1,
        rec.resist2,
        rec.resist3,
    )
    print(
        "risk_reward:",
        rec.risk_reward_t1,
        rec.risk_reward_t2,
        rec.risk_reward_t3,
    )
    print("reasons:", ",".join(rec.reasons))


if __name__ == "__main__":
    main()
