import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.backtest_service import backtest_screener


def main():
    parser = argparse.ArgumentParser(description="Backtest screener hit rate.")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--min-price", type=float, default=50.0)
    parser.add_argument("--max-price", type=float, default=None)
    parser.add_argument("--min-avg-volume-20d", type=float, default=200000)
    parser.add_argument("--limit-per-day", type=int, default=None)
    args = parser.parse_args()

    result = backtest_screener(
        start_date=args.start_date,
        end_date=args.end_date,
        min_price=args.min_price,
        max_price=args.max_price,
        min_avg_volume_20d=args.min_avg_volume_20d,
        limit_per_day=args.limit_per_day,
    )

    print(f"total_signals={result['total_signals']}")
    print(f"total_entry_hits={result['total_entry_hits']}")
    print(f"hits_r1={result['hits_r1']}")
    print(f"hits_r2={result['hits_r2']}")
    print(f"hits_r3={result['hits_r3']}")
    print(f"stop_hits={result['stop_hits']}")
    print(f"entry_rate_pct={result['entry_rate_pct']:.2f}")
    print(f"hit_rate_pct={result['hit_rate_pct']:.2f}")
    print(f"hit_r2_rate_pct={result['hit_r2_rate_pct']:.2f}")
    print(f"hit_r3_rate_pct={result['hit_r3_rate_pct']:.2f}")
    print(f"stop_rate_pct={result['stop_rate_pct']:.2f}")
    print(f"avg_max_gain_pct={result['avg_max_gain_pct']:.2f}")
    print(f"median_max_gain_pct={result['median_max_gain_pct']:.2f}")
    print(f"avg_max_drawdown_pct={result['avg_max_drawdown_pct']:.2f}")
    print(f"median_max_drawdown_pct={result['median_max_drawdown_pct']:.2f}")
    print(f"avg_close_pct={result['avg_close_pct']:.2f}")
    print(f"median_close_pct={result['median_close_pct']:.2f}")
    print(f"avg_r_multiple={result['avg_r_multiple']:.2f}")
    print(f"median_r_multiple={result['median_r_multiple']:.2f}")
    print("date,signals,entry_hits,hit_r1,hit_r2,hit_r3,stop_hits,proof_date")
    for item in result["per_date"]:
        print(
            f"{item['date']},{item['total_signals']},{item['entry_hits']},"
            f"{item['hit_r1']},{item['hit_r2']},{item['hit_r3']},"
            f"{item['stop_hits']},{item['proof_date']}"
        )


if __name__ == "__main__":
    main()
