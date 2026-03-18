import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.recommendation_service import _score_row


def test_score_row_happy_path():
    row = {
        "stock_code": "TEST",
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
    assert rec is not None
    assert rec.stock_code == "TEST"
    assert "trend_up_ma20_ma50" in rec.reasons
    assert "momentum_20d_pos" in rec.reasons
    assert "breakout_20d" in rec.reasons
    assert "volume_spike" in rec.reasons
    assert "bid_dominant" in rec.reasons

    # Pivot targets sanity checks
    assert rec.entry > 0
    assert rec.resist1 > rec.entry
    assert rec.support1 < rec.entry
    assert rec.risk_reward_t1 is not None
    assert rec.risk_reward_t1 > 0


def test_score_row_max_price_filter():
    row = {
        "stock_code": "TEST",
        "trade_date": "2026-03-16",
        "close": 200.0,
        "high": 210.0,
        "low": 190.0,
        "bid": None,
        "offer": None,
        "bid_volume": None,
        "offer_volume": None,
        "ma20": 195.0,
        "ma50": 190.0,
        "avg_vol20": 1000.0,
        "close_20d_ago": 180.0,
        "max_high_20d": 205.0,
        "volume": 1200,
    }

    rec = _score_row(row, min_price=50.0, max_price=150.0)
    assert rec is None
