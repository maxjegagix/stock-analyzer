from sqlalchemy.orm import Session
from app.models.summary import StockSummary

def get_latest_prices(db: Session, stock_code: str):
    return (
        db.query(StockSummary)
        .filter(StockSummary.stock_code == stock_code)
        .order_by(StockSummary.trade_date.desc())
        .limit(50)
        .all()
    )