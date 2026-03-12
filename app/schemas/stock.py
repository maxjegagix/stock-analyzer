from pydantic import BaseModel
from datetime import date

class StockPrice(BaseModel):
    trade_date: date
    stock_code: str
    close: float