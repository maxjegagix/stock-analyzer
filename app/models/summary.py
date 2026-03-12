from sqlalchemy import Column, Date, Numeric, BigInteger, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class StockSummary(Base):
    __tablename__ = "summary"
    __table_args__ = {"schema": "stock"}

    trade_date = Column(Date, primary_key=True)
    stock_code = Column(String(10), primary_key=True)

    open_price = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)
    volume = Column(BigInteger)