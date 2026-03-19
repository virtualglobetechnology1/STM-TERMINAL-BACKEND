from sqlalchemy import Column, Integer, String, Float, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class BacktestResult(Base):
    __tablename__ = "backtest_results"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    request_payload = Column(JSON)
    result_summary = Column(JSON)
    equity_curve = Column(JSON)
    trade_log = Column(JSON)