from pydantic import BaseModel, Field
from typing import List, Optional


class OHLCBar(BaseModel):
    Date: str
    Time: str
    Open: float
    High: float
    Low: float
    Close: float


class StockBacktestRequest(BaseModel):
    ticker: str = Field(..., min_length=1)
    starting_cash: float = Field(..., gt=0)
    k: float = Field(..., gt=1)
    stepsize: float = Field(..., gt=0)
    bars: List[OHLCBar] = Field(default_factory=list)


class PortfolioBacktestRequest(BaseModel):
    stocks: List[StockBacktestRequest] = Field(default_factory=list)