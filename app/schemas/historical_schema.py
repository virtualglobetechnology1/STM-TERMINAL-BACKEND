from pydantic import BaseModel


class HistoricalRequest(BaseModel):
    ticker: str
    start_date: str
    end_date: str