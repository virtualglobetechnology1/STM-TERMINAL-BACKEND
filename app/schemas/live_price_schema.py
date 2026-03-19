from typing import Optional, List, Literal
from pydantic import BaseModel, Field


# ─── Exchange / Mode Enums ────────────────────────────────────────────────────

ExchangeType = Literal[1, 2, 3, 4, 5, 7, 13]
SubscriptionMode = Literal[1, 2, 3]

EXCHANGE_NAME_MAP = {
    1: "NSE_CM",
    2: "NSE_FO",
    3: "BSE_CM",
    4: "BSE_FO",
    5: "MCX_FO",
    7: "NCX_FO",
    13: "CDE_FO",
}

MODE_NAME_MAP = {
    1: "LTP",
    2: "Quote",
    3: "SnapQuote",
}


# ─── Request Schemas ──────────────────────────────────────────────────────────

class LivePriceRequest(BaseModel):
    """
    Body sent by the client to POST /api/live-price/subscribe.
    Credentials + token info to start streaming.
    """
    jwt_token: str   = Field(..., description="JWT auth token from AngelOne Login API")
    api_key: str     = Field(..., description="AngelOne API key")
    client_code: str = Field(..., description="AngelOne trading account ID")
    feed_token: str  = Field(..., description="feedToken from Login API")

    tokens: List[str] = Field(
        ...,
        min_length=1,
        description="Token IDs to subscribe. e.g. ['10626'] for Infosys NSE",
    )
    exchange_type: ExchangeType = Field(
        default=1,
        description="1=NSE_CM | 2=NSE_FO | 3=BSE_CM | 4=BSE_FO | 5=MCX_FO | 7=NCX_FO | 13=CDE_FO",
    )
    mode: SubscriptionMode = Field(
        default=1,
        description="1=LTP (price only) | 2=Quote (OHLCV) | 3=SnapQuote (full depth)",
    )


class UnsubscribeRequest(BaseModel):
    tokens: List[str]
    exchange_type: ExchangeType = 1
    mode: SubscriptionMode = 1


# ─── Tick / Response Schemas ──────────────────────────────────────────────────

class BestFiveEntry(BaseModel):
    price: float
    quantity: int
    orders: int


class BestFiveData(BaseModel):
    buy: List[BestFiveEntry]
    sell: List[BestFiveEntry]


class TickData(BaseModel):
    """Parsed tick pushed to downstream WebSocket clients as JSON."""
    mode: str
    exchange: str
    token: str
    sequence_number: Optional[int] = None
    exchange_timestamp: Optional[str] = None
    ltp: float = Field(..., description="Last Traded Price in ₹")

    # Quote fields (mode >= 2)
    last_traded_qty: Optional[int] = None
    avg_traded_price: Optional[float] = None
    volume: Optional[int] = None
    total_buy_qty: Optional[float] = None
    total_sell_qty: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None

    # SnapQuote fields (mode == 3)
    last_traded_timestamp: Optional[str] = None
    open_interest: Optional[int] = None
    best_five: Optional[BestFiveData] = None
    upper_circuit: Optional[float] = None
    lower_circuit: Optional[float] = None
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None


class WSConnectionStatus(BaseModel):
    connected: bool
    active_subscriptions: List[dict]
    connected_clients: int
    message: str
