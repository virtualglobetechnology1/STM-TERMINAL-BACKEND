"""
AngelOne SmartAPI WebSocket Streaming 2.0 — Service Layer

Responsibilities:
  - Open / maintain the upstream WebSocket to AngelOne
  - Send heartbeat ping every 30 seconds
  - Subscribe / unsubscribe tokens (grouped by exchange + mode)
  - Parse every incoming binary tick (Little-Endian, spec-compliant)
  - Invoke an async callback for every parsed tick
"""

import asyncio
import requests
import json
import struct
import logging
from datetime import datetime
from typing import Optional, Callable, Awaitable, List

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

WS_BASE_URL       = "wss://smartapisocket.angelone.in/smart-stream"
HEARTBEAT_SECONDS = 30

EXCHANGE_NAME_MAP: dict[int, str] = {
    1: "NSE_CM",
    2: "NSE_FO",
    3: "BSE_CM",
    4: "BSE_FO",
    5: "MCX_FO",
    7: "NCX_FO",
    13: "CDE_FO",
}

MODE_NAME_MAP: dict[int, str] = {
    1: "LTP",
    2: "Quote",
    3: "SnapQuote",
}

TickCallback  = Callable[[dict], Awaitable[None]]
ErrorCallback = Callable[[str], Awaitable[None]]


# ─── Binary Parser ─────────────────────────────────────────────────────────────

def _read_token_str(data: bytes, offset: int = 2, length: int = 25) -> str:
    """
    Extract null-terminated UTF-8 token string from raw bytes.
    Token occupies bytes [2 … 26], null-terminated.
    """
    raw      = data[offset : offset + length]
    null_pos = raw.find(b"\x00")
    return raw[:null_pos].decode("utf-8") if null_pos != -1 else raw.decode("utf-8")


def _parse_best_five(data: bytes, base: int = 147) -> dict:
    """
    Parse 10 best-five packets starting at `base`.

    Each packet (20 bytes):
      [0:2]   int16  → buy/sell flag (1 = buy, 0 = sell)
      [2:10]  int64  → quantity
      [10:18] int64  → price (paise → ₹ on return)
      [18:20] int16  → number of orders
    """
    buys: list  = []
    sells: list = []

    for i in range(10):
        off   = base + i * 20
        flag  = struct.unpack_from("<h", data, off)[0]
        qty   = struct.unpack_from("<q", data, off + 2)[0]
        price = struct.unpack_from("<q", data, off + 10)[0] / 100.0
        orders= struct.unpack_from("<h", data, off + 18)[0]
        entry = {"price": price, "quantity": qty, "orders": orders}
        (buys if flag == 1 else sells).append(entry)

    return {"buy": buys, "sell": sells}


def parse_binary_tick(raw: bytes) -> Optional[dict]:
    """
    Parse a binary message from AngelOne WebSocket (Little-Endian).

    Binary layout (Section-1 Payload):
    ┌──────┬──────┬──────────────────────┬────────┬────────┬────────┐
    │  [0] │  [1] │      [2 … 26]        │[27…34] │[35…42] │[43…50] │
    │  i8  │  i8  │  25-byte token str   │ i64    │ i64    │ i32    │
    │ mode │ exch │  (null-terminated)   │ seqnum │ ex_ts  │  ltp   │
    └──────┴──────┴──────────────────────┴────────┴────────┴────────┘
    LTP packet ends at byte 51  (packet size = 51 bytes)

    Quote continues:
      [51]  i64  last_traded_qty
      [59]  i64  avg_traded_price   (paise)
      [67]  i64  volume
      [75]  f64  total_buy_qty
      [83]  f64  total_sell_qty
      [91]  i64  open   (paise)
      [99]  i64  high   (paise)
      [107] i64  low    (paise)
      [115] i64  close  (paise)
    Quote packet ends at byte 123 (packet size = 123 bytes)

    SnapQuote continues:
      [123] i64  last_traded_timestamp (epoch ms)
      [131] i64  open_interest
      [139] f64  open_interest_change_pct  (DUMMY — ignore)
      [147] 200 bytes  best_five (10 × 20-byte packets)
      [347] i64  upper_circuit_limit (paise)
      [355] i64  lower_circuit_limit (paise)
      [363] i64  52w_high            (paise)
      [371] i64  52w_low             (paise)
    SnapQuote packet ends at byte 379 (packet size = 379 bytes)
    """
    if len(raw) < 51:
        logger.warning("Binary tick too short: %d bytes, skipping.", len(raw))
        return None

    try:
        mode    = struct.unpack_from("<b", raw, 0)[0]
        ex_type = struct.unpack_from("<b", raw, 1)[0]
        token   = _read_token_str(raw)

        seq_num = struct.unpack_from("<q", raw, 27)[0]
        ex_ts   = struct.unpack_from("<q", raw, 35)[0]

        ltp_paise = struct.unpack_from("<i", raw, 43)[0]
        ltp       = ltp_paise / 100.0

        tick: dict = {
            "mode":               MODE_NAME_MAP.get(mode, str(mode)),
            "exchange":           EXCHANGE_NAME_MAP.get(ex_type, str(ex_type)),
            "token":              token,
            "sequence_number":    seq_num,
            "exchange_timestamp": datetime.fromtimestamp(ex_ts / 1000).isoformat() if ex_ts else None,
            "ltp":                ltp,
        }

        # ── Quote fields (mode 2 or 3) ────────────────────────────────────────
        if mode >= 2 and len(raw) >= 123:
            tick.update({
                "last_traded_qty":  struct.unpack_from("<q", raw, 51)[0],
                "avg_traded_price": struct.unpack_from("<q", raw, 59)[0] / 100.0,
                "volume":           struct.unpack_from("<q", raw, 67)[0],
                "total_buy_qty":    struct.unpack_from("<d", raw, 75)[0],
                "total_sell_qty":   struct.unpack_from("<d", raw, 83)[0],
                "open":             struct.unpack_from("<q", raw, 91)[0]  / 100.0,
                "high":             struct.unpack_from("<q", raw, 99)[0]  / 100.0,
                "low":              struct.unpack_from("<q", raw, 107)[0] / 100.0,
                "close":            struct.unpack_from("<q", raw, 115)[0] / 100.0,
            })

        # ── SnapQuote fields (mode 3) ─────────────────────────────────────────
        if mode == 3 and len(raw) >= 379:
            ltt_ms = struct.unpack_from("<q", raw, 123)[0]
            tick.update({
                "last_traded_timestamp": datetime.fromtimestamp(ltt_ms / 1000).isoformat()
                                         if ltt_ms else None,
                "open_interest":         struct.unpack_from("<q", raw, 131)[0],
                # [139] open_interest_change_pct → dummy field, skip
                "best_five":             _parse_best_five(raw, base=147),
                "upper_circuit":         struct.unpack_from("<q", raw, 347)[0] / 100.0,
                "lower_circuit":         struct.unpack_from("<q", raw, 355)[0] / 100.0,
                "week_52_high":          struct.unpack_from("<q", raw, 363)[0] / 100.0,
                "week_52_low":           struct.unpack_from("<q", raw, 371)[0] / 100.0,
            })

        return tick

    except struct.error as exc:
        logger.error("Binary parse error: %s", exc)
        return None


# ─── WebSocket Service ────────────────────────────────────────────────────────

class AngelOneWSService:
    """
    Manages one AngelOne SmartAPI WebSocket connection.

    Example
    -------
    service = AngelOneWSService(
        client_code="ABCD1234",
        feed_token="<feed_token>",
        api_key="<api_key>",
        on_tick=my_callback,
    )
    await service.connect()
    await service.subscribe(tokens=["10626"], exchange_type=1, mode=1)
    """

    def __init__(
        self,
        client_code:  str,
        feed_token:   str,
        api_key:      str,
        on_tick:      Optional[TickCallback]  = None,
        on_error:     Optional[ErrorCallback] = None,
    ) -> None:
        self.client_code = client_code
        self.feed_token  = feed_token
        self.api_key     = api_key
        self.on_tick     = on_tick
        self.on_error    = on_error

        self._ws = None
        self._running         = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._listen_task:    Optional[asyncio.Task] = None

        # Persisted subscriptions for reconnect: key = (token, exchange, mode)
        self._subscriptions: dict[tuple, dict] = {}

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def ws_url(self) -> str:
        """Browser-compatible WS URL with credentials as query params."""
        return (
            f"{WS_BASE_URL}"
            f"?clientCode={self.client_code}"
            f"&feedToken={self.feed_token}"
            f"&apiKey={self.api_key}"
        )

    @property
    def is_connected(self) -> bool:
        """Compatible with websockets v12, v13, v14, v15, v16+"""
        if self._ws is None:
            return False
        try:
            # websockets < v14: has .closed attribute
            return not self._ws.closed
        except AttributeError:
            pass
        try:
            # websockets v14+: uses .state
            from websockets.connection import State
            return self._ws.state is State.OPEN
        except Exception:
            pass
        try:
            # fallback: check state name string
            return self._ws.state.name == "OPEN"
        except Exception:
            return True  # assume connected if we can't tell

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Open WebSocket and start heartbeat + listener tasks."""
        logger.info("Connecting to AngelOne WebSocket …")
        self._ws = await websockets.connect(
            self.ws_url,
            ping_interval=None,   # manual heartbeat
        )
        self._running = True
        logger.info("AngelOne WebSocket connected.")

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._listen_task    = asyncio.create_task(self._listen_loop())

        # Re-subscribe if this is a reconnect
        if self._subscriptions:
            logger.info("Re-subscribing %d token(s) after reconnect …", len(self._subscriptions))
            await self._resubscribe_all()

    async def disconnect(self) -> None:
        """Gracefully close everything."""
        self._running = False
        for task in (self._heartbeat_task, self._listen_task):
            if task:
                task.cancel()
        if self._ws:
            await self._ws.close()
            self._ws = None
        logger.info("AngelOne WebSocket disconnected.")

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """Send 'ping' every 30 s to keep the connection alive."""
        while self._running:
            await asyncio.sleep(HEARTBEAT_SECONDS)
            if self.is_connected:
                try:
                    await self._ws.send("ping")
                    logger.debug("Heartbeat → ping")
                except ConnectionClosed:
                    logger.warning("Heartbeat failed: connection closed.")
                    break

    # ── Listener ──────────────────────────────────────────────────────────────

    async def _listen_loop(self) -> None:
        """Continuously receive and dispatch messages."""
        try:
            async for message in self._ws:
                await self._dispatch(message)
        except ConnectionClosed as exc:
            logger.warning("Connection closed: %s", exc)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.exception("Unexpected error in listen loop: %s", exc)

    async def _dispatch(self, message) -> None:
        # ── Text messages ─────────────────────────────────────────────────────
        if isinstance(message, str):
            if message == "pong":
                logger.debug("Heartbeat ← pong")
                return
            try:
                payload = json.loads(message)
                if "errorCode" in payload:
                    err = f"[{payload['errorCode']}] {payload.get('errorMessage', '')}"
                    logger.error("Server error: %s", err)
                    if self.on_error:
                        await self.on_error(err)
            except json.JSONDecodeError:
                logger.debug("Non-JSON text: %s", message[:120])
            return

        # ── Binary messages ───────────────────────────────────────────────────
        tick = parse_binary_tick(bytes(message))
        if tick and self.on_tick:
            await self.on_tick(tick)

    # ── Subscription API ──────────────────────────────────────────────────────

    async def subscribe(
        self,
        tokens:        List[str],
        exchange_type: int,
        mode:          int,
        correlation_id: Optional[str] = None,
    ) -> None:
        """
        Subscribe to live feed for the given tokens.

        Parameters
        ----------
        tokens        : token IDs, e.g. ["10626", "3045"]
        exchange_type : 1=NSE_CM, 2=NSE_FO, 3=BSE_CM, 4=BSE_FO,
                        5=MCX_FO, 7=NCX_FO, 13=CDE_FO
        mode          : 1=LTP, 2=Quote, 3=SnapQuote
        """
        self._assert_connected()
        payload = self._build_payload(1, tokens, exchange_type, mode, correlation_id)
        await self._ws.send(json.dumps(payload))

        for token in tokens:
            self._subscriptions[(token, exchange_type, mode)] = {
                "token": token,
                "exchange_type": exchange_type,
                "mode": mode,
            }

        logger.info(
            "Subscribed: tokens=%s  exchange=%s  mode=%s",
            tokens,
            EXCHANGE_NAME_MAP.get(exchange_type, exchange_type),
            MODE_NAME_MAP.get(mode, mode),
        )

    async def unsubscribe(
        self,
        tokens:        List[str],
        exchange_type: int,
        mode:          int,
        correlation_id: Optional[str] = None,
    ) -> None:
        """
        Unsubscribe tokens. Server gracefully ignores unknown tokens.
        """
        self._assert_connected()
        payload = self._build_payload(0, tokens, exchange_type, mode, correlation_id)
        await self._ws.send(json.dumps(payload))

        for token in tokens:
            self._subscriptions.pop((token, exchange_type, mode), None)

        logger.info("Unsubscribed: tokens=%s", tokens)

    async def _resubscribe_all(self) -> None:
        """Group existing subscriptions and replay them after a reconnect."""
        grouped: dict[tuple, List[str]] = {}
        for sub in self._subscriptions.values():
            key = (sub["exchange_type"], sub["mode"])
            grouped.setdefault(key, []).append(sub["token"])

        for (exchange_type, mode), tokens in grouped.items():
            await self.subscribe(tokens, exchange_type, mode)

    def get_active_subscriptions(self) -> List[dict]:
        return list(self._subscriptions.values())

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _assert_connected(self) -> None:
        if not self.is_connected:
            raise RuntimeError(
                "WebSocket is not connected. Call await service.connect() first."
            )

    @staticmethod
    def _build_payload(
        action:        int,
        tokens:        List[str],
        exchange_type: int,
        mode:          int,
        correlation_id: Optional[str],
    ) -> dict:
        payload: dict = {
            "action": action,
            "params": {
                "mode": mode,
                "tokenList": [{"exchangeType": exchange_type, "tokens": tokens}],
            },
        }
        if correlation_id:
            payload["correlationID"] = correlation_id
        return payload
    

    import requests
import mysql.connector
from app.core.config import (
    ANGEL_API_KEY,
    ANGEL_JWT_TOKEN,
    ANGEL_LTP_URL,
    DB_HOST,
    DB_USER,
    DB_PASSWORD,
    DB_NAME
)

# ============================================
# DB SE SECURITY CODE FETCH KARO
# ============================================
def get_security_code_from_db(ticker: str, exchange: str = None):
    """
    Ticker name se security code lo DB se

    ticker: "RELIANCE"
    exchange: "NSE" ya "BSE" (optional)

    Returns:
    {
        "ticker_id": 123,
        "ticker": "RELIANCE",
        "security_code": "2885",
        "exchange": "NSE"
    }
    """
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor(dictionary=True)

        # Exchange filter hai ya nahi?
        if exchange:
            query = """
                SELECT 
                    ticker_id,
                    ticker,
                    ticker_security_code,
                    ticker_exchange
                FROM dd_ticker_list
                WHERE ticker = %s
                AND ticker_exchange = %s
                AND ticker_security_code IS NOT NULL
                LIMIT 1
            """
            cursor.execute(query, (ticker.upper(), exchange.upper()))
        else:
            query = """
                SELECT 
                    ticker_id,
                    ticker,
                    ticker_security_code,
                    ticker_exchange
                FROM dd_ticker_list
                WHERE ticker = %s
                AND ticker_security_code IS NOT NULL
                LIMIT 1
            """
            cursor.execute(query, (ticker.upper(),))

        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if not result:
            return None

        return {
            "ticker_id":     result["ticker_id"],
            "ticker":        result["ticker"],
            "security_code": str(result["ticker_security_code"]),
            "exchange":      result["ticker_exchange"]
        }

    except Exception as e:
        print(f"DB Error: {e}")
        return None


# ============================================
# ANGEL ONE SE LIVE PRICE FETCH KARO
# ============================================
def fetch_price_from_angel(exchange: str, security_code: str):
    """
    Security code se live price lo AngelOne se
    """
    headers = {
        "Authorization":    f"Bearer {ANGEL_JWT_TOKEN}",
        "Content-Type":     "application/json",
        "Accept":           "application/json",
        "X-UserType":       "USER",
        "X-SourceID":       "WEB",
        "X-ClientLocalIP":  "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress":     "00:00:00:00:00:00",
        "X-PrivateKey":     ANGEL_API_KEY
    }

    body = {
        "mode": "LTP",
        "exchangeTokens": {
            exchange: [security_code]
        }
    }

    try:
        response = requests.post(
            ANGEL_LTP_URL,
            json=body,
            headers=headers
        )
        data = response.json()

        if not data.get("status"):
            return None, data.get("message", "API Error")

        fetched = data["data"]["fetched"]
        if not fetched:
            return None, "No price data"

        ltp = fetched[0]["ltp"]
        return ltp, None

    except Exception as e:
        return None, str(e)


# ============================================
# MAIN FUNCTION — TICKER → PRICE
# ============================================
def get_price_by_ticker(ticker: str, exchange: str = None):
    """
    Sirf ticker name do — price milegi

    Usage:
    result = get_price_by_ticker("RELIANCE")
    result = get_price_by_ticker("RELIANCE", "NSE")
    """

    # Step 1: DB se security code lo
    stock_info = get_security_code_from_db(ticker, exchange)

    if not stock_info:
        return {
            "success": False,
            "error": f"Ticker '{ticker}' database mein nahi mila"
        }

    # print(f"DB se mila: {stock_info}")

    # Step 2: Angel One se price lo
    ltp, error = fetch_price_from_angel(
        stock_info["exchange"],
        stock_info["security_code"]
    )

    if error:
        return {
            "success": False,
            "error": error
        }

    # Step 3: Response banao
    return {
        "success":       True,
        "ticker":        stock_info["ticker"],
        "exchange":      stock_info["exchange"],
        "security_code": stock_info["security_code"],
        "ltp":           ltp
    }


# ============================================
# MULTIPLE TICKERS KI PRICE
# ============================================
def get_prices_by_tickers(tickers: list, exchange: str = None):
    """
    Multiple tickers ki prices

    Usage:
    results = get_prices_by_tickers(["RELIANCE", "INFY", "TCS"])
    """
    results = {}

    for ticker in tickers:
        result = get_price_by_ticker(ticker, exchange)
        results[ticker] = result

    return results