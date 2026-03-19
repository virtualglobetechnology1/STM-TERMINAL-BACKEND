"""
Live Price WebSocket Router
===========================

WS  /api/live-price/stream-by-ticker
    Connect with just ticker names:
        ws://localhost:8000/api/live-price/stream-by-ticker?tickers=5PAISA&mode=2

    After connecting, send JSON messages to change subscriptions:

    Subscribe new tickers:
        { "action": "subscribe", "tickers": "RELIANCE,TCS", "mode": 2 }

    Unsubscribe tickers:
        { "action": "unsubscribe", "tickers": "5PAISA" }

    Check active subscriptions:
        { "action": "status" }
"""

import asyncio
import json
import logging
from typing import Optional, List

import mysql.connector
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Query, status
from pydantic import BaseModel

from app.schemas.live_price_schema import (
    LivePriceRequest,
    UnsubscribeRequest,
    WSConnectionStatus,
)
from app.services.live_price_service import (
    AngelOneWSService,
    get_price_by_ticker,
    get_prices_by_tickers,
)
from app.services.angel_token_manager import token_manager
from app.utils.response import success_response, error_response
from app.core.config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME

logger = logging.getLogger(__name__)
router = APIRouter()

EXCHANGE_TYPE_MAP = {
    "NSE": 1, "BSE": 3, "NSE_FO": 2, "BSE_FO": 4, "MCX": 5,
}


# ─── DB Helper ────────────────────────────────────────────────────────────────

def get_security_codes_for_tickers(tickers: List[str], exchange: str = None):
    try:
        conn = mysql.connector.connect(
            host=DB_HOST, user=DB_USER,
            password=DB_PASSWORD, database=DB_NAME
        )
        cursor = conn.cursor(dictionary=True)
        placeholders = ",".join(["%s"] * len(tickers))
        upper_tickers = [t.upper() for t in tickers]

        if exchange:
            cursor.execute(f"""
                SELECT ticker, ticker_security_code, ticker_exchange
                FROM dd_ticker_list
                WHERE ticker IN ({placeholders})
                AND ticker_exchange = %s
                AND ticker_security_code IS NOT NULL
                AND ticker_status = 'Active'
            """, upper_tickers + [exchange.upper()])
        else:
            cursor.execute(f"""
                SELECT ticker, ticker_security_code, ticker_exchange
                FROM dd_ticker_list
                WHERE ticker IN ({placeholders})
                AND ticker_security_code IS NOT NULL
                AND ticker_status = 'Active'
            """, upper_tickers)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        exchange_groups: dict[int, list[str]] = {}
        ticker_map: dict[str, str] = {}
        seen = set()

        for row in rows:
            ticker = row["ticker"]
            if ticker in seen:
                continue
            seen.add(ticker)
            ex_type = EXCHANGE_TYPE_MAP.get(row["ticker_exchange"].upper(), 3)
            code = str(row["ticker_security_code"])
            exchange_groups.setdefault(ex_type, []).append(code)
            ticker_map[code] = ticker

        return exchange_groups, ticker_map

    except Exception as e:
        logger.error("DB lookup error: %s", e)
        return {}, {}


# ─── Connection Manager ───────────────────────────────────────────────────────

class LivePriceManager:
    def __init__(self) -> None:
        self._service: Optional[AngelOneWSService] = None
        self._clients: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def ensure_connected(self) -> None:
        async with self._lock:
            if self._service and self._service.is_connected:
                return
            creds = await token_manager.get_credentials()
            self._service = AngelOneWSService(
                client_code=creds["client_code"],
                feed_token=creds["feed_token"],
                api_key=creds["api_key"],
                on_tick=self._on_tick,
                on_error=self._on_error,
            )
            await self._service.connect()

    async def subscribe(self, tokens: list[str], exchange_type: int, mode: int) -> None:
        if not self._service or not self._service.is_connected:
            raise HTTPException(status_code=503, detail="AngelOne not connected.")
        await self._service.subscribe(tokens, exchange_type, mode)

    async def unsubscribe(self, tokens: list[str], exchange_type: int, mode: int) -> None:
        if self._service and self._service.is_connected:
            await self._service.unsubscribe(tokens, exchange_type, mode)

    async def disconnect(self) -> None:
        if self._service:
            await self._service.disconnect()
            self._service = None

    async def add_client(self, ws: WebSocket) -> None:
        await ws.accept()
        self._clients.append(ws)
        logger.info("Client connected. Total: %d", len(self._clients))

    def remove_client(self, ws: WebSocket) -> None:
        if ws in self._clients:
            self._clients.remove(ws)
        logger.info("Client disconnected. Total: %d", len(self._clients))

    async def _broadcast(self, payload: dict) -> None:
        dead = []
        for client in self._clients:
            try:
                await client.send_json(payload)
            except Exception:
                dead.append(client)
        for c in dead:
            self.remove_client(c)

    async def _on_tick(self, tick: dict) -> None:
        await self._broadcast(tick)

    async def _on_error(self, msg: str) -> None:
        await self._broadcast({"error": msg})

    def get_status(self) -> WSConnectionStatus:
        connected = bool(self._service and self._service.is_connected)
        subs = self._service.get_active_subscriptions() if self._service else []
        return WSConnectionStatus(
            connected=connected,
            active_subscriptions=subs,
            connected_clients=len(self._clients),
            message=f"Connected · {len(subs)} sub(s) · {len(self._clients)} client(s)."
                    if connected else "Disconnected.",
        )


_manager = LivePriceManager()


# ─── Shared helper: subscribe tickers and return result ───────────────────────

async def subscribe_tickers(ticker_list: List[str], mode: int, exchange: str = None) -> dict:
    """Lookup tickers in DB and subscribe. Returns result dict to send to client."""
    exchange_groups, ticker_map = get_security_codes_for_tickers(ticker_list, exchange)

    if not exchange_groups:
        return {"error": f"Tickers not found in DB: {ticker_list}"}

    await _manager.ensure_connected()

    all_codes = []
    errors = []
    for ex_type, codes in exchange_groups.items():
        try:
            await _manager.subscribe(tokens=codes, exchange_type=ex_type, mode=mode)
            all_codes.extend(codes)
        except Exception as exc:
            errors.append(str(exc))

    result = {
        "status":     "subscribed",
        "tickers":    ticker_list,
        "tokens":     all_codes,
        "ticker_map": ticker_map,
        "mode":       mode,
    }
    if errors:
        result["errors"] = errors
    return result


async def unsubscribe_tickers(ticker_list: List[str], mode: int, exchange: str = None) -> dict:
    """Lookup tickers in DB and unsubscribe."""
    exchange_groups, ticker_map = get_security_codes_for_tickers(ticker_list, exchange)

    if not exchange_groups:
        return {"error": f"Tickers not found in DB: {ticker_list}"}

    all_codes = []
    for ex_type, codes in exchange_groups.items():
        await _manager.unsubscribe(tokens=codes, exchange_type=ex_type, mode=mode)
        all_codes.extend(codes)

    return {
        "status":  "unsubscribed",
        "tickers": ticker_list,
        "tokens":  all_codes,
    }


# ─── WebSocket: stream-by-ticker ─────────────────────────────────────────────

@router.websocket("/live-price/stream-by-ticker")
async def ws_stream_by_ticker(
    websocket: WebSocket,
    tickers:  Optional[str] = Query(default=None),
    exchange: Optional[str] = Query(default=None),
    mode:     int           = Query(default=2),
):
    """
    Connect with ticker names — no credentials needed:
        ws://…/api/live-price/stream-by-ticker?tickers=5PAISA,RELIANCE&mode=2

    Send JSON messages to change subscriptions mid-session:
        Subscribe:   { "action": "subscribe",   "tickers": "RELIANCE,TCS", "mode": 2 }
        Unsubscribe: { "action": "unsubscribe",  "tickers": "5PAISA" }
        Status:      { "action": "status" }
    """
    await _manager.add_client(websocket)
    try:
        # ── Auto-subscribe from URL params ────────────────────────────────────
        if tickers:
            ticker_list = [t.strip() for t in tickers.split(",") if t.strip()]
            result = await subscribe_tickers(ticker_list, mode, exchange)
            await websocket.send_json(result)

        # ── Listen for mid-session commands ───────────────────────────────────
        while True:
            raw = await websocket.receive_text()

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json({"error": "Invalid JSON"})
                continue

            action = msg.get("action", "").lower()

            if action == "subscribe":
                raw_tickers = msg.get("tickers", "")
                ticker_list = [t.strip() for t in raw_tickers.split(",") if t.strip()]
                m = int(msg.get("mode", mode))
                ex = msg.get("exchange", exchange)
                result = await subscribe_tickers(ticker_list, m, ex)
                await websocket.send_json(result)

            elif action == "unsubscribe":
                raw_tickers = msg.get("tickers", "")
                ticker_list = [t.strip() for t in raw_tickers.split(",") if t.strip()]
                ex = msg.get("exchange", exchange)
                result = await unsubscribe_tickers(ticker_list, mode, ex)
                await websocket.send_json(result)

            elif action == "status":
                status_data = _manager.get_status()
                await websocket.send_json({
                    "status":               "ok",
                    "connected":            status_data.connected,
                    "active_subscriptions": status_data.active_subscriptions,
                    "connected_clients":    status_data.connected_clients,
                })

            else:
                await websocket.send_json({
                    "error": f"Unknown action '{action}'. Use: subscribe | unsubscribe | status"
                })

    except WebSocketDisconnect:
        _manager.remove_client(websocket)


# ─── WebSocket: raw token stream ─────────────────────────────────────────────

@router.websocket("/live-price/stream")
async def ws_live_price_stream(
    websocket: WebSocket,
    tokens:        Optional[str] = Query(default=None),
    exchange_type: int           = Query(default=1),
    mode:          int           = Query(default=1),
):
    await _manager.add_client(websocket)
    try:
        if tokens:
            token_list = [t.strip() for t in tokens.split(",") if t.strip()]
            try:
                await _manager.ensure_connected()
                await _manager.subscribe(tokens=token_list, exchange_type=exchange_type, mode=mode)
                await websocket.send_json({"status": "subscribed", "tokens": token_list})
            except Exception as exc:
                await websocket.send_json({"error": str(exc)})

        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        _manager.remove_client(websocket)


# ─── REST endpoints ───────────────────────────────────────────────────────────

@router.post("/live-price/subscribe")
async def subscribe_live_price(request: LivePriceRequest):
    try:
        await _manager.ensure_connected()
        await _manager.subscribe(tokens=request.tokens, exchange_type=request.exchange_type, mode=request.mode)
        return success_response(
            message="Subscribed successfully",
            data={"tokens": request.tokens, "exchange_type": request.exchange_type, "mode": request.mode},
        )
    except HTTPException:
        raise
    except Exception as e:
        return error_response("Failed to subscribe", str(e))


@router.post("/live-price/unsubscribe")
async def unsubscribe_live_price(request: UnsubscribeRequest):
    try:
        await _manager.unsubscribe(tokens=request.tokens, exchange_type=request.exchange_type, mode=request.mode)
        return success_response(message="Unsubscribed", data={"tokens": request.tokens})
    except Exception as e:
        return error_response("Failed to unsubscribe", str(e))


@router.get("/live-price/status")
def get_live_price_status():
    try:
        s = _manager.get_status()
        return success_response(message=s.message, data=s.model_dump())
    except Exception as e:
        return error_response("Failed to fetch status", str(e))


@router.delete("/live-price/disconnect")
async def disconnect_live_price():
    try:
        await _manager.disconnect()
        return success_response(message="Disconnected")
    except Exception as e:
        return error_response("Failed to disconnect", str(e))


# ─── REST: ticker-based price lookup ─────────────────────────────────────────

class SingleTickerRequest(BaseModel):
    ticker: str
    exchange: Optional[str] = None


class MultipleTickerRequest(BaseModel):
    tickers: List[str]
    exchange: Optional[str] = None


@router.post("/live-price/by-ticker")
def get_single_ticker_price(request: SingleTickerRequest):
    result = get_price_by_ticker(request.ticker, request.exchange)
    if not result["success"]:
        return error_response("Ticker not found", result["error"])
    return success_response(message="Success", data=result)


@router.post("/live-price/by-tickers")
def get_multiple_ticker_prices(request: MultipleTickerRequest):
    results = get_prices_by_tickers(request.tickers, request.exchange)
    return success_response(message="Success", data=results)