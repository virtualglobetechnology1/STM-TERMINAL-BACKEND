"""
Live Price WebSocket Router
===========================

Endpoints
---------
WS  /api/live-price/stream
    Downstream WebSocket — browser/client connects here to receive
    live tick JSON as it arrives from AngelOne.

    After connecting, the client may optionally send a JSON message
    to auto-subscribe:
        {
          "jwt_token":    "...",
          "api_key":      "...",
          "client_code":  "...",
          "feed_token":   "...",
          "tokens":       ["10626"],
          "exchange_type": 1,
          "mode":          1
        }

POST /api/live-price/subscribe
    Connect to AngelOne + subscribe tokens.
    Body: LivePriceRequest

POST /api/live-price/unsubscribe
    Unsubscribe tokens.
    Body: UnsubscribeRequest

GET  /api/live-price/status
    Return connection state and active subscriptions.

DELETE /api/live-price/disconnect
    Tear down the AngelOne upstream WebSocket.
"""

import asyncio
import requests
import json
import logging
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, status

from app.schemas.live_price_schema import (
    LivePriceRequest,
    UnsubscribeRequest,
    WSConnectionStatus,
)
from app.services.live_price_service import AngelOneWSService
from app.utils.response import success_response, error_response

logger = logging.getLogger(__name__)
router = APIRouter()


# ─── Connection Manager ───────────────────────────────────────────────────────

class LivePriceManager:
    """
    Singleton that owns:
    1. One upstream AngelOneWSService  (server → AngelOne)
    2. N downstream WebSocket clients  (browser/app → server)

    Every tick received from AngelOne is broadcast to all active clients.
    """

    def __init__(self) -> None:
        self._service: Optional[AngelOneWSService] = None
        self._clients: list[WebSocket] = []
        self._lock = asyncio.Lock()

    # ── Upstream (AngelOne) ───────────────────────────────────────────────────

    async def ensure_connected(
        self,
        client_code: str,
        feed_token:  str,
        api_key:     str,
    ) -> None:
        """Open the AngelOne WS if not already open (idempotent)."""
        async with self._lock:
            if self._service and self._service.is_connected:
                logger.info("AngelOne WS already connected — reusing.")
                return

            self._service = AngelOneWSService(
                client_code=client_code,
                feed_token=feed_token,
                api_key=api_key,
                on_tick=self._on_tick,
                on_error=self._on_error,
            )
            await self._service.connect()

    async def subscribe(self, tokens: list[str], exchange_type: int, mode: int) -> None:
        if not self._service or not self._service.is_connected:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    "AngelOne WebSocket not connected. "
                    "POST /api/live-price/subscribe with credentials first."
                ),
            )
        await self._service.subscribe(tokens, exchange_type, mode)

    async def unsubscribe(self, tokens: list[str], exchange_type: int, mode: int) -> None:
        if self._service and self._service.is_connected:
            await self._service.unsubscribe(tokens, exchange_type, mode)

    async def disconnect(self) -> None:
        if self._service:
            await self._service.disconnect()
            self._service = None

    # ── Downstream (browser clients) ──────────────────────────────────────────

    async def add_client(self, ws: WebSocket) -> None:
        await ws.accept()
        self._clients.append(ws)
        logger.info("Client connected. Total clients: %d", len(self._clients))

    def remove_client(self, ws: WebSocket) -> None:
        if ws in self._clients:
            self._clients.remove(ws)
        logger.info("Client disconnected. Total clients: %d", len(self._clients))

    async def _broadcast(self, payload: dict) -> None:
        """Send payload to all downstream clients; remove dead ones."""
        dead: list[WebSocket] = []
        for client in self._clients:
            try:
                await client.send_json(payload)
            except Exception:
                dead.append(client)
        for c in dead:
            self.remove_client(c)

    # ── Callbacks ─────────────────────────────────────────────────────────────

    async def _on_tick(self, tick: dict) -> None:
        logger.debug("Tick: token=%s  ltp=%s", tick.get("token"), tick.get("ltp"))
        await self._broadcast(tick)

    async def _on_error(self, msg: str) -> None:
        logger.error("AngelOne error: %s", msg)
        await self._broadcast({"error": msg})

    # ── Status ────────────────────────────────────────────────────────────────

    def get_status(self) -> WSConnectionStatus:
        connected = bool(self._service and self._service.is_connected)
        subs = self._service.get_active_subscriptions() if self._service else []
        return WSConnectionStatus(
            connected=connected,
            active_subscriptions=subs,
            connected_clients=len(self._clients),
            message=f"Connected · {len(subs)} subscription(s) · {len(self._clients)} client(s)."
                    if connected else "Disconnected.",
        )


# Singleton shared across the lifetime of the app process
_manager = LivePriceManager()


# ─── WebSocket endpoint ───────────────────────────────────────────────────────

@router.websocket("/live-price/stream")
async def ws_live_price_stream(websocket: WebSocket):
    """
    Connect here to receive live ticks as JSON.

    Optionally send a subscribe command after connecting:
        {
          "jwt_token": "...", "api_key": "...",
          "client_code": "...", "feed_token": "...",
          "tokens": ["10626"], "exchange_type": 1, "mode": 1
        }
    """
    await _manager.add_client(websocket)
    try:
        while True:
            raw = await websocket.receive_text()

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json({"error": "Invalid JSON"})
                continue

            required = {"jwt_token", "api_key", "client_code", "feed_token", "tokens"}
            if required.issubset(msg.keys()):
                try:
                    await _manager.ensure_connected(
                        client_code=msg["client_code"],
                        feed_token=msg["feed_token"],
                        api_key=msg["api_key"],
                    )
                    await _manager.subscribe(
                        tokens=msg["tokens"],
                        exchange_type=msg.get("exchange_type", 1),
                        mode=msg.get("mode", 1),
                    )
                    await websocket.send_json({
                        "status": "subscribed",
                        "tokens": msg["tokens"],
                    })
                except Exception as exc:
                    await websocket.send_json({"error": str(exc)})
            else:
                await websocket.send_json({
                    "error": f"Missing keys: {required - msg.keys()}"
                })

    except WebSocketDisconnect:
        _manager.remove_client(websocket)


# ─── REST endpoints ───────────────────────────────────────────────────────────

@router.post("/live-price/subscribe")
async def subscribe_live_price(request: LivePriceRequest):
    """
    Connect to AngelOne and subscribe tokens for live streaming.
    Ticks will be pushed to all connected /api/live-price/stream clients.
    """
    try:
        await _manager.ensure_connected(
            client_code=request.client_code,
            feed_token=request.feed_token,
            api_key=request.api_key,
        )
        await _manager.subscribe(
            tokens=request.tokens,
            exchange_type=request.exchange_type,
            mode=request.mode,
        )
        return success_response(
            message="Subscribed successfully",
            data={
                "tokens":        request.tokens,
                "exchange_type": request.exchange_type,
                "mode":          request.mode,
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        return error_response("Failed to subscribe", str(e))


@router.post("/live-price/unsubscribe")
async def unsubscribe_live_price(request: UnsubscribeRequest):
    """Unsubscribe tokens from live feed."""
    try:
        await _manager.unsubscribe(
            tokens=request.tokens,
            exchange_type=request.exchange_type,
            mode=request.mode,
        )
        return success_response(
            message="Unsubscribed successfully",
            data={"tokens": request.tokens},
        )
    except Exception as e:
        return error_response("Failed to unsubscribe", str(e))


@router.get("/live-price/status")
def get_live_price_status():
    """Return current connection state and subscription list."""
    try:
        status_data = _manager.get_status()
        return success_response(
            message=status_data.message,
            data=status_data.model_dump(),
        )
    except Exception as e:
        return error_response("Failed to fetch status", str(e))


@router.delete("/live-price/disconnect")
async def disconnect_live_price():
    """Disconnect the AngelOne upstream WebSocket."""
    try:
        await _manager.disconnect()
        return success_response(message="Disconnected from AngelOne WebSocket")
    except Exception as e:
        return error_response("Failed to disconnect", str(e))



from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Optional
from app.services.live_price_service import (
    get_price_by_ticker,
    get_prices_by_tickers
)

router = APIRouter()

# ============================================
# Request Schemas
# ============================================
class SingleTickerRequest(BaseModel):
    ticker: str              # "RELIANCE"
    exchange: Optional[str] = None  # "NSE" optional

class MultipleTickerRequest(BaseModel):
    tickers: List[str]       # ["RELIANCE", "INFY"]
    exchange: Optional[str] = None

# ============================================
# ROUTE 1: Single Ticker
# ============================================
@router.post("/live-price/by-ticker")
def get_single_ticker_price(request: SingleTickerRequest):
    """
    Ek ticker ki live price lo

    POST /api/live-price/by-ticker
    Body:
    {
        "ticker": "RELIANCE",
        "exchange": "NSE"
    }
    """
    result = get_price_by_ticker(
        request.ticker,
        request.exchange
    )

    if not result["success"]:
        return {
            "status": False,
            "message": result["error"]
        }

    return {
        "status":  True,
        "message": "Success",
        "data":    result
    }

# ============================================
# ROUTE 2: Multiple Tickers
# ============================================
@router.post("/live-price/by-tickers")
def get_multiple_ticker_prices(request: MultipleTickerRequest):
    """
    Multiple tickers ki prices

    POST /api/live-price/by-tickers
    Body:
    {
        "tickers": ["RELIANCE", "INFY", "TCS"],
        "exchange": "NSE"
    }
    """
    results = get_prices_by_tickers(
        request.tickers,
        request.exchange
    )

    return {
        "status":  True,
        "message": "Success",
        "data":    results
    }