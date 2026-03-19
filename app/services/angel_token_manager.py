"""
AngelOne Token Manager
======================
Handles JWT + feed token generation and auto-refresh.
Tokens expire at midnight — this manager refreshes them automatically.

Usage:
    from app.services.angel_token_manager import token_manager

    jwt   = await token_manager.get_jwt_token()
    feed  = await token_manager.get_feed_token()
    creds = await token_manager.get_credentials()
"""

import asyncio
import logging
import pyotp
from datetime import datetime, date
from SmartApi import SmartConnect
from app.core.config import (
    ANGEL_API_KEY,
    ANGEL_CLIENT_CODE,
)
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)

PIN         = os.getenv("ANGEL_PIN")
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")


class AngelTokenManager:
    """
    Singleton that holds fresh AngelOne tokens.
    Auto-refreshes when tokens expire (midnight daily).
    """

    def __init__(self):
        self._jwt_token:  str | None = None
        self._feed_token: str | None = None
        self._last_refresh: date | None = None
        self._lock = asyncio.Lock()

    def _is_expired(self) -> bool:
        """Tokens expire at midnight — refresh if last refresh was a different day."""
        if self._jwt_token is None or self._last_refresh is None:
            return True
        return self._last_refresh < date.today()

    async def _refresh(self) -> None:
        """Login to AngelOne and get fresh tokens."""
        logger.info("Refreshing AngelOne tokens...")
        try:
            totp  = pyotp.TOTP(TOTP_SECRET).now()
            smart = SmartConnect(ANGEL_API_KEY)
            data  = smart.generateSession(ANGEL_CLIENT_CODE, PIN, totp)

            if not data or not data.get("data"):
                raise Exception(f"Login failed: {data}")

            self._jwt_token  = data["data"]["jwtToken"]
            self._feed_token = smart.getfeedToken()
            self._last_refresh = date.today()

            logger.info("✅ AngelOne tokens refreshed successfully.")

        except Exception as e:
            logger.error("❌ Failed to refresh AngelOne tokens: %s", e)
            raise

    async def get_credentials(self) -> dict:
        """Returns fresh credentials dict. Auto-refreshes if expired."""
        async with self._lock:
            if self._is_expired():
                await self._refresh()
        return {
            "jwt_token":   self._jwt_token,
            "feed_token":  self._feed_token,
            "api_key":     ANGEL_API_KEY,
            "client_code": ANGEL_CLIENT_CODE,
        }

    async def get_jwt_token(self) -> str:
        creds = await self.get_credentials()
        return creds["jwt_token"]

    async def get_feed_token(self) -> str:
        creds = await self.get_credentials()
        return creds["feed_token"]


# Singleton instance
token_manager = AngelTokenManager()