# app/services/cache_service.py

import os
import asyncio
import time
import pandas as pd
from typing import Set, Dict, Optional, Any
from dotenv import load_dotenv

load_dotenv()


class CacheService:
    def __init__(self):
        self.available_symbols: Set[str] = set()
        self.data_cache: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

        # Configuration from environment
        self.local_path    = os.getenv("LOCAL_DATA_PATH", "data/stocks")
        self.enabled       = os.getenv("ENABLE_LOCAL_CACHE", "true").lower() == "true"
        self.max_cache_size = int(os.getenv("MAX_CACHE_SIZE", "100"))
        self.cache_ttl     = int(os.getenv("CACHE_TTL", "3600"))

        # Create local directory if it doesn't exist
        if self.enabled:
            os.makedirs(self.local_path, exist_ok=True)
            self._load_existing_symbols()

    def _load_existing_symbols(self):
        """Load all available symbols from local directory at startup"""
        try:
            if os.path.exists(self.local_path):
                for file in os.listdir(self.local_path):
                    if file.endswith(".csv"):
                        symbol = file.replace(".csv", "").upper()
                        self.available_symbols.add(symbol)
            print(f"Cache: Loaded {len(self.available_symbols)} symbols from local storage")
        except Exception as e:
            print(f"Cache: Error loading symbols: {e}")

    def is_available(self, symbol: str) -> bool:
        """Check if symbol exists locally — O(1) operation"""
        if not self.enabled:
            return False
        return symbol.upper() in self.available_symbols

    async def get(self, symbol: str) -> Optional[pd.DataFrame]:
        """Get symbol data from local cache"""
        if not self.enabled:
            return None

        symbol = symbol.upper()

        # Check in-memory cache first
        async with self._lock:
            if symbol in self.data_cache:
                entry = self.data_cache[symbol]
                if time.time() - entry["timestamp"] < self.cache_ttl:
                    print(f"Cache: HIT (memory) for {symbol}")
                    return entry["data"].copy()
                else:
                    del self.data_cache[symbol]

        # Read from file using asyncio to avoid blocking
        try:
            local_file = os.path.join(self.local_path, f"{symbol}.csv")
            if os.path.exists(local_file):
                # Run pandas read in thread pool — avoids blocking event loop
                loop = asyncio.get_event_loop()
                df = await loop.run_in_executor(
                    None, pd.read_csv, local_file
                )

                async with self._lock:
                    if len(self.data_cache) >= self.max_cache_size:
                        oldest = min(
                            self.data_cache.keys(),
                            key=lambda k: self.data_cache[k]["timestamp"]
                        )
                        del self.data_cache[oldest]

                    self.data_cache[symbol] = {
                        "data":      df,
                        "timestamp": time.time()
                    }

                print(f"Cache: HIT (disk) for {symbol}")
                return df

        except Exception as e:
            print(f"Cache: Error reading {symbol}: {e}")
            await self.remove_symbol(symbol)

        print(f"Cache: MISS for {symbol}")
        return None

    async def set(self, symbol: str, data: pd.DataFrame):
        """Save symbol data to local cache"""
        if not self.enabled or data is None or data.empty:
            return

        symbol = symbol.upper()

        try:
            local_file = os.path.join(self.local_path, f"{symbol}.csv")

            # Run pandas to_csv in thread pool — avoids blocking event loop
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: data.to_csv(local_file, index=False)
            )

            self.available_symbols.add(symbol)

            async with self._lock:
                if len(self.data_cache) >= self.max_cache_size:
                    oldest = min(
                        self.data_cache.keys(),
                        key=lambda k: self.data_cache[k]["timestamp"]
                    )
                    del self.data_cache[oldest]

                self.data_cache[symbol] = {
                    "data":      data,
                    "timestamp": time.time()
                }

            print(f"Cache: Saved {symbol} to local storage")

        except Exception as e:
            print(f"Cache: Error saving {symbol}: {e}")

    async def remove_symbol(self, symbol: str):
        """Remove symbol from cache"""
        symbol = symbol.upper()
        async with self._lock:
            self.available_symbols.discard(symbol)
            if symbol in self.data_cache:
                del self.data_cache[symbol]

    async def clear_memory_cache(self):
        """Clear in-memory cache only"""
        async with self._lock:
            self.data_cache.clear()
        print("Cache: Memory cache cleared")

    def get_stats(self) -> dict:
        """Get cache statistics"""
        return {
            "enabled":          self.enabled,
            "total_symbols":    len(self.available_symbols),
            "cached_in_memory": len(self.data_cache),
            "local_path":       self.local_path,
            "max_cache_size":   self.max_cache_size,
            "cache_ttl":        self.cache_ttl
        }


# Global instance
cache_service = CacheService()