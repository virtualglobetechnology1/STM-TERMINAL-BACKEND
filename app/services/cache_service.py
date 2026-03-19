import os
import pandas as pd
from typing import Set, Dict, Optional, Any
from threading import Lock
import time
from dotenv import load_dotenv

load_dotenv()

class CacheService:
    def __init__(self):
        self.available_symbols: Set[str] = set()
        self.data_cache: Dict[str, Dict[str, Any]] = {}  # symbol -> {data: df, timestamp: time}
        self.cache_lock = Lock()
        
        # Configuration from environment
        self.local_path = os.getenv("LOCAL_DATA_PATH", "data/stocks")
        self.enabled = os.getenv("ENABLE_LOCAL_CACHE", "true").lower() == "true"
        self.max_cache_size = int(os.getenv("MAX_CACHE_SIZE", "100"))
        self.cache_ttl = int(os.getenv("CACHE_TTL", "3600"))  # 1 hour in seconds
        
        # Create local directory if it doesn't exist
        if self.enabled:
            os.makedirs(self.local_path, exist_ok=True)
            self._load_existing_symbols()
    
    def _load_existing_symbols(self):
        """Load all available symbols from local directory at startup"""
        try:
            if os.path.exists(self.local_path):
                for file in os.listdir(self.local_path):
                    if file.endswith('.csv'):
                        symbol = file.replace('.csv', '').upper()
                        self.available_symbols.add(symbol)
            
            print(f"✅ Cache: Loaded {len(self.available_symbols)} symbols from local storage")
        except Exception as e:
            print(f"❌ Cache: Error loading symbols: {e}")
    
    def is_available(self, symbol: str) -> bool:
        """Check if symbol exists locally (O(1) operation)"""
        if not self.enabled:
            return False
        return symbol.upper() in self.available_symbols
    
    def get(self, symbol: str) -> Optional[pd.DataFrame]:
        """Get symbol data from local cache"""
        if not self.enabled:
            return None
        
        symbol = symbol.upper()
        
        # Check in-memory cache first (FASTEST)
        with self.cache_lock:
            if symbol in self.data_cache:
                cache_entry = self.data_cache[symbol]
                # Check if cache is still valid
                if time.time() - cache_entry['timestamp'] < self.cache_ttl:
                    print(f"✅ Cache: HIT (memory) for {symbol}")
                    return cache_entry['data'].copy()
                else:
                    # Remove expired cache
                    del self.data_cache[symbol]
        
        # Read from file (FAST)
        try:
            local_file = os.path.join(self.local_path, f"{symbol}.csv")
            if os.path.exists(local_file):
                df = pd.read_csv(local_file)
                
                # Add to in-memory cache
                with self.cache_lock:
                    # Manage cache size (LRU-like)
                    if len(self.data_cache) >= self.max_cache_size:
                        # Remove oldest entry
                        oldest = min(self.data_cache.keys(), 
                                   key=lambda k: self.data_cache[k]['timestamp'])
                        del self.data_cache[oldest]
                    
                    self.data_cache[symbol] = {
                        'data': df,
                        'timestamp': time.time()
                    }
                
                print(f"✅ Cache: HIT (disk) for {symbol}")
                return df
        except Exception as e:
            print(f"❌ Cache: Error reading {symbol}: {e}")
            # Remove from available symbols if file is corrupted
            self.remove_symbol(symbol)
        
        print(f"❌ Cache: MISS for {symbol}")
        return None
    
    def set(self, symbol: str, data: pd.DataFrame):
        """Save symbol data to local cache"""
        if not self.enabled or data is None or data.empty:
            return
        
        symbol = symbol.upper()
        
        try:
            # Save to file
            local_file = os.path.join(self.local_path, f"{symbol}.csv")
            data.to_csv(local_file, index=False)
            
            # Add to available symbols
            self.available_symbols.add(symbol)
            
            # Update in-memory cache
            with self.cache_lock:
                # Manage cache size
                if len(self.data_cache) >= self.max_cache_size:
                    oldest = min(self.data_cache.keys(), 
                               lambda k: self.data_cache[k]['timestamp'])
                    del self.data_cache[oldest]
                
                self.data_cache[symbol] = {
                    'data': data,
                    'timestamp': time.time()
                }
            
            print(f"✅ Cache: Saved {symbol} to local storage")
        except Exception as e:
            print(f"❌ Cache: Error saving {symbol}: {e}")
    
    def remove_symbol(self, symbol: str):
        """Remove symbol from cache (if file is corrupted)"""
        symbol = symbol.upper()
        with self.cache_lock:
            self.available_symbols.discard(symbol)
            if symbol in self.data_cache:
                del self.data_cache[symbol]
    
    def clear_memory_cache(self):
        """Clear in-memory cache only"""
        with self.cache_lock:
            self.data_cache.clear()
        print("✅ Cache: Memory cache cleared")
    
    def get_stats(self) -> dict:
        """Get cache statistics"""
        return {
            'enabled': self.enabled,
            'total_symbols': len(self.available_symbols),
            'cached_in_memory': len(self.data_cache),
            'local_path': self.local_path,
            'max_cache_size': self.max_cache_size,
            'cache_ttl': self.cache_ttl
        }

# Create global cache instance
cache_service = CacheService()