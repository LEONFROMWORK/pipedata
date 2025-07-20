"""
StackOverflow 독립 캐시 시스템
StackOverflow 전용 SQLite 캐시로 다른 시스템과 완전히 분리
"""
import sqlite3
import json
import time
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Any, Dict
from pathlib import Path
import logging

logger = logging.getLogger('stackoverflow_system.cache')

class StackOverflowCache:
    """StackOverflow 전용 SQLite 캐시 시스템"""
    
    def __init__(self, db_path: Path, default_ttl: int = 86400):
        """
        Initialize StackOverflow cache
        
        Args:
            db_path: Path to StackOverflow cache database
            default_ttl: Default TTL in seconds (24h for StackOverflow API cache)
        """
        self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self.default_ttl = default_ttl
        self._init_database()
        logger.info(f"StackOverflow cache initialized: {self.db_path}")
    
    def _init_database(self) -> None:
        """Initialize StackOverflow cache database"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stackoverflow_cache (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    source TEXT DEFAULT 'stackoverflow'
                )
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_so_expires_at 
                ON stackoverflow_cache(expires_at)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_so_source 
                ON stackoverflow_cache(source)
            ''')
            
            conn.commit()
    
    def _generate_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key for StackOverflow requests"""
        key_data = f"stackoverflow:{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Any]:
        """Get StackOverflow data from cache"""
        if params is None:
            params = {}
        
        key = self._generate_key(endpoint, params)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT value, expires_at FROM stackoverflow_cache WHERE key = ?',
                (key,)
            )
            row = cursor.fetchone()
            
            if row is None:
                return None
            
            value, expires_at = row
            
            if time.time() > expires_at:
                # Expired, remove from cache
                conn.execute('DELETE FROM stackoverflow_cache WHERE key = ?', (key,))
                conn.commit()
                return None
            
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                logger.warning(f"Failed to decode cached StackOverflow data for key: {key}")
                return None
    
    def set(self, endpoint: str, data: Any, params: Dict[str, Any] = None, ttl: Optional[int] = None) -> None:
        """Set StackOverflow data in cache"""
        if params is None:
            params = {}
        
        key = self._generate_key(endpoint, params)
        ttl = ttl or self.default_ttl
        expires_at = time.time() + ttl
        
        try:
            value = json.dumps(data)
        except (TypeError, ValueError) as e:
            logger.warning(f"Failed to serialize StackOverflow data for caching: {e}")
            return
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO stackoverflow_cache 
                (key, value, expires_at, created_at, source) 
                VALUES (?, ?, ?, ?, ?)
            ''', (key, value, expires_at, time.time(), 'stackoverflow'))
            conn.commit()
    
    def delete(self, endpoint: str, params: Dict[str, Any] = None) -> None:
        """Delete StackOverflow data from cache"""
        if params is None:
            params = {}
        
        key = self._generate_key(endpoint, params)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM stackoverflow_cache WHERE key = ?', (key,))
            conn.commit()
    
    def clear_expired(self) -> int:
        """Clear expired StackOverflow cache entries"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('DELETE FROM stackoverflow_cache WHERE expires_at < ?', (time.time(),))
            conn.commit()
            return cursor.rowcount
    
    def clear_all(self) -> None:
        """Clear all StackOverflow cache entries"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM stackoverflow_cache')
            conn.commit()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get StackOverflow cache statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT COUNT(*) FROM stackoverflow_cache')
            total_entries = cursor.fetchone()[0]
            
            cursor = conn.execute('SELECT COUNT(*) FROM stackoverflow_cache WHERE expires_at < ?', (time.time(),))
            expired_entries = cursor.fetchone()[0]
            
            return {
                'total_entries': total_entries,
                'expired_entries': expired_entries,
                'valid_entries': total_entries - expired_entries,
                'cache_file': str(self.db_path),
                'source': 'stackoverflow'
            }

class StackOverflowAPICache:
    """StackOverflow API 캐시 래퍼"""
    
    def __init__(self, cache: StackOverflowCache):
        self.cache = cache
    
    def get_cached_response(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Any]:
        """Get cached StackOverflow API response"""
        return self.cache.get(endpoint, params)
    
    def cache_response(self, endpoint: str, response: Any, params: Dict[str, Any] = None, ttl: Optional[int] = None) -> None:
        """Cache StackOverflow API response"""
        self.cache.set(endpoint, response, params, ttl)
    
    def invalidate_cache(self, endpoint: str, params: Dict[str, Any] = None) -> None:
        """Invalidate StackOverflow API cache"""
        self.cache.delete(endpoint, params)