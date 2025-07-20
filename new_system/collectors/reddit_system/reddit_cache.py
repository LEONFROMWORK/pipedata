"""
Reddit 독립 캐시 시스템
Reddit 전용 SQLite 캐시로 다른 시스템과 완전히 분리
"""
import sqlite3
import json
import time
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Any, Dict
from pathlib import Path
import logging

logger = logging.getLogger('reddit_system.cache')

class RedditCache:
    """Reddit 전용 SQLite 캐시 시스템"""
    
    def __init__(self, db_path: Path, default_ttl: int = 86400):
        """
        Initialize Reddit cache
        
        Args:
            db_path: Path to Reddit cache database
            default_ttl: Default TTL in seconds (24h for Reddit API cache)
        """
        self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self.default_ttl = default_ttl
        self._init_database()
        logger.info(f"Reddit cache initialized: {self.db_path}")
    
    def _init_database(self) -> None:
        """Initialize Reddit cache database"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS reddit_cache (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    source TEXT DEFAULT 'reddit'
                )
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_reddit_expires_at 
                ON reddit_cache(expires_at)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_reddit_source 
                ON reddit_cache(source)
            ''')
            
            conn.commit()
    
    def _generate_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key for Reddit requests"""
        key_data = f"reddit:{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Any]:
        """Get Reddit data from cache"""
        if params is None:
            params = {}
        
        key = self._generate_key(endpoint, params)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT value, expires_at FROM reddit_cache WHERE key = ?',
                (key,)
            )
            row = cursor.fetchone()
            
            if row is None:
                return None
            
            value, expires_at = row
            
            if time.time() > expires_at:
                # Expired, remove from cache
                conn.execute('DELETE FROM reddit_cache WHERE key = ?', (key,))
                conn.commit()
                return None
            
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                logger.warning(f"Failed to decode cached Reddit data for key: {key}")
                return None
    
    def set(self, endpoint: str, data: Any, params: Dict[str, Any] = None, ttl: Optional[int] = None) -> None:
        """Set Reddit data in cache"""
        if params is None:
            params = {}
        
        key = self._generate_key(endpoint, params)
        ttl = ttl or self.default_ttl
        expires_at = time.time() + ttl
        
        try:
            value = json.dumps(data)
        except (TypeError, ValueError) as e:
            logger.warning(f"Failed to serialize Reddit data for caching: {e}")
            return
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO reddit_cache 
                (key, value, expires_at, created_at, source) 
                VALUES (?, ?, ?, ?, ?)
            ''', (key, value, expires_at, time.time(), 'reddit'))
            conn.commit()
    
    def delete(self, endpoint: str, params: Dict[str, Any] = None) -> None:
        """Delete Reddit data from cache"""
        if params is None:
            params = {}
        
        key = self._generate_key(endpoint, params)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM reddit_cache WHERE key = ?', (key,))
            conn.commit()
    
    def clear_expired(self) -> int:
        """Clear expired Reddit cache entries"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('DELETE FROM reddit_cache WHERE expires_at < ?', (time.time(),))
            conn.commit()
            return cursor.rowcount
    
    def clear_all(self) -> None:
        """Clear all Reddit cache entries"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM reddit_cache')
            conn.commit()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get Reddit cache statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT COUNT(*) FROM reddit_cache')
            total_entries = cursor.fetchone()[0]
            
            cursor = conn.execute('SELECT COUNT(*) FROM reddit_cache WHERE expires_at < ?', (time.time(),))
            expired_entries = cursor.fetchone()[0]
            
            return {
                'total_entries': total_entries,
                'expired_entries': expired_entries,
                'valid_entries': total_entries - expired_entries,
                'cache_file': str(self.db_path),
                'source': 'reddit'
            }

class RedditAPICache:
    """Reddit API 캐시 래퍼"""
    
    def __init__(self, cache: RedditCache):
        self.cache = cache
    
    def get_cached_response(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Any]:
        """Get cached Reddit API response"""
        return self.cache.get(endpoint, params)
    
    def cache_response(self, endpoint: str, response: Any, params: Dict[str, Any] = None, ttl: Optional[int] = None) -> None:
        """Cache Reddit API response"""
        self.cache.set(endpoint, response, params, ttl)
    
    def invalidate_cache(self, endpoint: str, params: Dict[str, Any] = None) -> None:
        """Invalidate Reddit API cache"""
        self.cache.delete(endpoint, params)