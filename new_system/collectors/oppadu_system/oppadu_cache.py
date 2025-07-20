"""
Oppadu 독립 캐시 시스템
Oppadu 전용 SQLite 캐시로 다른 시스템과 완전히 분리
"""
import sqlite3
import json
import time
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Any, Dict
from pathlib import Path
import logging

logger = logging.getLogger('oppadu_system.cache')

class OppaduCache:
    """Oppadu 전용 SQLite 캐시 시스템"""
    
    def __init__(self, db_path: Path, default_ttl: int = 21600):  # 6시간 기본 TTL (크롤링 대응)
        """
        Initialize Oppadu cache
        
        Args:
            db_path: Path to Oppadu cache database
            default_ttl: Default TTL in seconds (6h for web crawling)
        """
        self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self.default_ttl = default_ttl
        self._init_database()
        logger.info(f"Oppadu cache initialized: {self.db_path}")
    
    def _init_database(self) -> None:
        """Initialize Oppadu cache database"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS oppadu_cache (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    source TEXT DEFAULT 'oppadu'
                )
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_oppadu_expires_at 
                ON oppadu_cache(expires_at)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_oppadu_source 
                ON oppadu_cache(source)
            ''')
            
            conn.commit()
    
    def _generate_key(self, url: str, params: Dict[str, Any] = None) -> str:
        """Generate cache key for Oppadu requests"""
        if params is None:
            params = {}
        key_data = f"oppadu:{url}:{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, url: str, params: Dict[str, Any] = None) -> Optional[Any]:
        """Get Oppadu data from cache"""
        if params is None:
            params = {}
        
        key = self._generate_key(url, params)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT value, expires_at FROM oppadu_cache WHERE key = ?',
                (key,)
            )
            row = cursor.fetchone()
            
            if row is None:
                return None
            
            value, expires_at = row
            
            if time.time() > expires_at:
                # Expired, remove from cache
                conn.execute('DELETE FROM oppadu_cache WHERE key = ?', (key,))
                conn.commit()
                return None
            
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                logger.warning(f"Failed to decode cached Oppadu data for key: {key}")
                return None
    
    def set(self, url: str, data: Any, params: Dict[str, Any] = None, ttl: Optional[int] = None) -> None:
        """Set Oppadu data in cache"""
        if params is None:
            params = {}
        
        key = self._generate_key(url, params)
        ttl = ttl or self.default_ttl
        expires_at = time.time() + ttl
        
        try:
            value = json.dumps(data)
        except (TypeError, ValueError) as e:
            logger.warning(f"Failed to serialize Oppadu data for caching: {e}")
            return
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO oppadu_cache 
                (key, value, expires_at, created_at, source) 
                VALUES (?, ?, ?, ?, ?)
            ''', (key, value, expires_at, time.time(), 'oppadu'))
            conn.commit()
    
    def delete(self, url: str, params: Dict[str, Any] = None) -> None:
        """Delete Oppadu data from cache"""
        if params is None:
            params = {}
        
        key = self._generate_key(url, params)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM oppadu_cache WHERE key = ?', (key,))
            conn.commit()
    
    def clear_expired(self) -> int:
        """Clear expired Oppadu cache entries"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('DELETE FROM oppadu_cache WHERE expires_at < ?', (time.time(),))
            conn.commit()
            return cursor.rowcount
    
    def clear_all(self) -> None:
        """Clear all Oppadu cache entries"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM oppadu_cache')
            conn.commit()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get Oppadu cache statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT COUNT(*) FROM oppadu_cache')
            total_entries = cursor.fetchone()[0]
            
            cursor = conn.execute('SELECT COUNT(*) FROM oppadu_cache WHERE expires_at < ?', (time.time(),))
            expired_entries = cursor.fetchone()[0]
            
            return {
                'total_entries': total_entries,
                'expired_entries': expired_entries,
                'valid_entries': total_entries - expired_entries,
                'cache_file': str(self.db_path),
                'source': 'oppadu'
            }

class OppaduWebCache:
    """Oppadu 웹 크롤링 캐시 래퍼"""
    
    def __init__(self, cache: OppaduCache):
        self.cache = cache
    
    def get_cached_page(self, url: str, params: Dict[str, Any] = None) -> Optional[str]:
        """Get cached Oppadu page HTML"""
        return self.cache.get(url, params)
    
    def cache_page(self, url: str, html_content: str, params: Dict[str, Any] = None, ttl: Optional[int] = None) -> None:
        """Cache Oppadu page HTML"""
        self.cache.set(url, html_content, params, ttl)
    
    def invalidate_page(self, url: str, params: Dict[str, Any] = None) -> None:
        """Invalidate Oppadu page cache"""
        self.cache.delete(url, params)