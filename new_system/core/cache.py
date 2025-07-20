"""
Local SQLite-based caching system replacing Redis for local deployment
TRD Section 3.1: API requests cached with 24h TTL
"""
import sqlite3
import json
import time
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Any, Dict
from pathlib import Path
import logging

logger = logging.getLogger('pipeline.cache')

class LocalCache:
    """SQLite-based cache for API responses with TTL support"""
    
    def __init__(self, db_path: Path, default_ttl: int = 86400):
        """
        Initialize SQLite cache
        
        Args:
            db_path: Path to SQLite database file
            default_ttl: Default TTL in seconds (24h for API cache)
        """
        # Ensure db_path is a Path object
        self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self.default_ttl = default_ttl
        self._init_database()
    
    def _init_database(self) -> None:
        """Initialize SQLite database with cache table"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    created_at REAL NOT NULL
                )
            ''')
            
            # Index for efficient cleanup
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_expires_at 
                ON cache(expires_at)
            ''')
            
            conn.commit()
    
    def _generate_key(self, prefix: str, data: Dict[str, Any]) -> str:
        """Generate cache key from request data"""
        # Sort keys for consistent hashing
        sorted_data = json.dumps(data, sort_keys=True)
        hash_obj = hashlib.md5(sorted_data.encode())
        return f"{prefix}:{hash_obj.hexdigest()}"
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    'SELECT value, expires_at FROM cache WHERE key = ?',
                    (key,)
                )
                result = cursor.fetchone()
                
                if result is None:
                    return None
                
                value_str, expires_at = result
                
                # Check if expired
                if time.time() > expires_at:
                    # Clean up expired entry
                    conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                    conn.commit()
                    return None
                
                return json.loads(value_str)
                
        except Exception as e:
            logger.error(f"Cache get error for key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache with TTL"""
        try:
            if ttl is None:
                ttl = self.default_ttl
            
            expires_at = time.time() + ttl
            created_at = time.time()
            value_str = json.dumps(value)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO cache 
                    (key, value, expires_at, created_at)
                    VALUES (?, ?, ?, ?)
                ''', (key, value_str, expires_at, created_at))
                conn.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete key from cache"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('DELETE FROM cache WHERE key = ?', (key,))
                conn.commit()
                return cursor.rowcount > 0
                
        except Exception as e:
            logger.error(f"Cache delete error for key {key}: {e}")
            return False
    
    def cleanup_expired(self) -> int:
        """Remove expired entries and return count"""
        try:
            current_time = time.time()
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    'DELETE FROM cache WHERE expires_at < ?',
                    (current_time,)
                )
                conn.commit()
                return cursor.rowcount
                
        except Exception as e:
            logger.error(f"Cache cleanup error: {e}")
            return 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        try:
            current_time = time.time()
            with sqlite3.connect(self.db_path) as conn:
                # Total entries
                total = conn.execute('SELECT COUNT(*) FROM cache').fetchone()[0]
                
                # Expired entries
                expired = conn.execute(
                    'SELECT COUNT(*) FROM cache WHERE expires_at < ?',
                    (current_time,)
                ).fetchone()[0]
                
                # Size estimation
                size_result = conn.execute('''
                    SELECT SUM(LENGTH(key) + LENGTH(value)) 
                    FROM cache
                ''').fetchone()[0]
                
                return {
                    'total_entries': total,
                    'expired_entries': expired,
                    'valid_entries': total - expired,
                    'estimated_size_bytes': size_result or 0,
                    'db_path': str(self.db_path)
                }
                
        except Exception as e:
            logger.error(f"Cache stats error: {e}")
            return {}

class APICache:
    """Specialized cache for API responses following TRD specs"""
    
    def __init__(self, cache: LocalCache):
        self.cache = cache
    
    def get_stackoverflow_response(self, endpoint: str, params: Dict[str, Any]) -> Optional[Dict]:
        """Get cached Stack Overflow API response"""
        key = self.cache._generate_key('so_api', {
            'endpoint': endpoint,
            'params': params
        })
        return self.cache.get(key)
    
    def cache_stackoverflow_response(self, endpoint: str, params: Dict[str, Any], 
                                   response: Dict) -> bool:
        """Cache Stack Overflow API response for 24h"""
        key = self.cache._generate_key('so_api', {
            'endpoint': endpoint, 
            'params': params
        })
        # TRD specifies 24h TTL for API responses
        return self.cache.set(key, response, ttl=86400)
    
    def get_image_processing_result(self, image_url: str, processing_type: str) -> Optional[Dict]:
        """Get cached image processing result"""
        key = self.cache._generate_key('img_proc', {
            'url': image_url,
            'type': processing_type
        })
        return self.cache.get(key)
    
    def cache_image_processing_result(self, image_url: str, processing_type: str,
                                    result: Dict) -> bool:
        """Cache image processing result for 7 days"""
        key = self.cache._generate_key('img_proc', {
            'url': image_url,
            'type': processing_type
        })
        # Longer TTL for expensive image processing
        return self.cache.set(key, result, ttl=604800)  # 7 days
    
    def get_openrouter_response(self, model: str, messages: list, image_url: str) -> Optional[Dict]:
        """Get cached OpenRouter API response"""
        key = self.cache._generate_key('openrouter', {
            'model': model,
            'messages': messages,
            'image_url': image_url
        })
        return self.cache.get(key)
    
    def cache_openrouter_response(self, model: str, messages: list, 
                                image_url: str, response: Dict) -> bool:
        """Cache OpenRouter API response for 1 week"""
        key = self.cache._generate_key('openrouter', {
            'model': model,
            'messages': messages, 
            'image_url': image_url
        })
        # Cache expensive AI responses for longer
        return self.cache.set(key, response, ttl=604800)  # 7 days