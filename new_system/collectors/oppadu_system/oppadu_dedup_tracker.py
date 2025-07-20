"""
Oppadu 독립 중복 추적 시스템
Oppadu 전용 중복 수집 방지 시스템으로 다른 시스템과 완전히 분리
"""
import sqlite3
import json
import logging
from datetime import datetime
from typing import Set, List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger('oppadu_system.dedup_tracker')

class OppaduDuplicationTracker:
    """Oppadu 전용 중복 수집 방지 시스템"""
    
    def __init__(self, db_path: Path):
        """
        Initialize Oppadu deduplication tracker
        
        Args:
            db_path: Path to Oppadu deduplication database
        """
        self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self._init_database()
        logger.info(f"Oppadu DuplicationTracker initialized: {self.db_path}")
    
    def _init_database(self) -> None:
        """Initialize Oppadu deduplication database"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            # Oppadu 게시물 추적 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS oppadu_posts (
                    post_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    collected_at REAL NOT NULL,
                    quality_score REAL DEFAULT 0.0,
                    view_count INTEGER DEFAULT 0,
                    comment_count INTEGER DEFAULT 0,
                    has_answer BOOLEAN DEFAULT FALSE,
                    metadata TEXT
                )
            ''')
            
            # Oppadu 답변 추적 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS oppadu_answers (
                    answer_id TEXT PRIMARY KEY,
                    post_id TEXT NOT NULL,
                    author TEXT,
                    is_selected BOOLEAN DEFAULT FALSE,
                    collected_at REAL NOT NULL,
                    quality_score REAL DEFAULT 0.0,
                    metadata TEXT,
                    FOREIGN KEY (post_id) REFERENCES oppadu_posts(post_id)
                )
            ''')
            
            # Oppadu 수집 통계 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS oppadu_collection_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collection_date TEXT NOT NULL,
                    posts_collected INTEGER NOT NULL,
                    answers_collected INTEGER NOT NULL,
                    duplicates_skipped INTEGER NOT NULL,
                    collection_metadata TEXT
                )
            ''')
            
            # 인덱스 생성
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_oppadu_posts_collected_at 
                ON oppadu_posts(collected_at)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_oppadu_answers_collected_at 
                ON oppadu_answers(collected_at)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_oppadu_url 
                ON oppadu_posts(url)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_oppadu_selected_answers 
                ON oppadu_answers(is_selected)
            ''')
            
            conn.commit()
    
    def is_oppadu_post_collected(self, post_id: str) -> bool:
        """Oppadu 게시물이 이미 수집되었는지 확인"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    'SELECT 1 FROM oppadu_posts WHERE post_id = ?',
                    (post_id,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Oppadu post check error: {e}")
            return False
    
    def is_oppadu_answer_collected(self, answer_id: str) -> bool:
        """Oppadu 답변이 이미 수집되었는지 확인"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    'SELECT 1 FROM oppadu_answers WHERE answer_id = ?',
                    (answer_id,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Oppadu answer check error: {e}")
            return False
    
    def mark_oppadu_post_collected(self, post_id: str, title: str, url: str,
                                 view_count: int = 0,
                                 comment_count: int = 0,
                                 has_answer: bool = False,
                                 quality_score: float = 0.0, 
                                 metadata: Optional[Dict[str, Any]] = None) -> None:
        """Oppadu 게시물을 수집됨으로 표시"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO oppadu_posts 
                    (post_id, title, url, collected_at, quality_score, view_count, comment_count, has_answer, metadata) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    post_id,
                    title,
                    url,
                    datetime.now().timestamp(),
                    quality_score,
                    view_count,
                    comment_count,
                    has_answer,
                    json.dumps(metadata) if metadata else None
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Oppadu post marking error: {e}")
    
    def mark_oppadu_answer_collected(self, answer_id: str, post_id: str, 
                                   author: Optional[str] = None,
                                   is_selected: bool = False,
                                   quality_score: float = 0.0, 
                                   metadata: Optional[Dict[str, Any]] = None) -> None:
        """Oppadu 답변을 수집됨으로 표시"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO oppadu_answers 
                    (answer_id, post_id, author, is_selected, collected_at, quality_score, metadata) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    answer_id,
                    post_id,
                    author,
                    is_selected,
                    datetime.now().timestamp(),
                    quality_score,
                    json.dumps(metadata) if metadata else None
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Oppadu answer marking error: {e}")
    
    def get_oppadu_stats(self, period_days: int = 30) -> Dict[str, Any]:
        """Oppadu 수집 통계 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cutoff_time = datetime.now().timestamp() - (period_days * 24 * 3600)
                
                # 게시물 통계
                cursor = conn.execute('''
                    SELECT COUNT(*), AVG(quality_score), AVG(view_count), AVG(comment_count),
                           COUNT(CASE WHEN has_answer THEN 1 END),
                           MIN(collected_at), MAX(collected_at)
                    FROM oppadu_posts 
                    WHERE collected_at > ?
                ''', (cutoff_time,))
                
                post_stats = cursor.fetchone()
                
                # 답변 통계
                cursor = conn.execute('''
                    SELECT COUNT(*), AVG(quality_score), COUNT(CASE WHEN is_selected THEN 1 END),
                           MIN(collected_at), MAX(collected_at)
                    FROM oppadu_answers 
                    WHERE collected_at > ?
                ''', (cutoff_time,))
                
                answer_stats = cursor.fetchone()
                
                # 작성자별 통계
                cursor = conn.execute('''
                    SELECT author, COUNT(*) as count
                    FROM oppadu_answers 
                    WHERE collected_at > ? AND author IS NOT NULL
                    GROUP BY author
                    ORDER BY count DESC
                    LIMIT 10
                ''', (cutoff_time,))
                
                author_stats = cursor.fetchall()
                
                return {
                    'period_days': period_days,
                    'posts': {
                        'total_collected': post_stats[0] or 0,
                        'avg_quality_score': post_stats[1] or 0.0,
                        'avg_view_count': post_stats[2] or 0,
                        'avg_comment_count': post_stats[3] or 0,
                        'posts_with_answers': post_stats[4] or 0,
                        'first_collected': datetime.fromtimestamp(post_stats[5]).isoformat() if post_stats[5] else None,
                        'last_collected': datetime.fromtimestamp(post_stats[6]).isoformat() if post_stats[6] else None
                    },
                    'answers': {
                        'total_collected': answer_stats[0] or 0,
                        'avg_quality_score': answer_stats[1] or 0.0,
                        'selected_count': answer_stats[2] or 0,
                        'first_collected': datetime.fromtimestamp(answer_stats[3]).isoformat() if answer_stats[3] else None,
                        'last_collected': datetime.fromtimestamp(answer_stats[4]).isoformat() if answer_stats[4] else None
                    },
                    'author_breakdown': [
                        {'author': author, 'count': count} for author, count in author_stats
                    ]
                }
        except Exception as e:
            logger.error(f"Oppadu stats error: {e}")
            return {'error': str(e)}
    
    def cleanup_old_entries(self, days_to_keep: int = 90) -> int:
        """오래된 Oppadu 추적 항목 정리"""
        try:
            cutoff_time = datetime.now().timestamp() - (days_to_keep * 24 * 3600)
            
            with sqlite3.connect(self.db_path) as conn:
                # 답변 먼저 삭제 (외래키 제약 조건)
                cursor = conn.execute(
                    'DELETE FROM oppadu_answers WHERE collected_at < ?',
                    (cutoff_time,)
                )
                deleted_answers = cursor.rowcount
                
                # 게시물 삭제
                cursor = conn.execute(
                    'DELETE FROM oppadu_posts WHERE collected_at < ?',
                    (cutoff_time,)
                )
                deleted_posts = cursor.rowcount
                
                conn.commit()
                
                total_deleted = deleted_answers + deleted_posts
                logger.info(f"Oppadu cleanup: {total_deleted} entries removed")
                return total_deleted
                
        except Exception as e:
            logger.error(f"Oppadu cleanup error: {e}")
            return 0
    
    def reset_oppadu_tracking(self) -> None:
        """Oppadu 추적 데이터 초기화"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('DELETE FROM oppadu_answers')
                conn.execute('DELETE FROM oppadu_posts')
                conn.execute('DELETE FROM oppadu_collection_stats')
                conn.commit()
                logger.info("Oppadu tracking data reset")
        except Exception as e:
            logger.error(f"Oppadu reset error: {e}")

# 글로벌 Oppadu 추적기 인스턴스
_oppadu_tracker = None

def get_oppadu_tracker(db_path: Optional[Path] = None) -> OppaduDuplicationTracker:
    """Oppadu 추적기 인스턴스 반환"""
    global _oppadu_tracker
    if _oppadu_tracker is None:
        if db_path is None:
            db_path = Path('/Users/kevin/bigdata/data/oppadu/oppadu_dedup.db')
        _oppadu_tracker = OppaduDuplicationTracker(db_path)
    return _oppadu_tracker