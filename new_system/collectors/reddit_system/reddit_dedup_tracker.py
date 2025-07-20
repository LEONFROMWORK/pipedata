"""
Reddit 독립 중복 추적 시스템
Reddit 전용 중복 수집 방지 시스템으로 다른 시스템과 완전히 분리
"""
import sqlite3
import json
import logging
from datetime import datetime
from typing import Set, List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger('reddit_system.dedup_tracker')

class RedditDuplicationTracker:
    """Reddit 전용 중복 수집 방지 시스템"""
    
    def __init__(self, db_path: Path):
        """
        Initialize Reddit deduplication tracker
        
        Args:
            db_path: Path to Reddit deduplication database
        """
        self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self._init_database()
        logger.info(f"Reddit DuplicationTracker initialized: {self.db_path}")
    
    def _init_database(self) -> None:
        """Initialize Reddit deduplication database"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            # Reddit 게시물 추적 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS reddit_submissions (
                    submission_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    subreddit TEXT NOT NULL,
                    collected_at REAL NOT NULL,
                    quality_score REAL DEFAULT 0.0,
                    metadata TEXT
                )
            ''')
            
            # Reddit 답변 추적 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS reddit_answers (
                    answer_id TEXT PRIMARY KEY,
                    submission_id TEXT NOT NULL,
                    author TEXT,
                    collected_at REAL NOT NULL,
                    quality_score REAL DEFAULT 0.0,
                    metadata TEXT,
                    FOREIGN KEY (submission_id) REFERENCES reddit_submissions(submission_id)
                )
            ''')
            
            # Reddit 수집 통계 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS reddit_collection_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collection_date TEXT NOT NULL,
                    submissions_collected INTEGER NOT NULL,
                    answers_collected INTEGER NOT NULL,
                    duplicates_skipped INTEGER NOT NULL,
                    collection_metadata TEXT
                )
            ''')
            
            # 인덱스 생성
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_reddit_submissions_collected_at 
                ON reddit_submissions(collected_at)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_reddit_answers_collected_at 
                ON reddit_answers(collected_at)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_reddit_subreddit 
                ON reddit_submissions(subreddit)
            ''')
            
            conn.commit()
    
    def is_reddit_submission_collected(self, submission_id: str) -> bool:
        """Reddit 게시물이 이미 수집되었는지 확인"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    'SELECT 1 FROM reddit_submissions WHERE submission_id = ?',
                    (submission_id,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Reddit submission check error: {e}")
            return False
    
    def is_reddit_answer_collected(self, answer_id: str) -> bool:
        """Reddit 답변이 이미 수집되었는지 확인"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    'SELECT 1 FROM reddit_answers WHERE answer_id = ?',
                    (answer_id,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Reddit answer check error: {e}")
            return False
    
    def mark_reddit_submission_collected(self, submission_id: str, title: str, subreddit: str, 
                                       quality_score: float = 0.0, 
                                       metadata: Optional[Dict[str, Any]] = None) -> None:
        """Reddit 게시물을 수집됨으로 표시"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO reddit_submissions 
                    (submission_id, title, subreddit, collected_at, quality_score, metadata) 
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    submission_id,
                    title,
                    subreddit,
                    datetime.now().timestamp(),
                    quality_score,
                    json.dumps(metadata) if metadata else None
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Reddit submission marking error: {e}")
    
    def mark_reddit_answer_collected(self, answer_id: str, submission_id: str, 
                                   author: Optional[str] = None,
                                   quality_score: float = 0.0, 
                                   metadata: Optional[Dict[str, Any]] = None) -> None:
        """Reddit 답변을 수집됨으로 표시"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO reddit_answers 
                    (answer_id, submission_id, author, collected_at, quality_score, metadata) 
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    answer_id,
                    submission_id,
                    author,
                    datetime.now().timestamp(),
                    quality_score,
                    json.dumps(metadata) if metadata else None
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Reddit answer marking error: {e}")
    
    def get_reddit_stats(self, period_days: int = 30) -> Dict[str, Any]:
        """Reddit 수집 통계 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cutoff_time = datetime.now().timestamp() - (period_days * 24 * 3600)
                
                # 게시물 통계
                cursor = conn.execute('''
                    SELECT COUNT(*), AVG(quality_score), MIN(collected_at), MAX(collected_at)
                    FROM reddit_submissions 
                    WHERE collected_at > ?
                ''', (cutoff_time,))
                
                submission_stats = cursor.fetchone()
                
                # 답변 통계
                cursor = conn.execute('''
                    SELECT COUNT(*), AVG(quality_score), MIN(collected_at), MAX(collected_at)
                    FROM reddit_answers 
                    WHERE collected_at > ?
                ''', (cutoff_time,))
                
                answer_stats = cursor.fetchone()
                
                # 서브레딧별 통계
                cursor = conn.execute('''
                    SELECT subreddit, COUNT(*) as count
                    FROM reddit_submissions 
                    WHERE collected_at > ?
                    GROUP BY subreddit
                    ORDER BY count DESC
                    LIMIT 10
                ''', (cutoff_time,))
                
                subreddit_stats = cursor.fetchall()
                
                return {
                    'period_days': period_days,
                    'submissions': {
                        'total_collected': submission_stats[0] or 0,
                        'avg_quality_score': submission_stats[1] or 0.0,
                        'first_collected': datetime.fromtimestamp(submission_stats[2]).isoformat() if submission_stats[2] else None,
                        'last_collected': datetime.fromtimestamp(submission_stats[3]).isoformat() if submission_stats[3] else None
                    },
                    'answers': {
                        'total_collected': answer_stats[0] or 0,
                        'avg_quality_score': answer_stats[1] or 0.0,
                        'first_collected': datetime.fromtimestamp(answer_stats[2]).isoformat() if answer_stats[2] else None,
                        'last_collected': datetime.fromtimestamp(answer_stats[3]).isoformat() if answer_stats[3] else None
                    },
                    'subreddit_breakdown': [
                        {'subreddit': sub, 'count': count} for sub, count in subreddit_stats
                    ]
                }
        except Exception as e:
            logger.error(f"Reddit stats error: {e}")
            return {'error': str(e)}
    
    def cleanup_old_entries(self, days_to_keep: int = 90) -> int:
        """오래된 Reddit 추적 항목 정리"""
        try:
            cutoff_time = datetime.now().timestamp() - (days_to_keep * 24 * 3600)
            
            with sqlite3.connect(self.db_path) as conn:
                # 답변 먼저 삭제 (외래키 제약 조건)
                cursor = conn.execute(
                    'DELETE FROM reddit_answers WHERE collected_at < ?',
                    (cutoff_time,)
                )
                deleted_answers = cursor.rowcount
                
                # 게시물 삭제
                cursor = conn.execute(
                    'DELETE FROM reddit_submissions WHERE collected_at < ?',
                    (cutoff_time,)
                )
                deleted_submissions = cursor.rowcount
                
                conn.commit()
                
                total_deleted = deleted_answers + deleted_submissions
                logger.info(f"Reddit cleanup: {total_deleted} entries removed")
                return total_deleted
                
        except Exception as e:
            logger.error(f"Reddit cleanup error: {e}")
            return 0
    
    def reset_reddit_tracking(self) -> None:
        """Reddit 추적 데이터 초기화"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('DELETE FROM reddit_answers')
                conn.execute('DELETE FROM reddit_submissions')
                conn.execute('DELETE FROM reddit_collection_stats')
                conn.commit()
                logger.info("Reddit tracking data reset")
        except Exception as e:
            logger.error(f"Reddit reset error: {e}")

# 글로벌 Reddit 추적기 인스턴스
_reddit_tracker = None

def get_reddit_tracker(db_path: Optional[Path] = None) -> RedditDuplicationTracker:
    """Reddit 추적기 인스턴스 반환"""
    global _reddit_tracker
    if _reddit_tracker is None:
        if db_path is None:
            db_path = Path('/Users/kevin/bigdata/data/reddit/reddit_dedup.db')
        _reddit_tracker = RedditDuplicationTracker(db_path)
    return _reddit_tracker