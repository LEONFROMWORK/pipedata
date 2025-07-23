"""
중복 수집 방지 시스템
ID 기반으로 이미 수집된 항목을 추적하여 중복 수집 방지
"""
import sqlite3
import json
import logging
from datetime import datetime
from typing import Set, List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger('pipeline.dedup_tracker')

class DuplicationTracker:
    """중복 수집 방지를 위한 ID 추적 시스템"""
    
    def __init__(self, db_path: Path):
        """
        Initialize deduplication tracker
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self._init_database()
        logger.info(f"DuplicationTracker initialized: {self.db_path}")
    
    def _init_database(self) -> None:
        """Initialize SQLite database with tracking tables"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            # Stack Overflow ID 추적 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stackoverflow_collected (
                    question_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    collected_at REAL NOT NULL,
                    quality_score REAL,
                    metadata TEXT
                )
            ''')
            
            # Reddit ID 추적 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS reddit_collected (
                    submission_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    collected_at REAL NOT NULL,
                    quality_score REAL,
                    metadata TEXT
                )
            ''')
            
            # 오빠두 ID 추적 테이블 (한국 커뮤니티)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS oppadu_collected (
                    post_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    collected_at REAL NOT NULL,
                    quality_score REAL,
                    metadata TEXT
                )
            ''')
            
            # 수집 통계 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS collection_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    collection_date TEXT NOT NULL,
                    items_collected INTEGER NOT NULL,
                    items_skipped INTEGER NOT NULL,
                    collection_metadata TEXT
                )
            ''')
            
            # 인덱스 생성
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_so_collected_at 
                ON stackoverflow_collected(collected_at)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_reddit_collected_at 
                ON reddit_collected(collected_at)
            ''')
            
            conn.commit()
    
    def is_stackoverflow_question_collected(self, question_id: int) -> bool:
        """Stack Overflow 질문이 이미 수집되었는지 확인"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    'SELECT 1 FROM stackoverflow_collected WHERE question_id = ?',
                    (question_id,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking SO question {question_id}: {e}")
            return False
    
    def is_reddit_submission_collected(self, submission_id: str) -> bool:
        """Reddit 서브미션이 이미 수집되었는지 확인"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    'SELECT 1 FROM reddit_collected WHERE submission_id = ?',
                    (submission_id,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking Reddit submission {submission_id}: {e}")
            return False
    
    def mark_stackoverflow_collected(self, question_id: int, title: str, 
                                   quality_score: float = 0.0, 
                                   metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Stack Overflow 질문을 수집됨으로 표시"""
        try:
            metadata_json = json.dumps(metadata or {})
            collected_at = datetime.now().timestamp()
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO stackoverflow_collected 
                    (question_id, title, collected_at, quality_score, metadata)
                    VALUES (?, ?, ?, ?, ?)
                ''', (question_id, title, collected_at, quality_score, metadata_json))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error marking SO question {question_id} as collected: {e}")
            return False
    
    def mark_reddit_collected(self, submission_id: str, title: str, 
                            quality_score: float = 0.0, 
                            metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Reddit 서브미션을 수집됨으로 표시"""
        try:
            metadata_json = json.dumps(metadata or {})
            collected_at = datetime.now().timestamp()
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO reddit_collected 
                    (submission_id, title, collected_at, quality_score, metadata)
                    VALUES (?, ?, ?, ?, ?)
                ''', (submission_id, title, collected_at, quality_score, metadata_json))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error marking Reddit submission {submission_id} as collected: {e}")
            return False
    
    def filter_new_stackoverflow_questions(self, questions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """새로운 Stack Overflow 질문만 필터링"""
        new_questions = []
        skipped_count = 0
        
        for question in questions:
            question_id = question.get('question_id')
            if question_id and not self.is_stackoverflow_question_collected(question_id):
                new_questions.append(question)
            else:
                skipped_count += 1
                if question_id:
                    logger.debug(f"Skipping already collected SO question: {question_id}")
        
        logger.info(f"Filtered SO questions: {len(new_questions)} new, {skipped_count} skipped")
        return new_questions
    
    def filter_new_reddit_submissions(self, submissions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """새로운 Reddit 서브미션만 필터링"""
        new_submissions = []
        skipped_count = 0
        
        for submission in submissions:
            # Reddit submission ID 추출 (다양한 형태 지원)
            submission_id = None
            if 'id' in submission:
                submission_id = submission['id']
            elif 'reddit_id' in submission:
                submission_id = submission['reddit_id']
            elif 'submission_id' in submission:
                submission_id = submission['submission_id']
            
            if submission_id and not self.is_reddit_submission_collected(submission_id):
                new_submissions.append(submission)
            else:
                skipped_count += 1
                if submission_id:
                    logger.debug(f"Skipping already collected Reddit submission: {submission_id}")
        
        logger.info(f"Filtered Reddit submissions: {len(new_submissions)} new, {skipped_count} skipped")
        return new_submissions
    
    def is_oppadu_post_collected(self, post_id: str) -> bool:
        """오빠두 게시글이 이미 수집되었는지 확인"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    'SELECT 1 FROM oppadu_collected WHERE post_id = ?',
                    (post_id,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking Oppadu post {post_id}: {e}")
            return False
    
    def mark_oppadu_collected(self, post_id: str, title: str, 
                            quality_score: float = 0.0, 
                            metadata: Optional[Dict[str, Any]] = None) -> bool:
        """오빠두 게시글을 수집됨으로 표시"""
        try:
            metadata_json = json.dumps(metadata or {})
            collected_at = datetime.now().timestamp()
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO oppadu_collected 
                    (post_id, title, collected_at, quality_score, metadata)
                    VALUES (?, ?, ?, ?, ?)
                ''', (post_id, title, collected_at, quality_score, metadata_json))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error marking Oppadu post {post_id} as collected: {e}")
            return False
    
    def filter_new_oppadu_posts(self, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """새로운 오빠두 게시글만 필터링"""
        new_posts = []
        skipped_count = 0
        
        for post in posts:
            post_id = post.get('post_id') or post.get('id')
            if post_id and not self.is_oppadu_post_collected(post_id):
                new_posts.append(post)
            else:
                skipped_count += 1
                if post_id:
                    logger.debug(f"Skipping already collected Oppadu post: {post_id}")
        
        logger.info(f"Filtered Oppadu posts: {len(new_posts)} new, {skipped_count} skipped")
        return new_posts
    
    def record_collection_stats(self, source: str, items_collected: int, items_skipped: int, 
                               metadata: Optional[Dict[str, Any]] = None) -> bool:
        """수집 통계를 collection_stats 테이블에 기록"""
        try:
            metadata_json = json.dumps(metadata or {})
            collection_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO collection_stats 
                    (source, collection_date, items_collected, items_skipped, collection_metadata)
                    VALUES (?, ?, ?, ?, ?)
                ''', (source, collection_date, items_collected, items_skipped, metadata_json))
                
                conn.commit()
                logger.info(f"Recorded collection stats for {source}: {items_collected} collected, {items_skipped} skipped")
                return True
                
        except Exception as e:
            logger.error(f"Error recording collection stats: {e}")
            return False
    
    def get_collection_stats(self, days: int = 30) -> Dict[str, Any]:
        """최근 N일간 수집 통계 조회"""
        try:
            cutoff_time = (datetime.now().timestamp()) - (days * 24 * 60 * 60)
            
            with sqlite3.connect(self.db_path) as conn:
                # Stack Overflow 통계
                cursor = conn.execute('''
                    SELECT COUNT(*), AVG(quality_score), MIN(collected_at), MAX(collected_at)
                    FROM stackoverflow_collected 
                    WHERE collected_at > ?
                ''', (cutoff_time,))
                so_stats = cursor.fetchone()
                
                # Reddit 통계
                cursor = conn.execute('''
                    SELECT COUNT(*), AVG(quality_score), MIN(collected_at), MAX(collected_at)
                    FROM reddit_collected 
                    WHERE collected_at > ?
                ''', (cutoff_time,))
                reddit_stats = cursor.fetchone()
                
                return {
                    'period_days': days,
                    'stackoverflow': {
                        'total_collected': so_stats[0] or 0,
                        'avg_quality_score': so_stats[1] or 0.0,
                        'first_collected': datetime.fromtimestamp(so_stats[2]).isoformat() if so_stats[2] else None,
                        'last_collected': datetime.fromtimestamp(so_stats[3]).isoformat() if so_stats[3] else None
                    },
                    'reddit': {
                        'total_collected': reddit_stats[0] or 0,
                        'avg_quality_score': reddit_stats[1] or 0.0,
                        'first_collected': datetime.fromtimestamp(reddit_stats[2]).isoformat() if reddit_stats[2] else None,
                        'last_collected': datetime.fromtimestamp(reddit_stats[3]).isoformat() if reddit_stats[3] else None
                    }
                }
        except Exception as e:
            logger.error(f"Error getting collection stats: {e}")
            return {}
    
    def cleanup_old_records(self, days: int = 180) -> int:
        """오래된 추적 레코드 정리 (선택적)"""
        try:
            cutoff_time = (datetime.now().timestamp()) - (days * 24 * 60 * 60)
            
            with sqlite3.connect(self.db_path) as conn:
                # 너무 오래된 레코드 삭제
                cursor = conn.execute(
                    'DELETE FROM stackoverflow_collected WHERE collected_at < ?',
                    (cutoff_time,)
                )
                so_deleted = cursor.rowcount
                
                cursor = conn.execute(
                    'DELETE FROM reddit_collected WHERE collected_at < ?',
                    (cutoff_time,)
                )
                reddit_deleted = cursor.rowcount
                
                conn.commit()
                
                total_deleted = so_deleted + reddit_deleted
                if total_deleted > 0:
                    logger.info(f"Cleaned up {total_deleted} old tracking records (>{days} days)")
                
                return total_deleted
                
        except Exception as e:
            logger.error(f"Error cleaning up old records: {e}")
            return 0
    
    def record_collection_run(self, source: str, items_collected: int, 
                            items_skipped: int, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """수집 실행 기록 저장"""
        try:
            collection_date = datetime.now().date().isoformat()
            metadata_json = json.dumps(metadata or {})
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO collection_stats 
                    (source, collection_date, items_collected, items_skipped, collection_metadata)
                    VALUES (?, ?, ?, ?, ?)
                ''', (source, collection_date, items_collected, items_skipped, metadata_json))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error recording collection run: {e}")
            return False

# 전역 중복 추적기 인스턴스
_global_tracker: Optional[DuplicationTracker] = None

def get_global_tracker() -> DuplicationTracker:
    """전역 중복 추적기 인스턴스 가져오기"""
    global _global_tracker
    if _global_tracker is None:
        from config import Config
        tracker_db_path = Config.DATA_DIR / 'deduplication_tracker.db'
        _global_tracker = DuplicationTracker(tracker_db_path)
    return _global_tracker