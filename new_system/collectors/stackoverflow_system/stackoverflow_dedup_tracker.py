"""
StackOverflow 독립 중복 추적 시스템
StackOverflow 전용 중복 수집 방지 시스템으로 다른 시스템과 완전히 분리
"""
import sqlite3
import json
import logging
from datetime import datetime
from typing import Set, List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger('stackoverflow_system.dedup_tracker')

class StackOverflowDuplicationTracker:
    """StackOverflow 전용 중복 수집 방지 시스템"""
    
    def __init__(self, db_path: Path):
        """
        Initialize StackOverflow deduplication tracker
        
        Args:
            db_path: Path to StackOverflow deduplication database
        """
        self.db_path = Path(db_path) if isinstance(db_path, str) else db_path
        self._init_database()
        logger.info(f"StackOverflow DuplicationTracker initialized: {self.db_path}")
    
    def _init_database(self) -> None:
        """Initialize StackOverflow deduplication database"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            # StackOverflow 질문 추적 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stackoverflow_questions (
                    question_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    tags TEXT,
                    collected_at REAL NOT NULL,
                    quality_score REAL DEFAULT 0.0,
                    view_count INTEGER DEFAULT 0,
                    answer_count INTEGER DEFAULT 0,
                    metadata TEXT
                )
            ''')
            
            # StackOverflow 답변 추적 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stackoverflow_answers (
                    answer_id INTEGER PRIMARY KEY,
                    question_id INTEGER NOT NULL,
                    author TEXT,
                    is_accepted BOOLEAN DEFAULT FALSE,
                    score INTEGER DEFAULT 0,
                    collected_at REAL NOT NULL,
                    quality_score REAL DEFAULT 0.0,
                    metadata TEXT,
                    FOREIGN KEY (question_id) REFERENCES stackoverflow_questions(question_id)
                )
            ''')
            
            # StackOverflow 수집 통계 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stackoverflow_collection_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collection_date TEXT NOT NULL,
                    questions_collected INTEGER NOT NULL,
                    answers_collected INTEGER NOT NULL,
                    duplicates_skipped INTEGER NOT NULL,
                    collection_metadata TEXT
                )
            ''')
            
            # 인덱스 생성
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_so_questions_collected_at 
                ON stackoverflow_questions(collected_at)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_so_answers_collected_at 
                ON stackoverflow_answers(collected_at)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_so_tags 
                ON stackoverflow_questions(tags)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_so_accepted_answers 
                ON stackoverflow_answers(is_accepted)
            ''')
            
            conn.commit()
    
    def is_stackoverflow_question_collected(self, question_id: int) -> bool:
        """StackOverflow 질문이 이미 수집되었는지 확인"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    'SELECT 1 FROM stackoverflow_questions WHERE question_id = ?',
                    (question_id,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"StackOverflow question check error: {e}")
            return False
    
    def is_stackoverflow_answer_collected(self, answer_id: int) -> bool:
        """StackOverflow 답변이 이미 수집되었는지 확인"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    'SELECT 1 FROM stackoverflow_answers WHERE answer_id = ?',
                    (answer_id,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"StackOverflow answer check error: {e}")
            return False
    
    def mark_stackoverflow_question_collected(self, question_id: int, title: str, 
                                            tags: Optional[str] = None,
                                            view_count: int = 0,
                                            answer_count: int = 0,
                                            quality_score: float = 0.0, 
                                            metadata: Optional[Dict[str, Any]] = None) -> None:
        """StackOverflow 질문을 수집됨으로 표시"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO stackoverflow_questions 
                    (question_id, title, tags, collected_at, quality_score, view_count, answer_count, metadata) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    question_id,
                    title,
                    tags,
                    datetime.now().timestamp(),
                    quality_score,
                    view_count,
                    answer_count,
                    json.dumps(metadata) if metadata else None
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"StackOverflow question marking error: {e}")
    
    def mark_stackoverflow_answer_collected(self, answer_id: int, question_id: int, 
                                          author: Optional[str] = None,
                                          is_accepted: bool = False,
                                          score: int = 0,
                                          quality_score: float = 0.0, 
                                          metadata: Optional[Dict[str, Any]] = None) -> None:
        """StackOverflow 답변을 수집됨으로 표시"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO stackoverflow_answers 
                    (answer_id, question_id, author, is_accepted, score, collected_at, quality_score, metadata) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    answer_id,
                    question_id,
                    author,
                    is_accepted,
                    score,
                    datetime.now().timestamp(),
                    quality_score,
                    json.dumps(metadata) if metadata else None
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"StackOverflow answer marking error: {e}")
    
    def get_stackoverflow_stats(self, period_days: int = 30) -> Dict[str, Any]:
        """StackOverflow 수집 통계 조회"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cutoff_time = datetime.now().timestamp() - (period_days * 24 * 3600)
                
                # 질문 통계
                cursor = conn.execute('''
                    SELECT COUNT(*), AVG(quality_score), AVG(view_count), AVG(answer_count),
                           MIN(collected_at), MAX(collected_at)
                    FROM stackoverflow_questions 
                    WHERE collected_at > ?
                ''', (cutoff_time,))
                
                question_stats = cursor.fetchone()
                
                # 답변 통계
                cursor = conn.execute('''
                    SELECT COUNT(*), AVG(quality_score), AVG(score), COUNT(CASE WHEN is_accepted THEN 1 END),
                           MIN(collected_at), MAX(collected_at)
                    FROM stackoverflow_answers 
                    WHERE collected_at > ?
                ''', (cutoff_time,))
                
                answer_stats = cursor.fetchone()
                
                # 태그별 통계
                cursor = conn.execute('''
                    SELECT tags, COUNT(*) as count
                    FROM stackoverflow_questions 
                    WHERE collected_at > ? AND tags IS NOT NULL
                    GROUP BY tags
                    ORDER BY count DESC
                    LIMIT 10
                ''', (cutoff_time,))
                
                tag_stats = cursor.fetchall()
                
                return {
                    'period_days': period_days,
                    'questions': {
                        'total_collected': question_stats[0] or 0,
                        'avg_quality_score': question_stats[1] or 0.0,
                        'avg_view_count': question_stats[2] or 0,
                        'avg_answer_count': question_stats[3] or 0,
                        'first_collected': datetime.fromtimestamp(question_stats[4]).isoformat() if question_stats[4] else None,
                        'last_collected': datetime.fromtimestamp(question_stats[5]).isoformat() if question_stats[5] else None
                    },
                    'answers': {
                        'total_collected': answer_stats[0] or 0,
                        'avg_quality_score': answer_stats[1] or 0.0,
                        'avg_score': answer_stats[2] or 0,
                        'accepted_count': answer_stats[3] or 0,
                        'first_collected': datetime.fromtimestamp(answer_stats[4]).isoformat() if answer_stats[4] else None,
                        'last_collected': datetime.fromtimestamp(answer_stats[5]).isoformat() if answer_stats[5] else None
                    },
                    'tag_breakdown': [
                        {'tags': tags, 'count': count} for tags, count in tag_stats
                    ]
                }
        except Exception as e:
            logger.error(f"StackOverflow stats error: {e}")
            return {'error': str(e)}
    
    def cleanup_old_entries(self, days_to_keep: int = 90) -> int:
        """오래된 StackOverflow 추적 항목 정리"""
        try:
            cutoff_time = datetime.now().timestamp() - (days_to_keep * 24 * 3600)
            
            with sqlite3.connect(self.db_path) as conn:
                # 답변 먼저 삭제 (외래키 제약 조건)
                cursor = conn.execute(
                    'DELETE FROM stackoverflow_answers WHERE collected_at < ?',
                    (cutoff_time,)
                )
                deleted_answers = cursor.rowcount
                
                # 질문 삭제
                cursor = conn.execute(
                    'DELETE FROM stackoverflow_questions WHERE collected_at < ?',
                    (cutoff_time,)
                )
                deleted_questions = cursor.rowcount
                
                conn.commit()
                
                total_deleted = deleted_answers + deleted_questions
                logger.info(f"StackOverflow cleanup: {total_deleted} entries removed")
                return total_deleted
                
        except Exception as e:
            logger.error(f"StackOverflow cleanup error: {e}")
            return 0
    
    def reset_stackoverflow_tracking(self) -> None:
        """StackOverflow 추적 데이터 초기화"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('DELETE FROM stackoverflow_answers')
                conn.execute('DELETE FROM stackoverflow_questions')
                conn.execute('DELETE FROM stackoverflow_collection_stats')
                conn.commit()
                logger.info("StackOverflow tracking data reset")
        except Exception as e:
            logger.error(f"StackOverflow reset error: {e}")

# 글로벌 StackOverflow 추적기 인스턴스
_stackoverflow_tracker = None

def get_stackoverflow_tracker(db_path: Optional[Path] = None) -> StackOverflowDuplicationTracker:
    """StackOverflow 추적기 인스턴스 반환"""
    global _stackoverflow_tracker
    if _stackoverflow_tracker is None:
        if db_path is None:
            db_path = Path('/Users/kevin/bigdata/data/stackoverflow/so_dedup.db')
        _stackoverflow_tracker = StackOverflowDuplicationTracker(db_path)
    return _stackoverflow_tracker