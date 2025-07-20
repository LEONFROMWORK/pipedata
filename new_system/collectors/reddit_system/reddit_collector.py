"""
독립 Reddit 수집기
Reddit 전용 독립 시스템을 사용하는 완전히 분리된 수집기
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import re
import sys
from pathlib import Path

# 상위 디렉토리 경로 추가
sys.path.append(str(Path(__file__).parent.parent.parent))

import praw
from prawcore.exceptions import PrawcoreException
import backoff

# 독립 시스템 import
from .reddit_cache import RedditCache, RedditAPICache
from .reddit_dedup_tracker import get_reddit_tracker
from .reddit_config import get_reddit_config

# 공통 유틸리티 import
from shared.utils import generate_unique_id, calculate_quality_score, extract_code_blocks, clean_text
from shared.data_models import QAEntry, CollectionStats

# 봇 탐지 시스템 import (기존 Ultimate Bot Detector 사용)
from bot_detection.ultimate_bot_detector import UltimateBotDetector

logger = logging.getLogger('reddit_system.collector')

class RedditCollector:
    """독립 Reddit 수집기"""
    
    def __init__(self):
        """Reddit 수집기 초기화"""
        self.config = get_reddit_config()
        self.dedup_tracker = get_reddit_tracker()
        
        # 독립 캐시 시스템 초기화
        reddit_cache = RedditCache(self.config.cache_db_path)
        self.cache = RedditAPICache(reddit_cache)
        
        # 봇 탐지 시스템 초기화
        self.bot_detector = UltimateBotDetector()
        
        # Reddit API 초기화
        self.reddit = None
        self._init_reddit_api()
        
        # 수집 통계
        self.stats = {
            'total_processed': 0,
            'total_collected': 0,
            'total_skipped': 0,
            'bot_responses_filtered': 0,
            'duplicate_submissions': 0,
            'quality_failures': 0,
            'errors': 0
        }
        
        logger.info("독립 Reddit 수집기 초기화 완료")
    
    def _init_reddit_api(self) -> None:
        """Reddit API 초기화"""
        try:
            reddit_config = self.config.get_reddit_praw_config()
            self.reddit = praw.Reddit(**reddit_config)
            logger.info("Reddit API 초기화 완료")
        except Exception as e:
            logger.error(f"Reddit API 초기화 실패: {e}")
            raise
    
    async def collect_excel_qa_data(self, max_items: int = 100) -> List[QAEntry]:
        """Excel Q&A 데이터 수집"""
        logger.info(f"Reddit Excel Q&A 데이터 수집 시작 (최대 {max_items}개)")
        
        collected_data = []
        collection_config = self.config.get_collection_config()
        
        try:
            for subreddit_name in collection_config['subreddits']:
                if len(collected_data) >= max_items:
                    break
                    
                logger.info(f"서브레딧 r/{subreddit_name} 수집 중...")
                
                # 서브레딧 데이터 수집
                subreddit_data = await self._collect_subreddit_data(
                    subreddit_name, 
                    max_items - len(collected_data)
                )
                
                collected_data.extend(subreddit_data)
                logger.info(f"r/{subreddit_name}에서 {len(subreddit_data)}개 수집")
                
                # 요청 제한 준수
                await asyncio.sleep(1)
        
        except Exception as e:
            logger.error(f"Reddit 데이터 수집 중 오류: {e}")
            self.stats['errors'] += 1
        
        logger.info(f"Reddit 수집 완료: {len(collected_data)}개 항목")
        return collected_data
    
    async def _collect_subreddit_data(self, subreddit_name: str, max_items: int) -> List[QAEntry]:
        """특정 서브레딧에서 데이터 수집"""
        collected_data = []
        collection_config = self.config.get_collection_config()
        
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            
            # 최신 게시물 수집
            for submission in subreddit.new(limit=collection_config['max_submissions_per_subreddit']):
                if len(collected_data) >= max_items:
                    break
                
                self.stats['total_processed'] += 1
                
                # 중복 체크
                if self.dedup_tracker.is_reddit_submission_collected(submission.id):
                    self.stats['duplicate_submissions'] += 1
                    continue
                
                # 게시물 필터링
                if not self._is_valid_submission(submission):
                    self.stats['total_skipped'] += 1
                    continue
                
                # 답변 수집
                qa_entries = await self._extract_qa_from_submission(submission)
                
                for qa_entry in qa_entries:
                    if len(collected_data) >= max_items:
                        break
                    
                    collected_data.append(qa_entry)
                    self.stats['total_collected'] += 1
                
                # 수집된 게시물 추적
                self.dedup_tracker.mark_reddit_submission_collected(
                    submission.id,
                    submission.title,
                    subreddit_name,
                    quality_score=len(qa_entries),
                    metadata={'flair': getattr(submission, 'link_flair_text', None)}
                )
                
                # 요청 제한 준수
                await asyncio.sleep(0.5)
        
        except Exception as e:
            logger.error(f"서브레딧 r/{subreddit_name} 수집 중 오류: {e}")
            self.stats['errors'] += 1
        
        return collected_data
    
    def _is_valid_submission(self, submission) -> bool:
        """게시물 유효성 검사"""
        collection_config = self.config.get_collection_config()
        
        # 제목 길이 검사
        if len(submission.title) < collection_config['min_title_length']:
            return False
        
        if len(submission.title) > collection_config['max_title_length']:
            return False
        
        # 최소 업보트 검사
        if submission.score < collection_config['min_upvotes']:
            return False
        
        # 최소 댓글 수 검사
        if submission.num_comments < collection_config['min_comments']:
            return False
        
        # 나이 검사
        submission_age = datetime.utcnow() - datetime.utcfromtimestamp(submission.created_utc)
        if submission_age.days > collection_config['max_age_days']:
            return False
        
        # 플레어 검사
        flair = getattr(submission, 'link_flair_text', None)
        if flair:
            if collection_config['excluded_flairs']:
                if any(excluded in flair.lower() for excluded in collection_config['excluded_flairs']):
                    return False
        
        return True
    
    async def _extract_qa_from_submission(self, submission) -> List[QAEntry]:
        """게시물에서 Q&A 추출"""
        qa_entries = []
        
        try:
            # 댓글 로드
            submission.comments.replace_more(limit=5)
            
            # 각 댓글을 답변으로 검사
            for comment in submission.comments.list():
                if not self._is_valid_comment(comment):
                    continue
                
                # 봇 탐지
                if await self._is_bot_response(comment):
                    self.stats['bot_responses_filtered'] += 1
                    continue
                
                # Q&A 항목 생성
                qa_entry = self._create_qa_entry(submission, comment)
                
                if qa_entry:
                    qa_entries.append(qa_entry)
                    
                    # 답변 추적
                    self.dedup_tracker.mark_reddit_answer_collected(
                        comment.id,
                        submission.id,
                        str(comment.author),
                        quality_score=qa_entry.metadata.get('quality_score', 0.0),
                        metadata={'score': comment.score}
                    )
        
        except Exception as e:
            logger.error(f"Q&A 추출 중 오류: {e}")
            self.stats['errors'] += 1
        
        return qa_entries
    
    def _is_valid_comment(self, comment) -> bool:
        """댓글 유효성 검사"""
        if not hasattr(comment, 'body') or comment.body in ['[deleted]', '[removed]']:
            return False
        
        quality_config = self.config.get_quality_config()
        
        # 답변 길이 검사
        if len(comment.body) < quality_config['min_answer_length']:
            return False
        
        if len(comment.body) > quality_config['max_answer_length']:
            return False
        
        # 최소 점수 검사
        if hasattr(comment, 'score') and comment.score < 0:
            return False
        
        return True
    
    async def _is_bot_response(self, comment) -> bool:
        """봇 응답 탐지"""
        try:
            bot_config = self.config.get_bot_detection_config()
            
            if not bot_config['enabled']:
                return False
            
            # Ultimate Bot Detector 사용
            detection_result = await self.bot_detector.detect_bot_ultimate(
                content=comment.body,
                metadata={'author': str(comment.author)},
                user_data={'comment_score': getattr(comment, 'score', 0)},
                client_ip=f"reddit_collector_{comment.id}"
            )
            
            return detection_result.is_bot and detection_result.confidence >= bot_config['confidence_threshold']
        
        except Exception as e:
            logger.error(f"봇 탐지 중 오류: {e}")
            return False
    
    def _create_qa_entry(self, submission, comment) -> Optional[QAEntry]:
        """Q&A 항목 생성"""
        try:
            # 텍스트 정리
            user_question = clean_text(submission.title)
            user_context = clean_text(submission.selftext) if submission.selftext else ""
            assistant_response = clean_text(comment.body)
            
            # 코드 블록 추출
            code_blocks = extract_code_blocks(assistant_response)
            
            # 메타데이터 생성
            metadata = {
                'difficulty': self._estimate_difficulty(user_question, user_context),
                'functions': self._extract_excel_functions(assistant_response),
                'quality_score': calculate_quality_score(assistant_response, {
                    'upvotes': getattr(comment, 'score', 0),
                    'is_solved': 'solved' in str(getattr(submission, 'link_flair_text', '')).lower()
                }),
                'source': 'reddit',
                'is_solved': 'solved' in str(getattr(submission, 'link_flair_text', '')).lower(),
                'bot_detection_version': '4.0-ultimate',
                'reddit_metadata': {
                    'submission_id': submission.id,
                    'solution_id': comment.id,
                    'solution_type': self._classify_solution_type(submission),
                    'upvote_ratio': getattr(submission, 'upvote_ratio', 0.0),
                    'flair': getattr(submission, 'link_flair_text', None),
                    'has_images': bool(self._extract_image_urls(assistant_response)),
                    'image_urls': self._extract_image_urls(assistant_response)
                }
            }
            
            # 품질 점수 검사
            quality_config = self.config.get_quality_config()
            if metadata['quality_score'] < quality_config['min_quality_score']:
                self.stats['quality_failures'] += 1
                return None
            
            return QAEntry(
                id=generate_unique_id('reddit_qa'),
                user_question=user_question,
                user_context=user_context,
                assistant_response=assistant_response,
                code_blocks=code_blocks,
                metadata=metadata
            )
        
        except Exception as e:
            logger.error(f"Q&A 항목 생성 중 오류: {e}")
            return None
    
    def _estimate_difficulty(self, question: str, context: str) -> str:
        """난이도 추정"""
        combined_text = f"{question} {context}".lower()
        
        # 고급 키워드
        advanced_keywords = ['vba', 'macro', 'pivot', 'array', 'formula', 'function']
        advanced_count = sum(1 for keyword in advanced_keywords if keyword in combined_text)
        
        if advanced_count >= 2:
            return 'advanced'
        elif advanced_count >= 1:
            return 'intermediate'
        else:
            return 'beginner'
    
    def _extract_excel_functions(self, text: str) -> List[str]:
        """Excel 함수 추출"""
        # Excel 함수 패턴 매칭
        function_pattern = r'([A-Z][A-Z0-9_]*)\s*\('
        matches = re.findall(function_pattern, text.upper())
        
        # 알려진 Excel 함수 필터링
        excel_functions = [
            'SUM', 'AVERAGE', 'COUNT', 'MAX', 'MIN', 'IF', 'VLOOKUP', 'HLOOKUP', 
            'INDEX', 'MATCH', 'CONCATENATE', 'LEFT', 'RIGHT', 'MID', 'LEN', 
            'UPPER', 'LOWER', 'TRIM', 'SUBSTITUTE', 'FIND', 'SEARCH', 'DATE', 
            'TODAY', 'NOW', 'YEAR', 'MONTH', 'DAY', 'XLOOKUP', 'SUMIF', 'COUNTIF'
        ]
        
        return [func for func in matches if func in excel_functions]
    
    def _classify_solution_type(self, submission) -> str:
        """솔루션 타입 분류"""
        flair = str(getattr(submission, 'link_flair_text', '')).lower()
        
        if 'solved' in flair:
            return 'solved'
        elif 'discussion' in flair:
            return 'discussion'
        else:
            return 'general'
    
    def _extract_image_urls(self, text: str) -> List[str]:
        """이미지 URL 추출"""
        url_pattern = r'https?://[^\s]+\.(?:jpg|jpeg|png|gif|webp)'
        return re.findall(url_pattern, text, re.IGNORECASE)
    
    def get_collection_stats(self) -> CollectionStats:
        """수집 통계 반환"""
        return CollectionStats(
            source='reddit',
            total_collected=self.stats['total_collected'],
            total_skipped=self.stats['total_skipped'],
            collection_time_seconds=0.0,  # 실제 구현에서는 시간 측정
            quality_score_avg=0.0,  # 실제 구현에서는 평균 계산
            errors_count=self.stats['errors']
        )
    
    def get_detailed_stats(self) -> Dict[str, Any]:
        """상세 통계 반환"""
        return {
            **self.stats,
            'dedup_stats': self.dedup_tracker.get_reddit_stats(),
            'cache_stats': self.cache.cache.get_stats()
        }