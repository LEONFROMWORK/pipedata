"""
Reddit 독립 설정 시스템
Reddit 전용 설정 클래스로 다른 시스템과 완전히 분리
"""
import os
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging

logger = logging.getLogger('reddit_system.config')

class RedditConfig:
    """Reddit 전용 설정 클래스"""
    
    def __init__(self):
        """Reddit 설정 초기화"""
        self.base_path = Path('/Users/kevin/bigdata/data/reddit')
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Reddit API 설정
        self.reddit_client_id = os.environ.get('REDDIT_CLIENT_ID', '')
        self.reddit_client_secret = os.environ.get('REDDIT_CLIENT_SECRET', '')
        self.reddit_user_agent = os.environ.get('REDDIT_USER_AGENT', 'ExcelDataCollector/1.0')
        
        # Reddit 수집 설정
        self.subreddits = ['excel', 'ExcelTips', 'MSExcel']
        self.max_submissions_per_subreddit = 100
        self.min_upvotes = 5
        self.min_comments = 3
        self.max_age_days = 30
        
        # Reddit 필터링 설정
        self.required_flairs = ['solved', 'discussion', 'unsolved']
        self.excluded_flairs = ['meta', 'waiting on op']
        self.min_title_length = 10
        self.max_title_length = 200
        
        # Reddit 품질 기준
        self.min_answer_length = 50
        self.max_answer_length = 5000
        self.min_quality_score = 2.0
        self.upvote_ratio_threshold = 0.6
        
        # Reddit 캐시 설정
        self.cache_db_path = self.base_path / 'reddit_cache.db'
        self.cache_ttl_seconds = 86400  # 24시간
        
        # Reddit 중복 추적 설정
        self.dedup_db_path = self.base_path / 'reddit_dedup.db'
        
        # Reddit 출력 설정
        self.output_base_path = Path('/Users/kevin/bigdata/data/output')
        self.output_format = 'jsonl'
        
        # Reddit 봇 탐지 설정
        self.bot_detection_enabled = True
        self.bot_detection_confidence_threshold = 0.7
        
        # Reddit 수집 제한
        self.rate_limit_requests_per_minute = 60
        self.concurrent_requests = 5
        self.request_timeout = 30
        
        # Reddit 로깅 설정
        self.log_level = logging.INFO
        self.log_file = self.base_path / 'reddit_collector.log'
        
        logger.info("Reddit configuration initialized")
    
    def validate_config(self) -> bool:
        """Reddit 설정 유효성 검사"""
        if not self.reddit_client_id:
            logger.error("Reddit client ID not provided")
            return False
        
        if not self.reddit_client_secret:
            logger.error("Reddit client secret not provided")
            return False
        
        if not self.subreddits:
            logger.error("No subreddits configured")
            return False
        
        return True
    
    def get_reddit_praw_config(self) -> Dict[str, str]:
        """Reddit PRAW 설정 반환"""
        return {
            'client_id': self.reddit_client_id,
            'client_secret': self.reddit_client_secret,
            'user_agent': self.reddit_user_agent
        }
    
    def get_cache_config(self) -> Dict[str, Any]:
        """Reddit 캐시 설정 반환"""
        return {
            'db_path': self.cache_db_path,
            'ttl_seconds': self.cache_ttl_seconds
        }
    
    def get_dedup_config(self) -> Dict[str, Any]:
        """Reddit 중복 추적 설정 반환"""
        return {
            'db_path': self.dedup_db_path
        }
    
    def get_collection_config(self) -> Dict[str, Any]:
        """Reddit 수집 설정 반환"""
        return {
            'subreddits': self.subreddits,
            'max_submissions_per_subreddit': self.max_submissions_per_subreddit,
            'min_upvotes': self.min_upvotes,
            'min_comments': self.min_comments,
            'max_age_days': self.max_age_days,
            'required_flairs': self.required_flairs,
            'excluded_flairs': self.excluded_flairs,
            'min_title_length': self.min_title_length,
            'max_title_length': self.max_title_length
        }
    
    def get_quality_config(self) -> Dict[str, Any]:
        """Reddit 품질 기준 설정 반환"""
        return {
            'min_answer_length': self.min_answer_length,
            'max_answer_length': self.max_answer_length,
            'min_quality_score': self.min_quality_score,
            'upvote_ratio_threshold': self.upvote_ratio_threshold
        }
    
    def get_bot_detection_config(self) -> Dict[str, Any]:
        """Reddit 봇 탐지 설정 반환"""
        return {
            'enabled': self.bot_detection_enabled,
            'confidence_threshold': self.bot_detection_confidence_threshold
        }
    
    def get_rate_limit_config(self) -> Dict[str, Any]:
        """Reddit 수집 제한 설정 반환"""
        return {
            'requests_per_minute': self.rate_limit_requests_per_minute,
            'concurrent_requests': self.concurrent_requests,
            'request_timeout': self.request_timeout
        }
    
    def get_output_config(self) -> Dict[str, Any]:
        """Reddit 출력 설정 반환"""
        return {
            'base_path': self.output_base_path,
            'format': self.output_format
        }
    
    def update_config(self, config_updates: Dict[str, Any]) -> None:
        """Reddit 설정 업데이트"""
        for key, value in config_updates.items():
            if hasattr(self, key):
                setattr(self, key, value)
                logger.info(f"Reddit config updated: {key} = {value}")
            else:
                logger.warning(f"Unknown Reddit config key: {key}")

# 글로벌 Reddit 설정 인스턴스
_reddit_config = None

def get_reddit_config() -> RedditConfig:
    """Reddit 설정 인스턴스 반환"""
    global _reddit_config
    if _reddit_config is None:
        _reddit_config = RedditConfig()
    return _reddit_config