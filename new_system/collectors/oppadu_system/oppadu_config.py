"""
Oppadu 독립 설정 시스템
Oppadu 전용 설정 클래스로 다른 시스템과 완전히 분리
"""
import os
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging

logger = logging.getLogger('oppadu_system.config')

class OppaduConfig:
    """Oppadu 전용 설정 클래스"""
    
    def __init__(self):
        """Oppadu 설정 초기화"""
        self.base_path = Path('/Users/kevin/bigdata/data/oppadu')
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Oppadu 웹사이트 설정
        self.base_url = 'https://www.oppadu.com'
        self.community_url = f'{self.base_url}/community/question/'
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
        ]
        
        # Oppadu 수집 설정
        self.max_pages = 50
        self.max_posts_per_page = 20
        self.only_answered_posts = True
        self.max_age_days = 90
        
        # Oppadu 필터링 설정
        self.required_keywords = ['엑셀', '함수', '수식', '셀']
        self.excluded_keywords = ['구매', '판매', '광고']
        self.min_title_length = 5
        self.max_title_length = 200
        self.min_content_length = 50
        
        # Oppadu 품질 기준
        self.min_answer_length = 30
        self.max_answer_length = 5000
        self.min_quality_score = 2.0
        self.korean_content_weight = 1.5
        
        # Oppadu 크롤링 설정
        self.request_timeout = 30
        self.max_retries = 3
        self.retry_delay = 2.0
        self.human_delay_min = 1.0
        self.human_delay_max = 3.0
        self.page_delay_min = 3.0
        self.page_delay_max = 7.0
        
        # Oppadu 캐시 설정
        self.cache_db_path = self.base_path / 'oppadu_cache.db'
        self.cache_ttl_seconds = 21600  # 6시간 (크롤링 대응)
        
        # Oppadu 중복 추적 설정
        self.dedup_db_path = self.base_path / 'oppadu_dedup.db'
        
        # Oppadu 출력 설정
        self.output_base_path = Path('/Users/kevin/bigdata/data/output')
        self.output_format = 'jsonl'
        
        # Oppadu 웹 드라이버 설정
        self.use_selenium = True
        self.headless_mode = True
        self.window_size = '1920,1080'
        self.chrome_options = [
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--lang=ko-KR'
        ]
        
        # Oppadu 우회 설정
        self.use_cloudscraper = True
        self.rotate_user_agents = True
        self.use_proxy = False
        self.proxy_list = []
        
        # Oppadu 로깅 설정
        self.log_level = logging.INFO
        self.log_file = self.base_path / 'oppadu_collector.log'
        
        logger.info("Oppadu configuration initialized")
    
    def validate_config(self) -> bool:
        """Oppadu 설정 유효성 검사"""
        if not self.base_url:
            logger.error("Oppadu base URL not configured")
            return False
        
        if self.max_pages <= 0:
            logger.error("Invalid max_pages")
            return False
        
        if self.request_timeout <= 0:
            logger.error("Invalid request_timeout")
            return False
        
        return True
    
    def get_web_config(self) -> Dict[str, Any]:
        """Oppadu 웹 설정 반환"""
        return {
            'base_url': self.base_url,
            'community_url': self.community_url,
            'user_agents': self.user_agents,
            'timeout': self.request_timeout,
            'max_retries': self.max_retries,
            'retry_delay': self.retry_delay
        }
    
    def get_cache_config(self) -> Dict[str, Any]:
        """Oppadu 캐시 설정 반환"""
        return {
            'db_path': self.cache_db_path,
            'ttl_seconds': self.cache_ttl_seconds
        }
    
    def get_dedup_config(self) -> Dict[str, Any]:
        """Oppadu 중복 추적 설정 반환"""
        return {
            'db_path': self.dedup_db_path
        }
    
    def get_collection_config(self) -> Dict[str, Any]:
        """Oppadu 수집 설정 반환"""
        return {
            'max_pages': self.max_pages,
            'max_posts_per_page': self.max_posts_per_page,
            'only_answered_posts': self.only_answered_posts,
            'max_age_days': self.max_age_days,
            'required_keywords': self.required_keywords,
            'excluded_keywords': self.excluded_keywords,
            'min_title_length': self.min_title_length,
            'max_title_length': self.max_title_length,
            'min_content_length': self.min_content_length
        }
    
    def get_quality_config(self) -> Dict[str, Any]:
        """Oppadu 품질 기준 설정 반환"""
        return {
            'min_answer_length': self.min_answer_length,
            'max_answer_length': self.max_answer_length,
            'min_quality_score': self.min_quality_score,
            'korean_content_weight': self.korean_content_weight
        }
    
    def get_crawling_config(self) -> Dict[str, Any]:
        """Oppadu 크롤링 설정 반환"""
        return {
            'human_delay_min': self.human_delay_min,
            'human_delay_max': self.human_delay_max,
            'page_delay_min': self.page_delay_min,
            'page_delay_max': self.page_delay_max,
            'request_timeout': self.request_timeout,
            'max_retries': self.max_retries,
            'retry_delay': self.retry_delay
        }
    
    def get_selenium_config(self) -> Dict[str, Any]:
        """Oppadu Selenium 설정 반환"""
        return {
            'use_selenium': self.use_selenium,
            'headless_mode': self.headless_mode,
            'window_size': self.window_size,
            'chrome_options': self.chrome_options
        }
    
    def get_bypass_config(self) -> Dict[str, Any]:
        """Oppadu 우회 설정 반환"""
        return {
            'use_cloudscraper': self.use_cloudscraper,
            'rotate_user_agents': self.rotate_user_agents,
            'use_proxy': self.use_proxy,
            'proxy_list': self.proxy_list
        }
    
    def get_output_config(self) -> Dict[str, Any]:
        """Oppadu 출력 설정 반환"""
        return {
            'base_path': self.output_base_path,
            'format': self.output_format
        }
    
    def build_post_url(self, post_id: str) -> str:
        """Oppadu 게시물 URL 생성"""
        return f"{self.community_url}?board_id=1&action=view&uid={post_id}"
    
    def build_page_url(self, page: int) -> str:
        """Oppadu 페이지 URL 생성"""
        if page <= 1:
            return self.community_url
        return f"{self.community_url}?page={page}"
    
    def update_config(self, config_updates: Dict[str, Any]) -> None:
        """Oppadu 설정 업데이트"""
        for key, value in config_updates.items():
            if hasattr(self, key):
                setattr(self, key, value)
                logger.info(f"Oppadu config updated: {key} = {value}")
            else:
                logger.warning(f"Unknown Oppadu config key: {key}")

# 글로벌 Oppadu 설정 인스턴스
_oppadu_config = None

def get_oppadu_config() -> OppaduConfig:
    """Oppadu 설정 인스턴스 반환"""
    global _oppadu_config
    if _oppadu_config is None:
        _oppadu_config = OppaduConfig()
    return _oppadu_config