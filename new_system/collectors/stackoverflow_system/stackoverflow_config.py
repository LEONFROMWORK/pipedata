"""
StackOverflow 독립 설정 시스템
StackOverflow 전용 설정 클래스로 다른 시스템과 완전히 분리
"""
import os
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging

logger = logging.getLogger('stackoverflow_system.config')

class StackOverflowConfig:
    """StackOverflow 전용 설정 클래스"""
    
    def __init__(self):
        """StackOverflow 설정 초기화"""
        self.base_path = Path('/Users/kevin/bigdata/data/stackoverflow')
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # StackOverflow API 설정
        self.stackoverflow_api_key = os.environ.get('STACKOVERFLOW_API_KEY', '')
        self.stackoverflow_api_base_url = 'https://api.stackexchange.com/2.3'
        self.site = 'stackoverflow'
        
        # StackOverflow 수집 설정
        self.tags = ['excel', 'vba', 'excel-formula', 'microsoft-excel']
        self.max_questions_per_tag = 100
        self.min_score = 5
        self.min_answer_count = 1
        self.max_age_days = 30
        
        # StackOverflow 필터링 설정
        self.required_tags = ['excel']
        self.excluded_tags = ['sharepoint', 'power-bi']
        self.min_title_length = 15
        self.max_title_length = 250
        self.min_body_length = 100
        
        # StackOverflow 품질 기준
        self.min_answer_length = 100
        self.max_answer_length = 10000
        self.min_quality_score = 3.0
        self.accepted_answer_bonus = 2.0
        self.min_view_count = 50
        
        # StackOverflow 캐시 설정
        self.cache_db_path = self.base_path / 'stackoverflow_cache.db'
        self.cache_ttl_seconds = 86400  # 24시간
        
        # StackOverflow 중복 추적 설정
        self.dedup_db_path = self.base_path / 'stackoverflow_dedup.db'
        
        # StackOverflow 출력 설정
        self.output_base_path = Path('/Users/kevin/bigdata/data/output')
        self.output_format = 'jsonl'
        
        # StackOverflow API 제한
        self.rate_limit_requests_per_day = 10000
        self.rate_limit_requests_per_second = 30
        self.request_timeout = 30
        self.max_retries = 3
        
        # StackOverflow 페이지네이션
        self.page_size = 100
        self.max_pages = 10
        
        # StackOverflow 로깅 설정
        self.log_level = logging.INFO
        self.log_file = self.base_path / 'stackoverflow_collector.log'
        
        logger.info("StackOverflow configuration initialized")
    
    def validate_config(self) -> bool:
        """StackOverflow 설정 유효성 검사"""
        if not self.tags:
            logger.error("No StackOverflow tags configured")
            return False
        
        if self.max_questions_per_tag <= 0:
            logger.error("Invalid max_questions_per_tag")
            return False
        
        if self.min_score < 0:
            logger.error("Invalid min_score")
            return False
        
        return True
    
    def get_api_config(self) -> Dict[str, Any]:
        """StackOverflow API 설정 반환"""
        return {
            'api_key': self.stackoverflow_api_key,
            'base_url': self.stackoverflow_api_base_url,
            'site': self.site,
            'timeout': self.request_timeout,
            'max_retries': self.max_retries
        }
    
    def get_cache_config(self) -> Dict[str, Any]:
        """StackOverflow 캐시 설정 반환"""
        return {
            'db_path': self.cache_db_path,
            'ttl_seconds': self.cache_ttl_seconds
        }
    
    def get_dedup_config(self) -> Dict[str, Any]:
        """StackOverflow 중복 추적 설정 반환"""
        return {
            'db_path': self.dedup_db_path
        }
    
    def get_collection_config(self) -> Dict[str, Any]:
        """StackOverflow 수집 설정 반환"""
        return {
            'tags': self.tags,
            'max_questions_per_tag': self.max_questions_per_tag,
            'min_score': self.min_score,
            'min_answer_count': self.min_answer_count,
            'max_age_days': self.max_age_days,
            'required_tags': self.required_tags,
            'excluded_tags': self.excluded_tags,
            'min_title_length': self.min_title_length,
            'max_title_length': self.max_title_length,
            'min_body_length': self.min_body_length
        }
    
    def get_quality_config(self) -> Dict[str, Any]:
        """StackOverflow 품질 기준 설정 반환"""
        return {
            'min_answer_length': self.min_answer_length,
            'max_answer_length': self.max_answer_length,
            'min_quality_score': self.min_quality_score,
            'accepted_answer_bonus': self.accepted_answer_bonus,
            'min_view_count': self.min_view_count
        }
    
    def get_rate_limit_config(self) -> Dict[str, Any]:
        """StackOverflow 수집 제한 설정 반환"""
        return {
            'requests_per_day': self.rate_limit_requests_per_day,
            'requests_per_second': self.rate_limit_requests_per_second,
            'timeout': self.request_timeout,
            'max_retries': self.max_retries
        }
    
    def get_pagination_config(self) -> Dict[str, Any]:
        """StackOverflow 페이지네이션 설정 반환"""
        return {
            'page_size': self.page_size,
            'max_pages': self.max_pages
        }
    
    def get_output_config(self) -> Dict[str, Any]:
        """StackOverflow 출력 설정 반환"""
        return {
            'base_path': self.output_base_path,
            'format': self.output_format
        }
    
    def build_api_url(self, endpoint: str, params: Dict[str, Any]) -> str:
        """StackOverflow API URL 생성"""
        base_params = {
            'site': self.site,
            'pagesize': self.page_size
        }
        
        if self.stackoverflow_api_key:
            base_params['key'] = self.stackoverflow_api_key
        
        base_params.update(params)
        
        param_string = '&'.join([f"{k}={v}" for k, v in base_params.items()])
        return f"{self.stackoverflow_api_base_url}/{endpoint}?{param_string}"
    
    def update_config(self, config_updates: Dict[str, Any]) -> None:
        """StackOverflow 설정 업데이트"""
        for key, value in config_updates.items():
            if hasattr(self, key):
                setattr(self, key, value)
                logger.info(f"StackOverflow config updated: {key} = {value}")
            else:
                logger.warning(f"Unknown StackOverflow config key: {key}")

# 글로벌 StackOverflow 설정 인스턴스
_stackoverflow_config = None

def get_stackoverflow_config() -> StackOverflowConfig:
    """StackOverflow 설정 인스턴스 반환"""
    global _stackoverflow_config
    if _stackoverflow_config is None:
        _stackoverflow_config = StackOverflowConfig()
    return _stackoverflow_config