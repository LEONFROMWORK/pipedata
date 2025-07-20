"""
Stack Overflow API Data Collector with Custom Filter
TRD Section 2: API 호출 효율성을 위한 사용자 정의 필터 사용
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlencode
import json

import httpx
import backoff

from core.usage_tracker import usage_tracker

from core.cache import APICache, LocalCache
from core.dedup_tracker import get_global_tracker
from config import Config

logger = logging.getLogger('pipeline.stackoverflow_collector')

class RateLimitExceeded(Exception):
    """Custom exception for rate limit handling"""
    pass

class StackOverflowCollector:
    """
    Advanced Stack Overflow API collector implementing TRD specifications:
    - Custom filters for API efficiency (TRD Section 2)
    - 24h TTL caching for all requests
    - Exponential backoff with jitter
    - Batch processing for answer retrieval
    """
    
    def __init__(self, cache: APICache):
        self.cache = cache
        self.config = Config.SO_API_CONFIG
        self.rate_config = Config.RATE_LIMITING
        self.dedup_tracker = get_global_tracker()  # 중복 방지 추적기
        
        # API client setup
        self.client = httpx.AsyncClient(
            timeout=30,
            headers={
                'User-Agent': 'Excel-QA-Dataset-Pipeline/1.0'
            }
        )
        
        # Rate limiting tracking
        self.requests_today = 0
        self.last_request_time = 0
        self.daily_quota_reset = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.info("StackOverflowCollector initialized with custom filter configuration")
    
    async def collect_excel_questions(self, from_date: Optional[datetime] = None, 
                                    max_pages: int = 50) -> List[Dict[str, Any]]:
        """
        Main collection method following TRD workflow
        
        Args:
            from_date: Start date for incremental collection
            max_pages: Maximum pages to collect (API efficiency)
            
        Returns:
            List of Q&A pairs with enriched answer data
        """
        if from_date is None:
            # Default to last 5 years for enhanced data collection (user request)
            from_date = datetime.now() - timedelta(days=365*5)
        
        logger.info(f"Starting Excel question collection from {from_date}")
        
        collected_questions = []
        page = 1
        has_more = True
        
        while has_more and page <= max_pages and self._check_rate_limit():
            try:
                # Get questions with custom filter
                questions_batch = await self._get_questions_batch(
                    from_date=from_date,
                    page=page
                )
                
                if not questions_batch['items']:
                    logger.info("No more questions available")
                    break
                
                # 중복 체크: 이미 수집된 질문 제외
                new_questions = self.dedup_tracker.filter_new_stackoverflow_questions(questions_batch['items'])
                
                if not new_questions:
                    logger.info(f"Page {page}: All questions already collected, skipping")
                    page += 1
                    continue
                
                # Enrich with answer data (batch processing)
                enriched_questions = await self._enrich_with_answers(new_questions)
                
                # Filter for quality (accepted answers only for Stack Overflow)
                filtered_questions = [
                    q for q in enriched_questions 
                    if q.get('is_answered') and q.get('accepted_answer')
                ]
                
                # 수집된 질문들을 중복 추적기에 등록
                for question in filtered_questions:
                    question_id = question.get('question_id')
                    title = question.get('title', '')
                    if question_id:
                        self.dedup_tracker.mark_stackoverflow_collected(
                            question_id, title, 
                            quality_score=question.get('score', 0),
                            metadata={'page': page, 'collection_date': datetime.now().isoformat()}
                        )
                
                collected_questions.extend(filtered_questions)
                
                logger.info(f"Page {page}: collected {len(filtered_questions)} quality Q&A pairs")
                
                has_more = questions_batch.get('has_more', False)
                page += 1
                
                # Rate limiting delay
                await self._rate_limit_delay()
                
            except RateLimitExceeded:
                logger.warning("Daily rate limit exceeded, stopping collection")
                break
            except Exception as e:
                logger.error(f"Error collecting page {page}: {e}")
                break
        
        logger.info(f"Collection complete: {len(collected_questions)} total Q&A pairs")
        return collected_questions
    
    @backoff.on_exception(
        backoff.expo,
        (httpx.HTTPError, httpx.TimeoutException),
        max_tries=5,
        max_time=300,
        jitter=backoff.full_jitter
    )
    async def _get_questions_batch(self, from_date: datetime, page: int) -> Dict[str, Any]:
        """
        Get questions batch with custom filter (TRD Section 2)
        Implements exponential backoff for resilience
        """
        # Check cache first
        cache_key = f"questions_page_{page}_{from_date.isoformat()}"
        cached_result = self.cache.get_stackoverflow_response('questions', {'page': page, 'from_date': from_date.isoformat()})
        
        if cached_result:
            logger.info(f"Using cached questions for page {page}")
            return cached_result
        
        # Build API request with custom filter - 채택된 답변이 있는 질문 우선 검색
        params = {
            'site': self.config['site'],
            'order': 'desc',
            'sort': 'activity',  # 최근 활동순으로 정렬 (채택된 답변이 있을 가능성 높음)
            'tagged': ';'.join(self.config['tags']),
            'page': page,
            'pagesize': 100,  # Maximum allowed
            'fromdate': int(from_date.timestamp()),
            'filter': self._get_custom_filter(),  # TRD custom filter
            'key': Config.STACKOVERFLOW_API_KEY,
            'accepted': 'True'  # 채택된 답변이 있는 질문만 검색
        }
        
        url = f"{self.config['base_url']}/questions?" + urlencode(params)
        
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            
            self._update_rate_limit_tracking(response)
            
            result = response.json()
            
            # Cache successful response for 24h (TRD requirement)
            self.cache.cache_stackoverflow_response(
                'questions', 
                {'page': page, 'from_date': from_date.isoformat()},
                result
            )
            
            logger.info(f"API request successful: page {page}, {len(result.get('items', []))} questions")
            return result
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RateLimitExceeded("Rate limit exceeded")
            raise
    
    async def _enrich_with_answers(self, questions: List[Dict]) -> List[Dict]:
        """
        Enrich questions with answer data using batch processing
        TRD: 100개 ID까지 배치 처리 가능
        """
        # Extract answer IDs for batch retrieval
        answer_ids = []
        questions_with_answers = []
        
        for question in questions:
            if question.get('accepted_answer_id'):
                answer_ids.append(question['accepted_answer_id'])
                questions_with_answers.append(question)
        
        if not answer_ids:
            logger.info("No accepted answers to retrieve")
            return questions
        
        # Process in batches of 100 (API limit)
        batch_size = 100
        all_answers = {}
        
        for i in range(0, len(answer_ids), batch_size):
            batch = answer_ids[i:i + batch_size]
            answers_batch = await self._get_answers_batch(batch)
            
            for answer in answers_batch.get('items', []):
                all_answers[answer['answer_id']] = answer
            
            # Rate limiting delay between batches
            if i + batch_size < len(answer_ids):
                await self._rate_limit_delay()
        
        # Merge answers with questions
        enriched_questions = []
        for question in questions_with_answers:
            answer_id = question.get('accepted_answer_id')
            if answer_id and answer_id in all_answers:
                question['accepted_answer'] = all_answers[answer_id]
                enriched_questions.append(question)
        
        logger.info(f"Enriched {len(enriched_questions)} questions with answer data")
        return enriched_questions
    
    @backoff.on_exception(
        backoff.expo,
        (httpx.HTTPError, httpx.TimeoutException),
        max_tries=5,
        max_time=300,
        jitter=backoff.full_jitter
    )
    async def _get_answers_batch(self, answer_ids: List[int]) -> Dict[str, Any]:
        """Get batch of answers by IDs"""
        ids_str = ';'.join(map(str, answer_ids))
        
        # Check cache
        cached_result = self.cache.get_stackoverflow_response('answers', {'ids': ids_str})
        if cached_result:
            logger.info(f"Using cached answers for {len(answer_ids)} IDs")
            return cached_result
        
        params = {
            'site': self.config['site'],
            'order': 'desc',
            'sort': 'creation',
            'filter': self._get_answer_filter(),
            'key': Config.STACKOVERFLOW_API_KEY
        }
        
        url = f"{self.config['base_url']}/answers/{ids_str}?" + urlencode(params)
        
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            
            self._update_rate_limit_tracking(response)
            
            result = response.json()
            
            # Cache for 24h
            self.cache.cache_stackoverflow_response('answers', {'ids': ids_str}, result)
            
            return result
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RateLimitExceeded("Rate limit exceeded")
            raise
    
    def _get_custom_filter(self) -> str:
        """
        Generate custom filter for questions API (TRD Section 2)
        Only requests necessary fields for efficiency
        """
        fields = self.config['custom_filter']['question_fields']
        # Stack Overflow filter creation would typically be done via their API
        # For now, we'll use the default filter with all needed fields
        return '!9YdnSJ*_T'  # This would be generated/configured filter ID
    
    def _get_answer_filter(self) -> str:
        """Generate custom filter for answers API - includes body_markdown"""
        # Use withbody filter to include answer body content
        # This includes the body field for answers
        return 'withbody'
    
    def _check_rate_limit(self) -> bool:
        """Check if we can make another API request"""
        # Reset daily counter if needed
        now = datetime.now()
        if now >= self.daily_quota_reset + timedelta(days=1):
            self.requests_today = 0
            self.daily_quota_reset = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        return self.requests_today < self.rate_config['max_requests_per_day']
    
    def _update_rate_limit_tracking(self, response: httpx.Response) -> None:
        """Update rate limit tracking from API response headers"""
        self.requests_today += 1
        self.last_request_time = time.time()
        
        # Log quota information if available
        quota_remaining = response.headers.get('x-ratelimit-remaining')
        if quota_remaining:
            logger.info(f"API quota remaining: {quota_remaining}")
    
    async def _rate_limit_delay(self) -> None:
        """Implement rate limiting delay between requests"""
        min_delay = 60 / self.rate_config['requests_per_minute']  # Seconds between requests
        elapsed = time.time() - self.last_request_time
        
        if elapsed < min_delay:
            delay = min_delay - elapsed
            logger.debug(f"Rate limiting delay: {delay:.2f}s")
            await asyncio.sleep(delay)
    
    async def close(self) -> None:
        """Clean up HTTP client"""
        await self.client.aclose()
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get collection statistics"""
        return {
            'requests_today': self.requests_today,
            'daily_quota_remaining': self.rate_config['max_requests_per_day'] - self.requests_today,
            'last_request': datetime.fromtimestamp(self.last_request_time) if self.last_request_time else None,
            'daily_quota_reset': self.daily_quota_reset,
            'rate_limit_per_minute': self.rate_config['requests_per_minute']
        }