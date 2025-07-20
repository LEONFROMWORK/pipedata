#!/usr/bin/env python3
"""
수정된 Stack Overflow API Data Collector
주요 수정사항:
1. API 파라미터 수정 (accepted=True 제거)
2. 답변 수집 로직 개선
3. 질문-답변 매칭 알고리즘 수정
4. 실제 API 응답 구조에 맞는 코드 수정
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

from core.cache import APICache, LocalCache
from core.dedup_tracker import get_global_tracker
from config import Config

logger = logging.getLogger('pipeline.fixed_stackoverflow_collector')

class RateLimitExceeded(Exception):
    """Custom exception for rate limit handling"""
    pass

class FixedStackOverflowCollector:
    """
    수정된 Stack Overflow API collector
    - 답변이 있는 질문 우선 수집
    - 올바른 API 파라미터 사용
    - 개선된 답변 매칭 로직
    """
    
    def __init__(self, cache: APICache):
        self.cache = cache
        self.config = Config.SO_API_CONFIG
        self.rate_config = Config.RATE_LIMITING
        self.dedup_tracker = get_global_tracker()
        
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
        
        logger.info("FixedStackOverflowCollector initialized")

    async def collect_excel_questions_fixed(self, from_date: Optional[datetime] = None, 
                                          max_pages: int = 50) -> List[Dict[str, Any]]:
        """
        수정된 메인 수집 메소드
        - 답변이 있는 질문만 우선 수집
        - 개선된 매칭 로직
        """
        if from_date is None:
            from_date = datetime.now() - timedelta(days=30)
        
        logger.info(f"🚀 Fixed collection starting from {from_date}")
        
        collected_pairs = []
        page = 1
        has_more = True
        
        while has_more and page <= max_pages and self._check_rate_limit():
            try:
                logger.info(f"📄 Processing page {page}")
                
                # 1. 답변이 있는 질문들 수집 (수정된 파라미터)
                questions_batch = await self._get_answered_questions_batch(
                    from_date=from_date,
                    page=page
                )
                
                if not questions_batch['items']:
                    logger.info("No more questions available")
                    break
                
                # 2. 중복 체크
                new_questions = self.dedup_tracker.filter_new_stackoverflow_questions(questions_batch['items'])
                
                if not new_questions:
                    logger.info(f"Page {page}: All questions already collected")
                    page += 1
                    continue
                
                logger.info(f"📝 Found {len(new_questions)} new questions")
                
                # 3. 답변 데이터 수집 및 매칭 (개선된 로직)
                complete_pairs = await self._collect_complete_qa_pairs(new_questions)
                
                logger.info(f"✅ Created {len(complete_pairs)} complete Q&A pairs")
                
                # 4. 중복 추적기에 등록
                for pair in complete_pairs:
                    question = pair['question']
                    question_id = question.get('question_id')
                    title = question.get('title', '')
                    if question_id:
                        self.dedup_tracker.mark_stackoverflow_collected(
                            question_id, title,
                            quality_score=question.get('score', 0),
                            metadata={'page': page, 'has_answer': bool(pair.get('answer'))}
                        )
                
                collected_pairs.extend(complete_pairs)
                
                has_more = questions_batch.get('has_more', False)
                page += 1
                
                # Rate limiting
                await self._rate_limit_delay()
                
            except RateLimitExceeded:
                logger.warning("Daily rate limit exceeded")
                break
            except Exception as e:
                logger.error(f"Error on page {page}: {e}")
                break
        
        logger.info(f"🎉 Collection complete: {len(collected_pairs)} total Q&A pairs")
        return collected_pairs

    @backoff.on_exception(
        backoff.expo,
        (httpx.HTTPError, httpx.TimeoutException),
        max_tries=5,
        max_time=300,
        jitter=backoff.full_jitter
    )
    async def _get_answered_questions_batch(self, from_date: datetime, page: int) -> Dict[str, Any]:
        """
        답변이 있는 질문들 수집 (수정된 API 파라미터)
        """
        # 캐시 확인
        cache_key = f"fixed_questions_page_{page}_{from_date.isoformat()}"
        cached_result = self.cache.get_stackoverflow_response('fixed_questions', {'page': page, 'from_date': from_date.isoformat()})
        
        if cached_result:
            logger.info(f"Using cached questions for page {page}")
            return cached_result
        
        # 수정된 API 파라미터 - accepted 제거, answers 포함 필터 사용
        params = {
            'site': 'stackoverflow',
            'order': 'desc', 
            'sort': 'votes',  # 투표순으로 정렬 (답변 있을 가능성 높음)
            'tagged': 'excel-formula',
            'page': page,
            'pagesize': 50,  # 줄여서 품질 높이기
            'fromdate': int(from_date.timestamp()),
            'filter': '!nNPvSNdWme',  # 기본 필터에 답변 정보 포함
            'key': Config.STACKOVERFLOW_API_KEY,
            # accepted=True 제거 - 이 파라미터가 문제였음
        }
        
        url = f"{self.config['base_url']}/questions?" + urlencode(params)
        
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            
            self._update_rate_limit_tracking(response)
            
            result = response.json()
            
            # 캐시 저장
            self.cache.cache_stackoverflow_response(
                'fixed_questions',
                {'page': page, 'from_date': from_date.isoformat()},
                result
            )
            
            logger.info(f"✅ API success: page {page}, {len(result.get('items', []))} questions")
            return result
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise RateLimitExceeded("Rate limit exceeded")
            logger.error(f"HTTP error: {e.response.status_code}")
            raise

    async def _collect_complete_qa_pairs(self, questions: List[Dict]) -> List[Dict]:
        """
        질문과 답변을 매칭하여 완전한 Q&A 쌍 생성 (개선된 로직)
        """
        complete_pairs = []
        
        # 1. 이미 accepted_answer_id가 있는 질문들 처리
        questions_with_accepted = []
        for question in questions:
            if question.get('accepted_answer_id'):
                questions_with_accepted.append(question)
        
        logger.info(f"📋 Questions with accepted answers: {len(questions_with_accepted)}")
        
        # 2. 채택된 답변들 배치 수집
        if questions_with_accepted:
            answer_ids = [q['accepted_answer_id'] for q in questions_with_accepted]
            answers_data = await self._get_answers_batch_fixed(answer_ids)
            
            # 3. 질문-답변 매칭
            answers_by_id = {ans['answer_id']: ans for ans in answers_data.get('items', [])}
            
            for question in questions_with_accepted:
                answer_id = question.get('accepted_answer_id')
                if answer_id in answers_by_id:
                    complete_pairs.append({
                        'question': question,
                        'answer': answers_by_id[answer_id],
                        'quality_score': question.get('score', 0) + answers_by_id[answer_id].get('score', 0)
                    })
        
        # 4. accepted_answer_id가 없는 질문들은 답변 검색
        questions_without_accepted = [q for q in questions if not q.get('accepted_answer_id')]
        
        if questions_without_accepted:
            logger.info(f"🔍 Searching answers for {len(questions_without_accepted)} questions")
            
            for question in questions_without_accepted[:10]:  # 제한해서 API 절약
                try:
                    question_answers = await self._get_question_answers(question['question_id'])
                    
                    if question_answers and len(question_answers.get('items', [])) > 0:
                        # 가장 높은 점수의 답변 선택
                        best_answer = max(question_answers['items'], key=lambda x: x.get('score', 0))
                        
                        if best_answer.get('score', 0) >= 1:  # 최소 점수 1 이상
                            complete_pairs.append({
                                'question': question,
                                'answer': best_answer,
                                'quality_score': question.get('score', 0) + best_answer.get('score', 0)
                            })
                        
                        await asyncio.sleep(0.1)  # API 보호
                        
                except Exception as e:
                    logger.warning(f"Failed to get answers for question {question.get('question_id')}: {e}")
                    continue
        
        logger.info(f"🎯 Generated {len(complete_pairs)} complete Q&A pairs")
        return complete_pairs

    @backoff.on_exception(
        backoff.expo,
        (httpx.HTTPError, httpx.TimeoutException),
        max_tries=3,
        max_time=60
    )
    async def _get_answers_batch_fixed(self, answer_ids: List[int]) -> Dict[str, Any]:
        """수정된 답변 배치 수집"""
        ids_str = ';'.join(map(str, answer_ids))
        
        # 캐시 확인
        cached_result = self.cache.get_stackoverflow_response('fixed_answers', {'ids': ids_str})
        if cached_result:
            return cached_result
        
        params = {
            'site': 'stackoverflow',
            'order': 'desc',
            'sort': 'votes',
            'filter': 'withbody',  # 답변 본문 포함
            'key': Config.STACKOVERFLOW_API_KEY
        }
        
        url = f"{self.config['base_url']}/answers/{ids_str}?" + urlencode(params)
        
        response = await self.client.get(url)
        response.raise_for_status()
        
        self._update_rate_limit_tracking(response)
        result = response.json()
        
        # 캐시 저장
        self.cache.cache_stackoverflow_response('fixed_answers', {'ids': ids_str}, result)
        
        return result

    @backoff.on_exception(
        backoff.expo,
        (httpx.HTTPError, httpx.TimeoutException),
        max_tries=3,
        max_time=60
    )
    async def _get_question_answers(self, question_id: int) -> Dict[str, Any]:
        """특정 질문의 모든 답변 수집"""
        params = {
            'site': 'stackoverflow',
            'order': 'desc',
            'sort': 'votes',
            'filter': 'withbody',
            'key': Config.STACKOVERFLOW_API_KEY
        }
        
        url = f"{self.config['base_url']}/questions/{question_id}/answers?" + urlencode(params)
        
        response = await self.client.get(url)
        response.raise_for_status()
        
        self._update_rate_limit_tracking(response)
        
        return response.json()

    def _check_rate_limit(self) -> bool:
        """Rate limit 확인"""
        now = datetime.now()
        if now >= self.daily_quota_reset + timedelta(days=1):
            self.requests_today = 0
            self.daily_quota_reset = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        return self.requests_today < self.rate_config['max_requests_per_day']

    def _update_rate_limit_tracking(self, response: httpx.Response) -> None:
        """Rate limit 추적 업데이트"""
        self.requests_today += 1
        self.last_request_time = time.time()
        
        quota_remaining = response.headers.get('x-ratelimit-remaining')
        if quota_remaining:
            logger.info(f"API quota remaining: {quota_remaining}")

    async def _rate_limit_delay(self) -> None:
        """Rate limiting 지연"""
        min_delay = 60 / self.rate_config['requests_per_minute']
        elapsed = time.time() - self.last_request_time
        
        if elapsed < min_delay:
            delay = min_delay - elapsed
            await asyncio.sleep(delay)

    async def close(self) -> None:
        """HTTP client 정리"""
        await self.client.aclose()

    def get_collection_stats(self) -> Dict[str, Any]:
        """수집 통계"""
        return {
            'requests_today': self.requests_today,
            'daily_quota_remaining': self.rate_config['max_requests_per_day'] - self.requests_today,
            'last_request': datetime.fromtimestamp(self.last_request_time) if self.last_request_time else None,
            'daily_quota_reset': self.daily_quota_reset,
            'rate_limit_per_minute': self.rate_config['requests_per_minute']
        }