"""
독립 StackOverflow 수집기
StackOverflow 전용 독립 시스템을 사용하는 완전히 분리된 수집기
"""
import asyncio
import logging
import time
import aiohttp
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import re
import sys
from pathlib import Path
import gzip
from urllib.parse import urlencode

# 상위 디렉토리 경로 추가
sys.path.append(str(Path(__file__).parent.parent.parent))

# 독립 시스템 import
from .stackoverflow_cache import StackOverflowCache, StackOverflowAPICache
from .stackoverflow_dedup_tracker import get_stackoverflow_tracker
from .stackoverflow_config import get_stackoverflow_config

# 공통 유틸리티 import
from shared.utils import generate_unique_id, calculate_quality_score, extract_code_blocks, clean_text
from shared.data_models import QAEntry, CollectionStats

logger = logging.getLogger('stackoverflow_system.collector')

class StackOverflowCollector:
    """독립 StackOverflow 수집기"""
    
    def __init__(self):
        """StackOverflow 수집기 초기화"""
        self.config = get_stackoverflow_config()
        self.dedup_tracker = get_stackoverflow_tracker()
        
        # 독립 캐시 시스템 초기화
        stackoverflow_cache = StackOverflowCache(self.config.cache_db_path)
        self.cache = StackOverflowAPICache(stackoverflow_cache)
        
        # HTTP 세션
        self.session = None
        
        # 수집 통계
        self.stats = {
            'total_processed': 0,
            'total_collected': 0,
            'total_skipped': 0,
            'duplicate_questions': 0,
            'quality_failures': 0,
            'api_errors': 0,
            'rate_limit_hits': 0
        }
        
        logger.info("독립 StackOverflow 수집기 초기화 완료")
    
    async def collect_excel_qa_data(self, max_items: int = 100) -> List[QAEntry]:
        """Excel Q&A 데이터 수집"""
        logger.info(f"StackOverflow Excel Q&A 데이터 수집 시작 (최대 {max_items}개)")
        
        collected_data = []
        collection_config = self.config.get_collection_config()
        
        # HTTP 세션 초기화
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.request_timeout),
            headers={'User-Agent': 'ExcelDataCollector/1.0'}
        )
        
        try:
            for tag in collection_config['tags']:
                if len(collected_data) >= max_items:
                    break
                    
                logger.info(f"태그 '{tag}' 수집 중...")
                
                # 태그별 데이터 수집
                tag_data = await self._collect_tag_data(
                    tag, 
                    max_items - len(collected_data)
                )
                
                collected_data.extend(tag_data)
                logger.info(f"태그 '{tag}'에서 {len(tag_data)}개 수집")
                
                # API 제한 준수
                await asyncio.sleep(1)
        
        except Exception as e:
            logger.error(f"StackOverflow 데이터 수집 중 오류: {e}")
            self.stats['api_errors'] += 1
        
        finally:
            if self.session:
                await self.session.close()
        
        logger.info(f"StackOverflow 수집 완료: {len(collected_data)}개 항목")
        return collected_data
    
    async def _collect_tag_data(self, tag: str, max_items: int) -> List[QAEntry]:
        """특정 태그에서 데이터 수집"""
        collected_data = []
        collection_config = self.config.get_collection_config()
        pagination_config = self.config.get_pagination_config()
        
        page = 1
        has_more = True
        
        while has_more and len(collected_data) < max_items and page <= pagination_config['max_pages']:
            try:
                # 질문 목록 조회
                questions = await self._fetch_questions_by_tag(tag, page)
                
                if not questions:
                    break
                
                # 각 질문 처리
                for question in questions:
                    if len(collected_data) >= max_items:
                        break
                    
                    self.stats['total_processed'] += 1
                    
                    # 중복 체크
                    if self.dedup_tracker.is_stackoverflow_question_collected(question['question_id']):
                        self.stats['duplicate_questions'] += 1
                        continue
                    
                    # 질문 필터링
                    if not self._is_valid_question(question):
                        self.stats['total_skipped'] += 1
                        continue
                    
                    # 답변 수집
                    qa_entries = await self._extract_qa_from_question(question)
                    
                    for qa_entry in qa_entries:
                        if len(collected_data) >= max_items:
                            break
                        
                        collected_data.append(qa_entry)
                        self.stats['total_collected'] += 1
                    
                    # 수집된 질문 추적
                    self.dedup_tracker.mark_stackoverflow_question_collected(
                        question['question_id'],
                        question['title'],
                        ','.join(question.get('tags', [])),
                        view_count=question.get('view_count', 0),
                        answer_count=question.get('answer_count', 0),
                        quality_score=len(qa_entries),
                        metadata={'score': question.get('score', 0)}
                    )
                    
                    # API 제한 준수
                    await asyncio.sleep(0.1)
                
                page += 1
                has_more = len(questions) >= pagination_config['page_size']
            
            except Exception as e:
                logger.error(f"태그 '{tag}' 페이지 {page} 수집 중 오류: {e}")
                self.stats['api_errors'] += 1
                break
        
        return collected_data
    
    async def _fetch_questions_by_tag(self, tag: str, page: int) -> List[Dict[str, Any]]:
        """태그별 질문 목록 조회"""
        try:
            # 캐시 확인
            cache_key = f"questions_tag_{tag}_page_{page}"
            cached_data = self.cache.get_cached_response(cache_key)
            if cached_data:
                return cached_data
            
            # API 요청 파라미터
            params = {
                'tagged': tag,
                'page': page,
                'sort': 'creation',
                'order': 'desc',
                'filter': 'withbody'
            }
            
            # API URL 생성
            url = self.config.build_api_url('questions', params)
            
            # API 호출
            async with self.session.get(url) as response:
                if response.status == 200:
                    # gzip 압축 해제
                    if response.headers.get('content-encoding') == 'gzip':
                        data = gzip.decompress(await response.read())
                        result = json.loads(data.decode('utf-8'))
                    else:
                        result = await response.json()
                    
                    questions = result.get('items', [])
                    
                    # 캐시 저장
                    self.cache.cache_response(cache_key, questions)
                    
                    return questions
                
                elif response.status == 429:  # Rate limit
                    self.stats['rate_limit_hits'] += 1
                    logger.warning("StackOverflow API 요청 제한 도달")
                    await asyncio.sleep(5)
                    return []
                
                else:
                    logger.error(f"StackOverflow API 오류: {response.status}")
                    return []
        
        except Exception as e:
            logger.error(f"질문 목록 조회 중 오류: {e}")
            return []
    
    def _is_valid_question(self, question: Dict[str, Any]) -> bool:
        """질문 유효성 검사"""
        collection_config = self.config.get_collection_config()
        quality_config = self.config.get_quality_config()
        
        # 제목 길이 검사
        title = question.get('title', '')
        if len(title) < collection_config['min_title_length']:
            return False
        
        if len(title) > collection_config['max_title_length']:
            return False
        
        # 본문 길이 검사
        body = question.get('body', '')
        if len(body) < collection_config['min_body_length']:
            return False
        
        # 최소 점수 검사
        if question.get('score', 0) < collection_config['min_score']:
            return False
        
        # 최소 답변 수 검사
        if question.get('answer_count', 0) < collection_config['min_answer_count']:
            return False
        
        # 최소 조회수 검사
        if question.get('view_count', 0) < quality_config['min_view_count']:
            return False
        
        # 나이 검사
        creation_date = datetime.utcfromtimestamp(question.get('creation_date', 0))
        question_age = datetime.utcnow() - creation_date
        if question_age.days > collection_config['max_age_days']:
            return False
        
        # 필수 태그 검사
        tags = question.get('tags', [])
        if collection_config['required_tags']:
            if not any(req_tag in tags for req_tag in collection_config['required_tags']):
                return False
        
        # 제외 태그 검사
        if collection_config['excluded_tags']:
            if any(exc_tag in tags for exc_tag in collection_config['excluded_tags']):
                return False
        
        return True
    
    async def _extract_qa_from_question(self, question: Dict[str, Any]) -> List[QAEntry]:
        """질문에서 Q&A 추출"""
        qa_entries = []
        
        try:
            # 답변 조회
            answers = await self._fetch_answers_for_question(question['question_id'])
            
            for answer in answers:
                if not self._is_valid_answer(answer):
                    continue
                
                # Q&A 항목 생성
                qa_entry = self._create_qa_entry(question, answer)
                
                if qa_entry:
                    qa_entries.append(qa_entry)
                    
                    # 답변 추적
                    self.dedup_tracker.mark_stackoverflow_answer_collected(
                        answer['answer_id'],
                        question['question_id'],
                        answer.get('owner', {}).get('display_name', ''),
                        is_accepted=answer.get('is_accepted', False),
                        score=answer.get('score', 0),
                        quality_score=qa_entry.metadata.get('quality_score', 0.0),
                        metadata={'creation_date': answer.get('creation_date', 0)}
                    )
        
        except Exception as e:
            logger.error(f"Q&A 추출 중 오류: {e}")
            self.stats['api_errors'] += 1
        
        return qa_entries
    
    async def _fetch_answers_for_question(self, question_id: int) -> List[Dict[str, Any]]:
        """질문에 대한 답변 조회"""
        try:
            # 캐시 확인
            cache_key = f"answers_question_{question_id}"
            cached_data = self.cache.get_cached_response(cache_key)
            if cached_data:
                return cached_data
            
            # API 요청 파라미터
            params = {
                'sort': 'votes',
                'order': 'desc',
                'filter': 'withbody'
            }
            
            # API URL 생성
            url = self.config.build_api_url(f'questions/{question_id}/answers', params)
            
            # API 호출
            async with self.session.get(url) as response:
                if response.status == 200:
                    # gzip 압축 해제
                    if response.headers.get('content-encoding') == 'gzip':
                        data = gzip.decompress(await response.read())
                        result = json.loads(data.decode('utf-8'))
                    else:
                        result = await response.json()
                    
                    answers = result.get('items', [])
                    
                    # 캐시 저장
                    self.cache.cache_response(cache_key, answers)
                    
                    return answers
                
                elif response.status == 429:  # Rate limit
                    self.stats['rate_limit_hits'] += 1
                    logger.warning("StackOverflow API 요청 제한 도달")
                    await asyncio.sleep(5)
                    return []
                
                else:
                    logger.error(f"StackOverflow API 오류: {response.status}")
                    return []
        
        except Exception as e:
            logger.error(f"답변 조회 중 오류: {e}")
            return []
    
    def _is_valid_answer(self, answer: Dict[str, Any]) -> bool:
        """답변 유효성 검사"""
        quality_config = self.config.get_quality_config()
        
        # 답변 길이 검사
        body = answer.get('body', '')
        if len(body) < quality_config['min_answer_length']:
            return False
        
        if len(body) > quality_config['max_answer_length']:
            return False
        
        # 최소 점수 검사 (음수 점수 제외)
        if answer.get('score', 0) < 0:
            return False
        
        return True
    
    def _create_qa_entry(self, question: Dict[str, Any], answer: Dict[str, Any]) -> Optional[QAEntry]:
        """Q&A 항목 생성"""
        try:
            # 텍스트 정리
            user_question = clean_text(question.get('title', ''))
            user_context = clean_text(self._html_to_text(question.get('body', '')))
            assistant_response = clean_text(self._html_to_text(answer.get('body', '')))
            
            # 코드 블록 추출
            code_blocks = extract_code_blocks(assistant_response)
            
            # 메타데이터 생성
            metadata = {
                'difficulty': self._estimate_difficulty(user_question, user_context),
                'functions': self._extract_excel_functions(assistant_response),
                'quality_score': self._calculate_stackoverflow_quality_score(question, answer),
                'source': 'stackoverflow',
                'is_solved': answer.get('is_accepted', False),
                'stackoverflow_metadata': {
                    'question_id': question['question_id'],
                    'answer_id': answer['answer_id'],
                    'question_score': question.get('score', 0),
                    'answer_score': answer.get('score', 0),
                    'view_count': question.get('view_count', 0),
                    'answer_count': question.get('answer_count', 0),
                    'tags': question.get('tags', []),
                    'is_accepted': answer.get('is_accepted', False),
                    'creation_date': question.get('creation_date', 0),
                    'answer_date': answer.get('creation_date', 0)
                }
            }
            
            # 품질 점수 검사
            quality_config = self.config.get_quality_config()
            if metadata['quality_score'] < quality_config['min_quality_score']:
                self.stats['quality_failures'] += 1
                return None
            
            return QAEntry(
                id=generate_unique_id('stackoverflow_qa'),
                user_question=user_question,
                user_context=user_context,
                assistant_response=assistant_response,
                code_blocks=code_blocks,
                metadata=metadata
            )
        
        except Exception as e:
            logger.error(f"Q&A 항목 생성 중 오류: {e}")
            return None
    
    def _html_to_text(self, html: str) -> str:
        """HTML을 텍스트로 변환"""
        # 간단한 HTML 태그 제거
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'&[^;]+;', ' ', text)  # HTML 엔티티 제거
        text = re.sub(r'\s+', ' ', text)  # 연속된 공백 제거
        return text.strip()
    
    def _estimate_difficulty(self, question: str, context: str) -> str:
        """난이도 추정"""
        combined_text = f"{question} {context}".lower()
        
        # 고급 키워드
        advanced_keywords = ['vba', 'macro', 'array', 'regex', 'pivot', 'sql', 'power query']
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
            'TODAY', 'NOW', 'YEAR', 'MONTH', 'DAY', 'XLOOKUP', 'SUMIF', 'COUNTIF',
            'SUMPRODUCT', 'COUNTIFS', 'SUMIFS', 'IFERROR', 'ISERROR', 'ISBLANK'
        ]
        
        return [func for func in matches if func in excel_functions]
    
    def _calculate_stackoverflow_quality_score(self, question: Dict[str, Any], answer: Dict[str, Any]) -> float:
        """StackOverflow 품질 점수 계산"""
        quality_config = self.config.get_quality_config()
        
        score = 5.0  # 기본 점수
        
        # 질문 점수 반영
        question_score = question.get('score', 0)
        score += min(2.0, question_score / 10)
        
        # 답변 점수 반영
        answer_score = answer.get('score', 0)
        score += min(3.0, answer_score / 5)
        
        # 채택된 답변 보너스
        if answer.get('is_accepted', False):
            score += quality_config['accepted_answer_bonus']
        
        # 조회수 반영
        view_count = question.get('view_count', 0)
        score += min(1.0, view_count / 1000)
        
        # 답변 길이 반영
        answer_length = len(answer.get('body', ''))
        if answer_length > 500:
            score += 0.5
        
        return min(score, 10.0)  # 최대 10점
    
    def get_collection_stats(self) -> CollectionStats:
        """수집 통계 반환"""
        return CollectionStats(
            source='stackoverflow',
            total_collected=self.stats['total_collected'],
            total_skipped=self.stats['total_skipped'],
            collection_time_seconds=0.0,  # 실제 구현에서는 시간 측정
            quality_score_avg=0.0,  # 실제 구현에서는 평균 계산
            errors_count=self.stats['api_errors']
        )
    
    def get_detailed_stats(self) -> Dict[str, Any]:
        """상세 통계 반환"""
        return {
            **self.stats,
            'dedup_stats': self.dedup_tracker.get_stackoverflow_stats(),
            'cache_stats': self.cache.cache.get_stats()
        }