#!/usr/bin/env python3
"""
웹 스크래핑 기반 Stack Overflow 수집기
- s-pagination 클래스 기반 페이지 순회
- 더 많은 데이터 확보를 위한 웹 스크래핑
- API 제한 없이 대량 데이터 수집
"""
import asyncio
import logging
import time
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin, parse_qs, urlparse
import json

import httpx
from bs4 import BeautifulSoup
import backoff

from core.cache import APICache, LocalCache
from core.dedup_tracker import get_global_tracker
from config import Config

logger = logging.getLogger('pipeline.web_scraping_stackoverflow')

class WebScrapingStackOverflowCollector:
    """
    웹 스크래핑 기반 Stack Overflow 수집기
    - s-pagination 클래스로 페이지 순회
    - 질문 목록 페이지 스크래핑
    - 개별 질문/답변 페이지 스크래핑
    """
    
    def __init__(self, cache: APICache):
        self.cache = cache
        self.dedup_tracker = get_global_tracker()
        
        # HTTP 클라이언트 설정 (브라우저처럼 보이게)
        self.client = httpx.AsyncClient(
            timeout=30,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            },
            follow_redirects=True
        )
        
        self.base_url = "https://stackoverflow.com"
        self.collected_count = 0
        self.last_request_time = 0
        
        logger.info("WebScrapingStackOverflowCollector initialized")

    async def collect_excel_questions_web(self, max_pages: int = 50) -> List[Dict[str, Any]]:
        """
        웹 스크래핑으로 Excel 질문 수집
        - 페이지네이션 자동 순회
        - 질문별 상세 정보 수집
        """
        logger.info(f"🌐 웹 스크래핑 수집 시작 (최대 {max_pages}페이지)")
        
        collected_qa_pairs = []
        current_page = 1
        
        # Excel 관련 태그 검색 URL
        base_search_url = f"{self.base_url}/questions/tagged/excel-formula"
        
        while current_page <= max_pages:
            try:
                logger.info(f"📄 페이지 {current_page} 처리 중...")
                
                # 페이지 URL 구성
                if current_page == 1:
                    page_url = base_search_url
                else:
                    page_url = f"{base_search_url}?tab=newest&page={current_page}"
                
                # 질문 목록 페이지 스크래핑
                question_links = await self._scrape_question_list_page(page_url)
                
                if not question_links:
                    logger.info(f"페이지 {current_page}에서 질문을 찾을 수 없음. 수집 종료.")
                    break
                
                logger.info(f"📝 페이지 {current_page}에서 {len(question_links)}개 질문 발견")
                
                # 각 질문의 상세 정보 수집
                page_qa_pairs = []
                for i, question_link in enumerate(question_links[:10], 1):  # 페이지당 최대 10개로 제한
                    try:
                        logger.info(f"   질문 {i}/{len(question_links[:10])} 처리 중...")
                        
                        qa_pair = await self._scrape_question_detail(question_link)
                        
                        if qa_pair and qa_pair.get('answer'):
                            # 중복 체크
                            question_id = qa_pair['question'].get('question_id')
                            if question_id:
                                # 간단한 중복 체크 (ID 기반)
                                if not any(existing['question'].get('question_id') == question_id 
                                          for existing in collected_qa_pairs):
                                    page_qa_pairs.append(qa_pair)
                                    
                                    # 중복 추적기에 등록
                                    self.dedup_tracker.mark_stackoverflow_collected(
                                        question_id, 
                                        qa_pair['question'].get('title', ''),
                                        quality_score=qa_pair.get('quality_score', 0),
                                        metadata={'page': current_page, 'source': 'web_scraping'}
                                    )
                        
                        # 요청 간격 조절 (서버 부하 방지)
                        await self._polite_delay()
                        
                    except Exception as e:
                        logger.warning(f"질문 {question_link} 처리 실패: {e}")
                        continue
                
                collected_qa_pairs.extend(page_qa_pairs)
                logger.info(f"✅ 페이지 {current_page} 완료: {len(page_qa_pairs)}개 Q&A 수집")
                
                # 다음 페이지 존재 여부 확인
                has_next = await self._check_next_page_exists(page_url)
                if not has_next:
                    logger.info("마지막 페이지에 도달했습니다.")
                    break
                
                current_page += 1
                
                # 페이지 간 지연
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"페이지 {current_page} 처리 오류: {e}")
                break
        
        logger.info(f"🎉 웹 스크래핑 수집 완료: {len(collected_qa_pairs)}개 Q&A 쌍")
        return collected_qa_pairs

    async def _scrape_question_list_page(self, page_url: str) -> List[str]:
        """질문 목록 페이지에서 질문 링크들 추출"""
        try:
            response = await self.client.get(page_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 질문 링크 추출 (.s-post-summary 클래스 내의 제목 링크)
            question_links = []
            
            # Stack Overflow의 질문 제목 링크 선택자
            question_elements = soup.select('.s-post-summary .s-link')
            
            for element in question_elements:
                href = element.get('href')
                if href and '/questions/' in href:
                    full_url = urljoin(self.base_url, href)
                    question_links.append(full_url)
            
            # 중복 제거
            question_links = list(dict.fromkeys(question_links))
            
            return question_links
            
        except Exception as e:
            logger.error(f"질문 목록 페이지 스크래핑 실패 {page_url}: {e}")
            return []

    async def _scrape_question_detail(self, question_url: str) -> Optional[Dict]:
        """개별 질문 페이지에서 질문과 답변 상세 정보 추출"""
        try:
            response = await self.client.get(question_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 질문 정보 추출
            question_data = self._extract_question_data(soup, question_url)
            if not question_data:
                return None
            
            # 답변 정보 추출 (채택된 답변 또는 가장 높은 점수의 답변)
            answer_data = self._extract_best_answer_data(soup)
            
            if not answer_data:
                return None  # 답변이 없는 질문은 제외
            
            # 품질 점수 계산
            quality_score = question_data.get('score', 0) + answer_data.get('score', 0)
            
            return {
                'question': question_data,
                'answer': answer_data,
                'quality_score': quality_score,
                'source': 'web_scraping',
                'collected_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"질문 상세 페이지 스크래핑 실패 {question_url}: {e}")
            return None

    def _extract_question_data(self, soup: BeautifulSoup, question_url: str) -> Optional[Dict]:
        """질문 데이터 추출"""
        try:
            # 질문 ID 추출 (URL에서)
            question_id_match = re.search(r'/questions/(\d+)/', question_url)
            question_id = int(question_id_match.group(1)) if question_id_match else None
            
            # 제목 추출
            title_element = soup.select_one('h1[itemprop="name"] a, h1 .question-hyperlink')
            title = title_element.get_text().strip() if title_element else ""
            
            # 질문 본문 추출
            question_body_element = soup.select_one('.s-prose.js-post-body')
            body_markdown = ""
            if question_body_element:
                # HTML을 마크다운 스타일로 변환 (간단히)
                body_markdown = question_body_element.get_text().strip()
            
            # 점수 추출
            score_element = soup.select_one('.js-vote-count')
            score = 0
            if score_element:
                try:
                    score = int(score_element.get_text().strip())
                except ValueError:
                    score = 0
            
            # 조회수 추출
            view_count = 0
            view_element = soup.select_one('[title*="viewed"], .fs-body1')
            if view_element:
                view_text = view_element.get_text()
                view_match = re.search(r'(\d+)', view_text.replace(',', ''))
                if view_match:
                    view_count = int(view_match.group(1))
            
            # 태그 추출
            tags = []
            tag_elements = soup.select('.post-tag, .s-tag')
            for tag_element in tag_elements:
                tag_text = tag_element.get_text().strip()
                if tag_text:
                    tags.append(tag_text)
            
            return {
                'question_id': question_id,
                'title': title,
                'body_markdown': body_markdown,
                'score': score,
                'view_count': view_count,
                'tags': tags,
                'is_answered': True,  # 답변이 있는 것만 처리하므로
                'link': question_url
            }
            
        except Exception as e:
            logger.error(f"질문 데이터 추출 실패: {e}")
            return None

    def _extract_best_answer_data(self, soup: BeautifulSoup) -> Optional[Dict]:
        """가장 좋은 답변 데이터 추출 (채택된 답변 우선, 없으면 높은 점수)"""
        try:
            # 1. 채택된 답변 찾기
            accepted_answer = soup.select_one('.answer.accepted-answer')
            
            if accepted_answer:
                return self._extract_answer_from_element(accepted_answer, is_accepted=True)
            
            # 2. 채택된 답변이 없으면 모든 답변 중 가장 높은 점수
            all_answers = soup.select('.answer')
            
            if not all_answers:
                return None
            
            best_answer = None
            best_score = -999
            
            for answer_element in all_answers:
                answer_data = self._extract_answer_from_element(answer_element, is_accepted=False)
                if answer_data and answer_data.get('score', 0) > best_score:
                    best_score = answer_data.get('score', 0)
                    best_answer = answer_data
            
            return best_answer if best_score >= 0 else None  # 음수 점수는 제외
            
        except Exception as e:
            logger.error(f"답변 데이터 추출 실패: {e}")
            return None

    def _extract_answer_from_element(self, answer_element, is_accepted: bool = False) -> Optional[Dict]:
        """답변 요소에서 답변 데이터 추출"""
        try:
            # 답변 ID 추출
            answer_id = None
            id_attr = answer_element.get('id')
            if id_attr:
                id_match = re.search(r'answer-(\d+)', id_attr)
                if id_match:
                    answer_id = int(id_match.group(1))
            
            # 답변 본문 추출
            answer_body_element = answer_element.select_one('.s-prose.js-post-body')
            body_markdown = ""
            if answer_body_element:
                body_markdown = answer_body_element.get_text().strip()
            
            # 답변 점수 추출
            score_element = answer_element.select_one('.js-vote-count')
            score = 0
            if score_element:
                try:
                    score = int(score_element.get_text().strip())
                except ValueError:
                    score = 0
            
            return {
                'answer_id': answer_id,
                'body_markdown': body_markdown,
                'score': score,
                'is_accepted': is_accepted
            }
            
        except Exception as e:
            logger.error(f"개별 답변 추출 실패: {e}")
            return None

    async def _check_next_page_exists(self, current_page_url: str) -> bool:
        """다음 페이지 존재 여부 확인"""
        try:
            response = await self.client.get(current_page_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # s-pagination 클래스에서 다음 페이지 링크 찾기
            pagination = soup.select_one('.s-pagination')
            if pagination:
                next_link = pagination.select_one('a[rel="next"], a:contains("Next")')
                return next_link is not None
            
            return False
            
        except Exception as e:
            logger.error(f"다음 페이지 확인 실패: {e}")
            return False

    async def _polite_delay(self):
        """서버에 부담을 주지 않는 정중한 지연"""
        min_delay = 1.0  # 최소 1초 지연
        elapsed = time.time() - self.last_request_time
        
        if elapsed < min_delay:
            delay = min_delay - elapsed
            await asyncio.sleep(delay)
        
        self.last_request_time = time.time()

    async def close(self):
        """HTTP 클라이언트 정리"""
        await self.client.aclose()

    def get_collection_stats(self) -> Dict[str, Any]:
        """수집 통계"""
        return {
            'collected_count': self.collected_count,
            'last_request_time': datetime.fromtimestamp(self.last_request_time) if self.last_request_time else None,
            'collection_method': 'web_scraping'
        }