#!/usr/bin/env python3
"""
무한 스크롤 방식 웹 수집기
- Reddit 스타일 무한 스크롤 처리
- Stack Overflow 페이지네이션 + 무한 스크롤 하이브리드
- 더 많은 데이터 수집을 위한 고도화된 스크래핑
"""
import asyncio
import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin, parse_qs, urlparse

import httpx
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from core.cache import APICache, LocalCache
from core.dedup_tracker import get_global_tracker
from config import Config

logger = logging.getLogger('pipeline.infinite_scroll_collector')

class InfiniteScrollCollector:
    """
    무한 스크롤 + 페이지네이션 하이브리드 수집기
    - Stack Overflow: s-pagination 클래스 기반 페이지 순회
    - Reddit: 무한 스크롤 방식으로 더 많은 게시물 로드
    - 브라우저 자동화로 동적 콘텐츠 수집
    """
    
    def __init__(self, cache: APICache, headless: bool = True):
        self.cache = cache
        self.dedup_tracker = get_global_tracker()
        self.headless = headless
        
        # Chrome 옵션 설정
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument('--headless')
        
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--window-size=1920,1080')
        self.chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        self.driver = None
        self.collected_count = 0
        
        logger.info("InfiniteScrollCollector initialized")

    async def collect_stackoverflow_infinite(self, max_pages: int = 20, 
                                           max_questions_per_page: int = 20) -> List[Dict[str, Any]]:
        """
        Stack Overflow 무한 스크롤 + 페이지네이션 하이브리드 수집
        """
        logger.info(f"🌊 Stack Overflow 무한 스크롤 수집 시작 (최대 {max_pages}페이지)")
        
        self._initialize_driver()
        collected_qa_pairs = []
        
        try:
            base_url = "https://stackoverflow.com/questions/tagged/excel-formula?tab=newest"
            
            for page in range(1, max_pages + 1):
                logger.info(f"📄 페이지 {page} 처리 중...")
                
                # 페이지 URL 구성
                if page == 1:
                    page_url = base_url
                else:
                    page_url = f"{base_url}&page={page}"
                
                # 페이지 로드
                self.driver.get(page_url)
                await asyncio.sleep(2)  # 페이지 로드 대기
                
                # 무한 스크롤로 더 많은 질문 로드
                page_questions = await self._infinite_scroll_questions(max_questions_per_page)
                
                if not page_questions:
                    logger.info(f"페이지 {page}에서 질문을 찾을 수 없음")
                    break
                
                logger.info(f"📝 페이지 {page}에서 {len(page_questions)}개 질문 발견")
                
                # 각 질문의 상세 정보 수집
                for i, question_link in enumerate(page_questions, 1):
                    try:
                        logger.info(f"   질문 {i}/{len(page_questions)} 처리 중...")
                        
                        qa_pair = await self._scrape_question_with_selenium(question_link)
                        
                        if qa_pair and qa_pair.get('answer'):
                            # 중복 체크
                            question_id = qa_pair['question'].get('question_id')
                            if question_id:
                                if not any(existing['question'].get('question_id') == question_id 
                                          for existing in collected_qa_pairs):
                                    collected_qa_pairs.append(qa_pair)
                                    
                                    # 중복 추적기에 등록
                                    self.dedup_tracker.mark_stackoverflow_collected(
                                        question_id,
                                        qa_pair['question'].get('title', ''),
                                        quality_score=qa_pair.get('quality_score', 0),
                                        metadata={'page': page, 'source': 'infinite_scroll'}
                                    )
                        
                        # 요청 간격 조절
                        await asyncio.sleep(0.5)
                        
                    except Exception as e:
                        logger.warning(f"질문 {question_link} 처리 실패: {e}")
                        continue
                
                logger.info(f"✅ 페이지 {page} 완료: {len([p for p in collected_qa_pairs if p.get('answer')])}개 Q&A 수집")
                
                # 페이지 간 지연
                await asyncio.sleep(2)
            
            logger.info(f"🎉 무한 스크롤 수집 완료: {len(collected_qa_pairs)}개 Q&A 쌍")
            return collected_qa_pairs
            
        finally:
            self._close_driver()

    async def collect_reddit_infinite_scroll(self, subreddit: str = "excel", 
                                            max_posts: int = 100) -> List[Dict[str, Any]]:
        """
        Reddit 무한 스크롤 방식으로 게시물 수집
        """
        logger.info(f"🌊 Reddit r/{subreddit} 무한 스크롤 수집 시작 (최대 {max_posts}개)")
        
        self._initialize_driver()
        collected_posts = []
        
        try:
            # Reddit 페이지 로드
            reddit_url = f"https://www.reddit.com/r/{subreddit}/top/?t=month"
            self.driver.get(reddit_url)
            await asyncio.sleep(3)  # 페이지 로드 대기
            
            # 무한 스크롤로 게시물 수집
            seen_posts = set()
            scroll_attempts = 0
            max_scroll_attempts = max_posts // 10  # 대략적인 스크롤 횟수
            
            while len(collected_posts) < max_posts and scroll_attempts < max_scroll_attempts:
                logger.info(f"🔄 스크롤 {scroll_attempts + 1}/{max_scroll_attempts} (수집된 게시물: {len(collected_posts)}개)")
                
                # 현재 화면의 게시물들 수집
                current_posts = await self._extract_reddit_posts_from_page()
                
                new_posts_found = 0
                for post in current_posts:
                    post_id = post.get('id')
                    if post_id and post_id not in seen_posts:
                        seen_posts.add(post_id)
                        collected_posts.append(post)
                        new_posts_found += 1
                
                logger.info(f"   새로운 게시물 {new_posts_found}개 발견")
                
                # 스크롤 다운
                await self._scroll_down_reddit()
                
                # 새로운 콘텐츠 로딩 대기
                await asyncio.sleep(2)
                
                scroll_attempts += 1
                
                # 더 이상 새로운 게시물이 없으면 중단
                if new_posts_found == 0:
                    logger.info("더 이상 새로운 게시물이 없습니다.")
                    break
            
            logger.info(f"🎉 Reddit 무한 스크롤 수집 완료: {len(collected_posts)}개 게시물")
            return collected_posts
            
        finally:
            self._close_driver()

    def _initialize_driver(self):
        """Selenium WebDriver 초기화"""
        try:
            self.driver = webdriver.Chrome(options=self.chrome_options)
            logger.info("✅ WebDriver 초기화 완료")
        except Exception as e:
            logger.error(f"❌ WebDriver 초기화 실패: {e}")
            raise

    def _close_driver(self):
        """WebDriver 종료"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            logger.info("🔚 WebDriver 종료")

    async def _infinite_scroll_questions(self, max_questions: int) -> List[str]:
        """Stack Overflow 페이지에서 무한 스크롤로 질문 링크 수집"""
        question_links = []
        seen_links = set()
        
        try:
            # 초기 질문들 수집
            initial_questions = self.driver.find_elements(By.CSS_SELECTOR, '.s-post-summary .s-link')
            
            for element in initial_questions:
                href = element.get_attribute('href')
                if href and '/questions/' in href and href not in seen_links:
                    question_links.append(href)
                    seen_links.add(href)
            
            # 스크롤하여 더 많은 질문 로드 (일부 페이지에서는 동적 로딩)
            scroll_attempts = 3
            for i in range(scroll_attempts):
                # 페이지 끝까지 스크롤
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                await asyncio.sleep(1)
                
                # 새로운 질문들 찾기
                all_questions = self.driver.find_elements(By.CSS_SELECTOR, '.s-post-summary .s-link')
                
                for element in all_questions:
                    href = element.get_attribute('href')
                    if href and '/questions/' in href and href not in seen_links:
                        question_links.append(href)
                        seen_links.add(href)
                        
                        if len(question_links) >= max_questions:
                            break
                
                if len(question_links) >= max_questions:
                    break
            
            return question_links[:max_questions]
            
        except Exception as e:
            logger.error(f"무한 스크롤 질문 수집 실패: {e}")
            return question_links

    async def _extract_reddit_posts_from_page(self) -> List[Dict[str, Any]]:
        """현재 Reddit 페이지에서 게시물 정보 추출"""
        posts = []
        
        try:
            # Reddit 게시물 요소들 찾기
            post_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid="post-container"], .Post')
            
            for post_element in post_elements:
                try:
                    # 게시물 제목
                    title_element = post_element.find_element(By.CSS_SELECTOR, 'h3, [data-testid="post-content"] h3, .s1b4bwul')
                    title = title_element.text.strip() if title_element else ""
                    
                    # 게시물 링크
                    link_element = post_element.find_element(By.CSS_SELECTOR, 'a[data-testid="post-title"], a')
                    post_url = link_element.get_attribute('href') if link_element else ""
                    
                    # 게시물 ID 추출
                    post_id = ""
                    if post_url:
                        import re
                        id_match = re.search(r'/comments/([a-zA-Z0-9]+)/', post_url)
                        if id_match:
                            post_id = id_match.group(1)
                    
                    # 점수 (업보트)
                    score = 0
                    try:
                        score_element = post_element.find_element(By.CSS_SELECTOR, '[aria-label*="upvote"], .s1yr86ss')
                        score_text = score_element.text.strip()
                        if score_text.replace('k', '').replace('.', '').isdigit():
                            score = int(float(score_text.replace('k', '')) * (1000 if 'k' in score_text else 1))
                    except:
                        pass
                    
                    if title and post_id:
                        posts.append({
                            'id': post_id,
                            'title': title,
                            'url': post_url,
                            'score': score,
                            'source': 'reddit_infinite_scroll',
                            'collected_at': datetime.now().isoformat()
                        })
                
                except Exception as e:
                    logger.debug(f"개별 게시물 추출 실패: {e}")
                    continue
            
            return posts
            
        except Exception as e:
            logger.error(f"Reddit 게시물 추출 실패: {e}")
            return posts

    async def _scroll_down_reddit(self):
        """Reddit 페이지 무한 스크롤"""
        try:
            # 여러 가지 스크롤 방법 시도
            
            # 방법 1: 페이지 끝까지 스크롤
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            await asyncio.sleep(1)
            
            # 방법 2: 조금씩 스크롤 (더 자연스러운 로딩)
            for i in range(3):
                self.driver.execute_script("window.scrollBy(0, 800);")
                await asyncio.sleep(0.5)
            
            # 방법 3: "Load more" 버튼이 있다면 클릭
            try:
                load_more_button = self.driver.find_element(By.CSS_SELECTOR, '[data-testid="load-more"], .more')
                if load_more_button.is_displayed():
                    load_more_button.click()
                    await asyncio.sleep(2)
            except:
                pass
            
        except Exception as e:
            logger.debug(f"스크롤 실패: {e}")

    async def _scrape_question_with_selenium(self, question_url: str) -> Optional[Dict]:
        """Selenium으로 질문 상세 페이지 스크래핑"""
        try:
            self.driver.get(question_url)
            await asyncio.sleep(1)
            
            # 페이지 소스 가져와서 BeautifulSoup으로 파싱
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 기존 웹 스크래핑 로직 재사용
            question_data = self._extract_question_data_selenium(soup, question_url)
            if not question_data:
                return None
            
            answer_data = self._extract_best_answer_data_selenium(soup)
            if not answer_data:
                return None
            
            quality_score = question_data.get('score', 0) + answer_data.get('score', 0)
            
            return {
                'question': question_data,
                'answer': answer_data,
                'quality_score': quality_score,
                'source': 'selenium_scraping',
                'collected_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Selenium 질문 스크래핑 실패 {question_url}: {e}")
            return None

    def _extract_question_data_selenium(self, soup: BeautifulSoup, question_url: str) -> Optional[Dict]:
        """Selenium으로 질문 데이터 추출 (기존 로직과 동일)"""
        try:
            import re
            
            # 질문 ID 추출
            question_id_match = re.search(r'/questions/(\d+)/', question_url)
            question_id = int(question_id_match.group(1)) if question_id_match else None
            
            # 제목 추출
            title_element = soup.select_one('h1[itemprop="name"] a, h1 .question-hyperlink')
            title = title_element.get_text().strip() if title_element else ""
            
            # 질문 본문 추출
            question_body_element = soup.select_one('.s-prose.js-post-body')
            body_markdown = question_body_element.get_text().strip() if question_body_element else ""
            
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
                'is_answered': True,
                'link': question_url
            }
            
        except Exception as e:
            logger.error(f"Selenium 질문 데이터 추출 실패: {e}")
            return None

    def _extract_best_answer_data_selenium(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Selenium으로 답변 데이터 추출 (기존 로직과 동일)"""
        try:
            import re
            
            # 채택된 답변 찾기
            accepted_answer = soup.select_one('.answer.accepted-answer')
            
            if accepted_answer:
                return self._extract_answer_from_element_selenium(accepted_answer, is_accepted=True)
            
            # 채택된 답변이 없으면 가장 높은 점수 답변
            all_answers = soup.select('.answer')
            
            if not all_answers:
                return None
            
            best_answer = None
            best_score = -999
            
            for answer_element in all_answers:
                answer_data = self._extract_answer_from_element_selenium(answer_element, is_accepted=False)
                if answer_data and answer_data.get('score', 0) > best_score:
                    best_score = answer_data.get('score', 0)
                    best_answer = answer_data
            
            return best_answer if best_score >= 0 else None
            
        except Exception as e:
            logger.error(f"Selenium 답변 데이터 추출 실패: {e}")
            return None

    def _extract_answer_from_element_selenium(self, answer_element, is_accepted: bool = False) -> Optional[Dict]:
        """Selenium으로 개별 답변 추출 (기존 로직과 동일)"""
        try:
            import re
            
            # 답변 ID 추출
            answer_id = None
            id_attr = answer_element.get('id')
            if id_attr:
                id_match = re.search(r'answer-(\d+)', id_attr)
                if id_match:
                    answer_id = int(id_match.group(1))
            
            # 답변 본문 추출
            answer_body_element = answer_element.select_one('.s-prose.js-post-body')
            body_markdown = answer_body_element.get_text().strip() if answer_body_element else ""
            
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
            logger.error(f"Selenium 개별 답변 추출 실패: {e}")
            return None

    def get_collection_stats(self) -> Dict[str, Any]:
        """수집 통계"""
        return {
            'collected_count': self.collected_count,
            'collection_method': 'infinite_scroll',
            'driver_active': self.driver is not None
        }