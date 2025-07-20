"""
독립 Oppadu 수집기
Oppadu 전용 독립 시스템을 사용하는 완전히 분리된 수집기
"""
import asyncio
import logging
import time
import random
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
import json
import re
import sys
from pathlib import Path

# 상위 디렉토리 경로 추가
sys.path.append(str(Path(__file__).parent.parent.parent))

import aiohttp
import cloudscraper
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# 독립 시스템 import
from .oppadu_cache import OppaduCache, OppaduWebCache
from .oppadu_dedup_tracker import get_oppadu_tracker
from .oppadu_config import get_oppadu_config

# 공통 유틸리티 import
from shared.utils import generate_unique_id, calculate_quality_score, extract_code_blocks, clean_text
from shared.data_models import QAEntry, CollectionStats

logger = logging.getLogger('oppadu_system.collector')

class OppaduAntiDetection:
    """Oppadu 크롤링 방지 우회를 위한 고급 기술"""
    
    def __init__(self, config):
        self.config = config
        web_config = config.get_web_config()
        self.user_agents = web_config['user_agents']
        self.session_headers = self._generate_headers()
        
    def _generate_headers(self) -> Dict[str, str]:
        """한국 사용자를 모방한 헤더 생성"""
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Cache-Control': 'max-age=0'
        }
    
    def rotate_headers(self):
        """헤더 로테이션"""
        self.session_headers['User-Agent'] = random.choice(self.user_agents)
        
    async def human_delay(self, min_delay: float = 2.0, max_delay: float = 5.0):
        """인간적인 지연 시뮬레이션"""
        delay = random.uniform(min_delay, max_delay)
        await asyncio.sleep(delay)

class OppaduCollector:
    """독립 Oppadu 수집기"""
    
    def __init__(self):
        """Oppadu 수집기 초기화"""
        self.config = get_oppadu_config()
        self.dedup_tracker = get_oppadu_tracker()
        
        # 독립 캐시 시스템 초기화
        oppadu_cache = OppaduCache(self.config.cache_db_path)
        self.cache = OppaduWebCache(oppadu_cache)
        
        # 크롤링 방지 우회 시스템
        self.anti_detection = OppaduAntiDetection(self.config)
        
        # CloudScraper 설정
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        
        # Selenium 드라이버 (필요시 사용)
        self.driver = None
        
        # 수집 통계
        self.stats = {
            'total_processed': 0,
            'total_collected': 0,
            'total_skipped': 0,
            'duplicate_posts': 0,
            'quality_failures': 0,
            'crawling_errors': 0,
            'blocked_requests': 0
        }
        
        logger.info("독립 Oppadu 수집기 초기화 완료")
    
    async def collect_excel_qa_data(self, max_items: int = 100) -> List[QAEntry]:
        """Excel Q&A 데이터 수집 (개선된 페이지네이션 지원)"""
        logger.info(f"Oppadu Excel Q&A 데이터 수집 시작 (최대 {max_items}개)")
        
        collected_data = []
        collection_config = self.config.get_collection_config()
        
        try:
            # 첫 페이지부터 시작
            page_urls = await self._get_all_page_urls()
            max_pages = min(len(page_urls), collection_config['max_pages'])
            
            logger.info(f"총 {len(page_urls)}개 페이지 발견, 최대 {max_pages}개 페이지 처리")
            
            for page_num, page_url in enumerate(page_urls[:max_pages], 1):
                if len(collected_data) >= max_items:
                    break
                
                logger.info(f"페이지 {page_num}/{max_pages} 처리 중: {page_url}")
                
                # 답변 완료된 게시글 목록 수집
                answered_posts = await self._get_answered_posts_from_url(page_url)
                
                if not answered_posts:
                    logger.info(f"페이지 {page_num}에서 더 이상 답변 완료된 게시글이 없음")
                    continue
                
                logger.info(f"🎯 {len(answered_posts)}개의 답변 완료 게시글 발견")
                
                # 각 게시글의 상세 데이터 수집
                for post_url in answered_posts:
                    if len(collected_data) >= max_items:
                        break
                    
                    try:
                        self.stats['total_processed'] += 1
                        
                        # 중복 체크
                        post_id = self._extract_post_id(post_url)
                        if self.dedup_tracker.is_oppadu_post_collected(post_id):
                            logger.debug(f"이미 수집된 게시글 건너뜀: {post_id}")
                            self.stats['duplicate_posts'] += 1
                            continue
                        
                        # 상세 데이터 수집
                        qa_entry = await self._scrape_post_detail(post_url)
                        
                        if qa_entry:
                            collected_data.append(qa_entry)
                            self.stats['total_collected'] += 1
                            
                            # 수집된 게시글 추적
                            self.dedup_tracker.mark_oppadu_post_collected(
                                post_id,
                                qa_entry.user_question,
                                post_url,
                                quality_score=qa_entry.metadata.get('quality_score', 0.0),
                                has_answer=bool(qa_entry.assistant_response),
                                metadata={'page': page_num, 'collection_date': datetime.now().isoformat()}
                            )
                            
                            logger.info(f"✅ 게시글 수집 완료: {qa_entry.user_question[:50]}...")
                        else:
                            self.stats['total_skipped'] += 1
                        
                        # 인간적인 지연
                        crawling_config = self.config.get_crawling_config()
                        await self.anti_detection.human_delay(
                            crawling_config['human_delay_min'],
                            crawling_config['human_delay_max']
                        )
                        
                    except Exception as e:
                        logger.error(f"게시글 처리 중 오류: {post_url} - {e}")
                        self.stats['crawling_errors'] += 1
                        continue
                
                # 페이지 간 지연
                crawling_config = self.config.get_crawling_config()
                await self.anti_detection.human_delay(
                    crawling_config['page_delay_min'],
                    crawling_config['page_delay_max']
                )
        
        except Exception as e:
            logger.error(f"Oppadu 데이터 수집 중 오류: {e}")
            self.stats['crawling_errors'] += 1
        
        finally:
            if self.driver:
                self.driver.quit()
        
        logger.info(f"Oppadu 수집 완료: {len(collected_data)}개 항목")
        return collected_data
    
    async def _get_all_page_urls(self) -> List[str]:
        """모든 페이지 URL 목록 수집 (class="oppadu-pagination" 사용)"""
        try:
            # 첫 페이지에서 페이지네이션 정보 수집
            first_page_url = self.config.get_web_config()['community_url']
            
            # 1차 시도: CloudScraper
            response = await self._fetch_with_cloudscraper(first_page_url)
            if not response:
                # 2차 시도: Selenium
                response = await self._fetch_with_selenium_for_pagination(first_page_url)
            
            if not response:
                logger.warning("첫 페이지를 가져올 수 없음")
                return [first_page_url]
            
            # 페이지네이션 파싱
            page_urls = self._parse_pagination(response)
            
            if not page_urls:
                # 페이지네이션이 없으면 첫 페이지만 반환
                return [first_page_url]
            
            logger.info(f"총 {len(page_urls)}개 페이지 URL 발견")
            return page_urls
            
        except Exception as e:
            logger.error(f"페이지 URL 목록 수집 실패: {e}")
            return [self.config.get_web_config()['community_url']]
    
    async def _get_answered_posts_from_url(self, page_url: str) -> List[str]:
        """특정 페이지 URL에서 답변 완료된 게시글 URL 목록 수집"""
        try:
            # 1차 시도: CloudScraper
            response = await self._fetch_with_cloudscraper(page_url)
            if response:
                return self._parse_answered_posts(response)
            
            # 2차 시도: Selenium (JavaScript 렌더링 필요한 경우)
            return await self._fetch_with_selenium_for_posts(page_url)
            
        except Exception as e:
            logger.error(f"답변 완료 게시글 목록 수집 실패: {e}")
            return []
    
    async def _fetch_with_cloudscraper(self, url: str) -> Optional[str]:
        """CloudScraper를 사용한 페이지 수집"""
        try:
            # 캐시 확인
            cached_content = self.cache.get_cached_page(url)
            if cached_content:
                return cached_content
            
            # 헤더 로테이션
            self.anti_detection.rotate_headers()
            self.scraper.headers.update(self.anti_detection.session_headers)
            
            # Referer 설정 (자연스러운 탐색 시뮬레이션)
            if 'page=' in url:
                web_config = self.config.get_web_config()
                self.scraper.headers['Referer'] = web_config['community_url']
            
            response = self.scraper.get(url, timeout=30)
            response.raise_for_status()
            
            # 캐시 저장
            self.cache.cache_page(url, response.text)
            
            logger.debug(f"CloudScraper 성공: {url}")
            return response.text
            
        except Exception as e:
            logger.warning(f"CloudScraper 실패: {e}")
            self.stats['blocked_requests'] += 1
            return None
    
    def _parse_pagination(self, html_content: str) -> List[str]:
        """HTML에서 페이지네이션 URL 목록 추출 (class="oppadu-pagination")"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # class="oppadu-pagination" 컨테이너 찾기
            pagination = soup.find(class_='oppadu-pagination')
            if not pagination:
                logger.warning("oppadu-pagination 컨테이너를 찾을 수 없음")
                return []
            
            page_urls = []
            web_config = self.config.get_web_config()
            
            # class="page-number" 각 페이지 링크 추출
            page_links = pagination.find_all(class_='page-number')
            
            for link in page_links:
                href = link.get('href')
                if href:
                    if href.startswith('?'):
                        page_url = web_config['community_url'] + href
                    elif href.startswith('/'):
                        page_url = web_config['base_url'] + href
                    else:
                        page_url = urljoin(web_config['community_url'], href)
                    
                    page_urls.append(page_url)
            
            # 중복 제거 및 정렬
            unique_urls = list(dict.fromkeys(page_urls))  # 순서 유지하면서 중복 제거
            
            logger.debug(f"페이지네이션에서 {len(unique_urls)}개 페이지 URL 추출")
            return unique_urls
            
        except Exception as e:
            logger.error(f"페이지네이션 파싱 중 오류: {e}")
            return []
    
    async def _fetch_with_selenium_for_pagination(self, url: str) -> Optional[str]:
        """Selenium을 사용한 페이지네이션 수집"""
        try:
            if not self.driver:
                self._init_selenium_driver()
            
            self.driver.get(url)
            
            # 페이지네이션 로딩 대기
            WebDriverWait(self.driver, 10).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CLASS_NAME, "oppadu-pagination")),
                    EC.presence_of_element_located((By.CLASS_NAME, "post-list-modern"))
                )
            )
            
            await asyncio.sleep(2)
            page_source = self.driver.page_source
            
            # 캐시 저장
            self.cache.cache_page(url, page_source)
            
            return page_source
            
        except Exception as e:
            logger.error(f"Selenium 페이지네이션 수집 실패: {e}")
            return None
    
    async def _fetch_with_selenium_for_posts(self, url: str) -> List[str]:
        """Selenium을 사용한 게시글 목록 수집"""
        try:
            if not self.driver:
                self._init_selenium_driver()
            
            self.driver.get(url)
            
            # 게시글 목록 로딩 대기
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "post-list-modern"))
            )
            
            # 스크롤 다운 (레이지 로딩 대응)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            await asyncio.sleep(2)
            
            page_source = self.driver.page_source
            
            # 캐시 저장
            self.cache.cache_page(url, page_source)
            
            return self._parse_answered_posts(page_source)
            
        except Exception as e:
            logger.error(f"Selenium 게시글 목록 수집 실패: {e}")
            self.stats['blocked_requests'] += 1
            return []
    
    def _init_selenium_driver(self):
        """Selenium 드라이버 초기화"""
        try:
            selenium_config = self.config.get_selenium_config()
            
            options = uc.ChromeOptions()
            for option in selenium_config['chrome_options']:
                options.add_argument(option)
            
            options.add_argument(f'--window-size={selenium_config["window_size"]}')
            options.add_argument('--user-agent=' + random.choice(self.anti_detection.user_agents))
            
            if selenium_config['headless_mode']:
                options.add_argument('--headless')
            
            # 한국 언어 설정
            options.add_experimental_option('prefs', {
                'intl.accept_languages': 'ko-KR,ko,en'
            })
            
            self.driver = uc.Chrome(options=options)
            
        except Exception as e:
            logger.error(f"Selenium 드라이버 초기화 실패: {e}")
            raise
    
    def _parse_answered_posts(self, html_content: str) -> List[str]:
        """HTML에서 답변 완료된 게시글 URL 추출 (사용자 제공 정확한 구조)"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # class="post-list-modern" 컨테이너 찾기
            post_list = soup.find(class_='post-list-modern')
            if not post_list:
                logger.warning("post-list-modern 컨테이너를 찾을 수 없음")
                return []
            
            answered_posts = []
            
            # 게시물 목록에서 각 게시물 항목 찾기
            post_items = post_list.find_all('div', recursive=True)
            logger.debug(f"총 {len(post_items)}개의 게시글 항목 검사")
            
            for item in post_items:
                # class="answer-complete-badge"가 있는지 확인
                answer_badge = item.find(class_='answer-complete-badge')
                if answer_badge:
                    # 게시글 링크 추출 (가장 가까운 링크 찾기)
                    link_element = (
                        item.find('a', href=True) or 
                        item.find_parent().find('a', href=True) if item.find_parent() else None
                    )
                    
                    if link_element:
                        href = link_element['href']
                        
                        # 올바른 URL 구성
                        web_config = self.config.get_web_config()
                        if href.startswith('?'):
                            post_url = web_config['community_url'] + href
                        elif href.startswith('/'):
                            post_url = web_config['base_url'] + href
                        else:
                            post_url = urljoin(web_config['community_url'], href)
                        
                        answered_posts.append(post_url)
                        logger.debug(f"답변 완료 게시글 발견: {post_url}")
            
            logger.info(f"답변 완료된 게시글 {len(answered_posts)}개 발견")
            return answered_posts
            
        except Exception as e:
            logger.error(f"HTML 파싱 중 오류: {e}")
            return []
    
    async def _scrape_post_detail(self, post_url: str) -> Optional[QAEntry]:
        """게시글 상세 페이지에서 데이터 추출"""
        try:
            # 상세 페이지 수집
            html_content = await self._fetch_with_cloudscraper(post_url)
            if not html_content and self.driver:
                self.driver.get(post_url)
                await asyncio.sleep(3)
                html_content = self.driver.page_source
            
            if not html_content:
                logger.error(f"상세 페이지 수집 실패: {post_url}")
                return None
            
            return self._parse_post_detail(html_content, post_url)
            
        except Exception as e:
            logger.error(f"게시글 상세 수집 중 오류: {e}")
            return None
    
    def _parse_post_detail(self, html_content: str, post_url: str) -> Optional[QAEntry]:
        """게시글 상세 페이지 HTML 파싱 (사용자 제공 정확한 구조)"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 제목 추출
            title_element = soup.find('h1') or soup.find(class_='post-title')
            title = title_element.get_text(strip=True) if title_element else ""
            
            # 질문 내용 추출 (class="post-content")
            post_content = soup.find(class_='post-content')
            question_text = ""
            question_images = []
            
            if post_content:
                # 텍스트 추출
                question_text = clean_text(post_content.get_text(strip=True, separator=' '))
                
                # 이미지 추출
                question_images = self._extract_images_from_element(post_content, post_url)
            
            # 답변 내용 추출 (id="comment-list" 내 class="selected-answer")
            answer_text = ""
            answer_images = []
            
            comment_list = soup.find(id='comment-list')
            if comment_list:
                selected_answer = comment_list.find(class_='selected-answer')
                if selected_answer:
                    # 답변 텍스트 추출
                    answer_text = clean_text(selected_answer.get_text(strip=True, separator=' '))
                    
                    # 답변 이미지 추출
                    answer_images = self._extract_images_from_element(selected_answer, post_url)
            
            # 유효성 검사
            if not self._is_valid_post_content(title, question_text, answer_text):
                return None
            
            # 이미지 텍스트 통합
            all_question_text = question_text
            all_answer_text = answer_text
            
            if question_images:
                image_texts = [img.get('extracted_text', '') for img in question_images if img.get('extracted_text')]
                if image_texts:
                    all_question_text += ' [이미지 내용: ' + ' '.join(image_texts) + ']'
            
            if answer_images:
                image_texts = [img.get('extracted_text', '') for img in answer_images if img.get('extracted_text')]
                if image_texts:
                    all_answer_text += ' [이미지 내용: ' + ' '.join(image_texts) + ']'
            
            # 메타데이터 생성
            metadata = {
                'difficulty': self._estimate_difficulty(title, all_question_text),
                'functions': self._extract_excel_functions(all_answer_text),
                'quality_score': self._calculate_oppadu_quality_score(title, all_question_text, all_answer_text),
                'source': 'oppadu',
                'is_solved': bool(answer_text),
                'oppadu_metadata': {
                    'post_id': self._extract_post_id(post_url),
                    'url': post_url,
                    'title': title,
                    'has_answer': bool(answer_text),
                    'collection_date': datetime.now().isoformat(),
                    'question_images': question_images,
                    'answer_images': answer_images
                }
            }
            
            # 품질 점수 검사
            quality_config = self.config.get_quality_config()
            if metadata['quality_score'] < quality_config['min_quality_score']:
                self.stats['quality_failures'] += 1
                return None
            
            # 코드 블록 추출
            code_blocks = extract_code_blocks(f"{all_question_text} {all_answer_text}")
            
            return QAEntry(
                id=generate_unique_id('oppadu_qa'),
                user_question=title,
                user_context=all_question_text,
                assistant_response=all_answer_text,
                code_blocks=code_blocks,
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"게시글 상세 파싱 중 오류: {e}")
            return None
    
    def _extract_images_from_element(self, element, base_url: str) -> List[Dict[str, Any]]:
        """HTML 요소에서 이미지 추출 및 처리"""
        try:
            images = []
            img_tags = element.find_all('img')
            
            for img in img_tags:
                src = img.get('src')
                if src:
                    # 상대 URL을 절대 URL로 변환
                    if src.startswith('/'):
                        web_config = self.config.get_web_config()
                        image_url = web_config['base_url'] + src
                    elif src.startswith('http'):
                        image_url = src
                    else:
                        image_url = urljoin(base_url, src)
                    
                    # 이미지 정보 생성
                    image_info = {
                        'url': image_url,
                        'alt_text': img.get('alt', ''),
                        'title': img.get('title', ''),
                        'width': img.get('width', ''),
                        'height': img.get('height', ''),
                        'extracted_text': ''  # OCR 처리는 향후 구현
                    }
                    
                    images.append(image_info)
            
            return images
            
        except Exception as e:
            logger.error(f"이미지 추출 중 오류: {e}")
            return []
    
    def _is_valid_post_content(self, title: str, question: str, answer: str) -> bool:
        """게시물 내용 유효성 검사"""
        collection_config = self.config.get_collection_config()
        
        # 제목 길이 검사
        if len(title) < collection_config['min_title_length']:
            return False
        
        # 질문 길이 검사
        if len(question) < collection_config['min_content_length']:
            return False
        
        # 답변이 있는지 검사
        if collection_config['only_answered_posts'] and not answer:
            return False
        
        # 키워드 검사
        combined_text = f"{title} {question} {answer}".lower()
        
        # 필수 키워드 검사
        if collection_config['required_keywords']:
            if not any(keyword in combined_text for keyword in collection_config['required_keywords']):
                return False
        
        # 제외 키워드 검사
        if collection_config['excluded_keywords']:
            if any(keyword in combined_text for keyword in collection_config['excluded_keywords']):
                return False
        
        return True
    
    def _estimate_difficulty(self, title: str, content: str) -> str:
        """난이도 추정"""
        combined_text = f"{title} {content}".lower()
        
        # 고급 키워드 (한국어)
        advanced_keywords = ['vba', '매크로', '피벗', '배열', '수식', '함수', '조건부서식']
        advanced_count = sum(1 for keyword in advanced_keywords if keyword in combined_text)
        
        if advanced_count >= 2:
            return 'advanced'
        elif advanced_count >= 1:
            return 'intermediate'
        else:
            return 'beginner'
    
    def _extract_excel_functions(self, text: str) -> List[str]:
        """Excel 함수 추출 (한국어 포함)"""
        functions = []
        
        # 영어 함수 패턴
        function_pattern = r'([A-Z][A-Z0-9_]*)\s*\('
        matches = re.findall(function_pattern, text.upper())
        functions.extend(matches)
        
        # 한국어 함수명 매핑
        korean_functions = {
            '합계': 'SUM',
            '평균': 'AVERAGE',
            '개수': 'COUNT',
            '최대': 'MAX',
            '최소': 'MIN',
            '조건': 'IF',
            '찾기': 'VLOOKUP',
            '연결': 'CONCATENATE'
        }
        
        for korean, english in korean_functions.items():
            if korean in text:
                functions.append(english)
        
        return list(set(functions))
    
    def _calculate_oppadu_quality_score(self, title: str, question: str, answer: str) -> float:
        """Oppadu 품질 점수 계산"""
        quality_config = self.config.get_quality_config()
        
        score = 5.0  # 기본 점수
        
        # 제목 길이 점수
        if len(title) >= 20:
            score += 0.5
        
        # 질문 길이 점수
        question_length = len(question)
        if question_length >= 100:
            score += 1.0
        elif question_length >= 50:
            score += 0.5
        
        # 답변 길이 점수
        answer_length = len(answer)
        if answer_length >= 100:
            score += 1.5
        elif answer_length >= 50:
            score += 1.0
        
        # 한국어 콘텐츠 가중치
        korean_content_weight = quality_config['korean_content_weight']
        score *= korean_content_weight
        
        # 코드나 수식 포함 시 가산점
        if self._contains_excel_content(f"{question} {answer}"):
            score += 1.0
        
        return min(score, 10.0)  # 최대 10점
    
    def _contains_excel_content(self, text: str) -> bool:
        """Excel 관련 내용 포함 여부 확인"""
        excel_indicators = [
            '=', 'SUM', 'IF', 'VLOOKUP', 'INDEX', 'MATCH',
            '합계', '평균', '개수', '조건', '찾기', '수식', '함수',
            '셀', '워크시트', '엑셀', '피벗'
        ]
        
        text_upper = text.upper()
        return any(indicator.upper() in text_upper for indicator in excel_indicators)
    
    def _extract_post_id(self, url: str) -> str:
        """URL에서 게시글 ID 추출"""
        try:
            # uid 파라미터 추출
            if 'uid=' in url:
                uid_match = re.search(r'uid=(\d+)', url)
                if uid_match:
                    return uid_match.group(1)
            
            # URL 경로에서 ID 추출
            parts = url.split('/')
            for part in reversed(parts):
                if part.isdigit():
                    return part
            
            # 숫자 ID가 없으면 URL 해시 사용
            return str(hash(url))
        except:
            return str(hash(url))
    
    def get_collection_stats(self) -> CollectionStats:
        """수집 통계 반환"""
        return CollectionStats(
            source='oppadu',
            total_collected=self.stats['total_collected'],
            total_skipped=self.stats['total_skipped'],
            collection_time_seconds=0.0,  # 실제 구현에서는 시간 측정
            quality_score_avg=0.0,  # 실제 구현에서는 평균 계산
            errors_count=self.stats['crawling_errors']
        )
    
    def get_detailed_stats(self) -> Dict[str, Any]:
        """상세 통계 반환"""
        return {
            **self.stats,
            'dedup_stats': self.dedup_tracker.get_oppadu_stats(),
            'cache_stats': self.cache.cache.get_stats()
        }