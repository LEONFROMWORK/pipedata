"""
오빠두(oppadu.com) 웹 크롤러
한국 Excel 커뮤니티 데이터 수집을 위한 고급 웹 크롤링 시스템
"""

import asyncio
import logging
import time
import random
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
import json
import re

import aiohttp
import cloudscraper
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

# Selenium imports - optional for environments without Chrome
SELENIUM_ENABLED = os.getenv('SELENIUM_ENABLED', 'false').lower() == 'true'

if SELENIUM_ENABLED:
    try:
        import undetected_chromedriver as uc
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.action_chains import ActionChains
        from selenium.common.exceptions import TimeoutException, NoSuchElementException
        SELENIUM_AVAILABLE = True
    except ImportError:
        SELENIUM_AVAILABLE = False
        logging.warning("Selenium/undetected_chromedriver not available. Some features will be limited.")
else:
    SELENIUM_AVAILABLE = False
    logging.info("Selenium disabled by environment variable SELENIUM_ENABLED=false")

from core.cache import APICache
from core.dedup_tracker import get_global_tracker
from config import Config

logger = logging.getLogger('pipeline.oppadu_crawler')

class OppaduAntiDetection:
    """오빠두 크롤링 방지 우회를 위한 고급 기술"""
    
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36'
        ]
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

class OppaduCrawler:
    """
    오빠두 커뮤니티 크롤러
    - 크롤링 방지 우회
    - 페이지네이션 처리
    - 한국 데이터 특성화
    - 중복 방지
    """
    
    def __init__(self, cache: APICache):
        self.cache = cache
        self.base_url = "https://www.oppadu.com"
        self.community_url = f"{self.base_url}/community/question/"
        self.dedup_tracker = get_global_tracker()
        self.anti_detection = OppaduAntiDetection()
        
        # CloudScraper 설정 (Cloudflare 우회)
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        
        # Selenium 드라이버 (필요시 사용)
        self.driver = None
        
        logger.info("OppaduCrawler initialized with anti-detection measures")
    
    async def collect_oppadu_questions(self, max_pages: int = 50) -> List[Dict[str, Any]]:
        """
        오빠두 커뮤니티에서 답변 완료된 질문들을 수집
        
        Args:
            max_pages: 수집할 최대 페이지 수
            
        Returns:
            수집된 Q&A 데이터 리스트
        """
        logger.info(f"🇰🇷 오빠두 커뮤니티 데이터 수집 시작 (최대 {max_pages}페이지)")
        
        collected_data = []
        page = 1
        
        try:
            while page <= max_pages:
                logger.info(f"📄 페이지 {page} 처리 중...")
                
                # 페이지 URL 생성
                page_url = f"{self.community_url}?page={page}" if page > 1 else self.community_url
                
                # 답변 완료된 게시글 목록 수집
                answered_posts = await self._get_answered_posts(page_url)
                
                if not answered_posts:
                    logger.info(f"페이지 {page}에서 더 이상 답변 완료된 게시글이 없음")
                    break
                
                logger.info(f"   🎯 {len(answered_posts)}개의 답변 완료 게시글 발견")
                
                # 각 게시글의 상세 데이터 수집
                for post_url in answered_posts:
                    try:
                        # 중복 체크
                        post_id = self._extract_post_id(post_url)
                        if self.dedup_tracker.is_oppadu_post_collected(post_id):
                            logger.debug(f"이미 수집된 게시글 건너뜀: {post_id}")
                            continue
                        
                        # 상세 데이터 수집
                        post_data = await self._scrape_post_detail(post_url)
                        
                        if post_data:
                            # 한국 데이터 특성화 메타데이터 추가
                            post_data['metadata']['country'] = 'KR'
                            post_data['metadata']['language'] = 'ko'
                            post_data['metadata']['source_type'] = 'korean_community'
                            post_data['metadata']['cultural_context'] = 'korean_business'
                            
                            collected_data.append(post_data)
                            
                            # 중복 방지 추적기에 등록
                            self.dedup_tracker.mark_oppadu_collected(
                                post_id,
                                post_data.get('title', ''),
                                quality_score=post_data.get('quality_score', 0.0),
                                metadata={'page': page, 'collection_date': datetime.now().isoformat()}
                            )
                            
                            logger.info(f"   ✅ 게시글 수집 완료: {post_data.get('title', '')[:50]}...")
                        
                        # 인간적인 지연
                        await self.anti_detection.human_delay(1.0, 3.0)
                        
                    except Exception as e:
                        logger.error(f"게시글 처리 중 오류: {post_url} - {e}")
                        continue
                
                # 페이지네이션 확인
                has_next = await self._check_next_page(page_url)
                if not has_next:
                    logger.info("마지막 페이지에 도달함")
                    break
                
                page += 1
                
                # 페이지 간 지연
                await self.anti_detection.human_delay(3.0, 7.0)
        
        except Exception as e:
            logger.error(f"오빠두 데이터 수집 중 오류: {e}")
        
        finally:
            if self.driver:
                self.driver.quit()
        
        logger.info(f"🎉 오빠두 데이터 수집 완료: 총 {len(collected_data)}개 항목")
        return collected_data
    
    async def _get_answered_posts(self, page_url: str) -> List[str]:
        """답변 완료된 게시글 URL 목록 수집"""
        try:
            # 1차 시도: CloudScraper
            response = await self._fetch_with_cloudscraper(page_url)
            if response:
                return self._parse_answered_posts(response)
            
            # 2차 시도: Selenium (JavaScript 렌더링 필요한 경우)
            return await self._fetch_with_selenium(page_url)
            
        except Exception as e:
            logger.error(f"답변 완료 게시글 목록 수집 실패: {e}")
            return []
    
    async def _fetch_with_cloudscraper(self, url: str) -> Optional[str]:
        """CloudScraper를 사용한 페이지 수집"""
        try:
            # 헤더 로테이션
            self.anti_detection.rotate_headers()
            self.scraper.headers.update(self.anti_detection.session_headers)
            
            # Referer 설정 (자연스러운 탐색 시뮬레이션)
            if 'page=' in url:
                self.scraper.headers['Referer'] = self.community_url
            
            response = self.scraper.get(url, timeout=30)
            response.raise_for_status()
            
            logger.debug(f"CloudScraper 성공: {url}")
            return response.text
            
        except Exception as e:
            logger.warning(f"CloudScraper 실패: {e}")
            return None
    
    async def _fetch_with_selenium(self, url: str) -> List[str]:
        """Selenium을 사용한 JavaScript 렌더링 페이지 수집"""
        if not SELENIUM_AVAILABLE:
            self.logger.warning("Selenium not available, falling back to regular HTTP")
            return []
            
        try:
            if not self.driver:
                # Undetected Chrome 설정
                options = uc.ChromeOptions()
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-gpu')
                options.add_argument('--window-size=1920,1080')
                options.add_argument('--user-agent=' + random.choice(self.anti_detection.user_agents))
                
                # 한국 언어 설정
                options.add_argument('--lang=ko-KR')
                options.add_experimental_option('prefs', {
                    'intl.accept_languages': 'ko-KR,ko,en'
                })
                
                self.driver = uc.Chrome(options=options)
            
            self.driver.get(url)
            
            # 페이지 로딩 대기
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "post-list-modern"))
            )
            
            # 스크롤 다운 (레이지 로딩 대응)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            await asyncio.sleep(2)
            
            page_source = self.driver.page_source
            return self._parse_answered_posts(page_source)
            
        except Exception as e:
            logger.error(f"Selenium 페이지 수집 실패: {e}")
            return []
    
    def _parse_answered_posts(self, html_content: str) -> List[str]:
        """HTML에서 답변 완료된 게시글 URL 추출"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # post-list-modern 컨테이너 찾기
            post_list = soup.find('div', class_='post-list-modern')
            if not post_list:
                logger.warning("post-list-modern 컨테이너를 찾을 수 없음")
                return []
            
            answered_posts = []
            
            # 각 post-item-modern 검사
            post_items = post_list.find_all('div', class_='post-item-modern')
            logger.debug(f"총 {len(post_items)}개의 게시글 항목 발견")
            
            for item in post_items:
                # answer-complete-badge가 있는지 확인
                answer_badge = item.find(class_='answer-complete-badge')
                if answer_badge:
                    # 게시글 링크 추출 (post-title-modern 클래스 우선)
                    link_element = item.find('a', class_='post-title-modern') or item.find('a', href=True)
                    if link_element:
                        # 올바른 URL 구성: community_url + href (href는 ?로 시작)
                        href = link_element['href']
                        if href.startswith('?'):
                            # 쿼리 파라미터만 있는 경우 community_url과 결합
                            post_url = self.community_url + href
                        elif href.startswith('/'):
                            # 절대 경로인 경우 base_url과 결합
                            post_url = self.base_url + href
                        elif href.startswith('http'):
                            # 완전한 URL인 경우 그대로 사용
                            post_url = href
                        else:
                            # 상대 경로인 경우 urljoin 사용
                            post_url = urljoin(self.community_url, href)
                        
                        # URL 검증: 실제 게시글 URL인지 확인
                        if 'board_id=' in post_url and 'action=view' in post_url and 'uid=' in post_url:
                            answered_posts.append(post_url)
                            logger.debug(f"답변 완료 게시글 발견: {post_url}")
                        else:
                            logger.debug(f"유효하지 않은 게시글 URL 무시: {post_url}")
            
            logger.info(f"답변 완료된 게시글 {len(answered_posts)}개 발견")
            return answered_posts
            
        except Exception as e:
            logger.error(f"HTML 파싱 중 오류: {e}")
            return []
    
    async def _scrape_post_detail(self, post_url: str) -> Optional[Dict[str, Any]]:
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
    
    def _parse_post_detail(self, html_content: str, post_url: str) -> Optional[Dict[str, Any]]:
        """게시글 상세 페이지 HTML 파싱"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 기본 데이터 구조
            post_data = {
                'source': 'oppadu',
                'url': post_url,
                'post_id': self._extract_post_id(post_url),
                'collected_at': datetime.now().isoformat(),
                'metadata': {}
            }
            
            # 제목 추출
            title_element = soup.find('h1') or soup.find(class_='post-title')
            post_data['title'] = title_element.get_text(strip=True) if title_element else ""
            
            # 시스템 정보 추출 (post-options-display > options-container)
            options_display = soup.find(class_='post-options-display')
            if options_display:
                options_container = options_display.find(class_='options-container')
                if options_container:
                    excel_version = self._extract_version_info(options_container, '엑셀버전')
                    os_version = self._extract_version_info(options_container, 'OS버전')
                    
                    post_data['metadata']['excel_version'] = excel_version
                    post_data['metadata']['os_version'] = os_version
                    logger.debug(f"Extracted versions: Excel={excel_version}, OS={os_version}")
            
            # 질문 내용 추출 (post-content)
            post_content = soup.find(class_='post-content')
            if post_content:
                question_data = self._extract_content_data(post_content)
                post_data['question'] = question_data
                logger.debug(f"Extracted question data: {len(question_data.get('text', ''))} chars")
            
            # 채택된 답변 추출 (selected-answer-badge와 연관된 답변 찾기)
            selected_answer_badge = soup.find(class_='selected-answer-badge')
            if selected_answer_badge:
                # 여러 방법으로 답변 컨테이너 찾기
                answer_container = None
                
                # 방법 1: 부모 요소에서 comment-content-wrapper 찾기
                parent = selected_answer_badge.find_parent()
                if parent:
                    # 오빠두 특화: comment-wrapper selected-answer 내의 comment-content-wrapper
                    answer_content = (parent.find(class_='comment-content-wrapper') or 
                                    parent.find(class_='answer-content') or 
                                    parent.find(class_='post-content'))
                    if answer_content:
                        answer_container = answer_content
                
                # 방법 2: 조상 요소에서 comment-wrapper selected-answer 찾기
                if not answer_container:
                    comment_wrapper = selected_answer_badge.find_parent(class_='comment-wrapper')
                    if comment_wrapper and 'selected-answer' in comment_wrapper.get('class', []):
                        content_wrapper = comment_wrapper.find(class_='comment-content-wrapper')
                        if content_wrapper:
                            answer_container = content_wrapper
                
                # 방법 3: 형제 요소에서 찾기
                if not answer_container:
                    next_sibling = selected_answer_badge.find_next_sibling()
                    if next_sibling:
                        answer_container = next_sibling
                
                # 방법 4: 전체 답변 영역에서 찾기
                if not answer_container:
                    answer_section = soup.find(class_='answer-section') or soup.find(class_='answers')
                    if answer_section:
                        answer_container = answer_section
                
                if answer_container:
                    answer_data = self._extract_content_data(answer_container)
                    post_data['answer'] = answer_data
                    logger.debug(f"Extracted answer data: {len(answer_data.get('text', ''))} chars")
                else:
                    logger.warning("Selected answer badge found but no answer content located")
                    post_data['answer'] = {'text': '', 'images': [], 'has_code': False}
            else:
                # 채택된 답변이 없는 경우 빈 답변 설정
                post_data['answer'] = {'text': '', 'images': [], 'has_code': False}
            # 품질 점수 계산 (한국 데이터 특성 반영)
            post_data['quality_score'] = self._calculate_korean_quality_score(post_data)
            
            return post_data
            
        except Exception as e:
            logger.error(f"게시글 상세 파싱 중 오류: {e}")
            return None
    
    def _extract_version_info(self, container, version_type: str) -> str:
        """버전 정보 추출 (실제 HTML 구조에 맞게)"""
        try:
            # options-container 내에서 option-item들을 찾기
            option_items = container.find_all('div', class_='option-item')
            
            for option_item in option_items:
                # option-label과 option-value 찾기
                label_element = option_item.find('span', class_='option-label')
                value_element = option_item.find('span', class_='option-value')
                
                if label_element and value_element:
                    label_text = label_element.get_text(strip=True)
                    value_text = value_element.get_text(strip=True)
                    
                    # 엑셀버전 또는 OS버전 매치
                    if label_text == version_type:
                        logger.debug(f"Found {version_type}: {value_text}")
                        return value_text
            
            # 대체 방법: 전체 텍스트에서 패턴 검색
            text = container.get_text()
            if version_type in text:
                # 정규식으로 버전 정보 추출
                pattern = rf"{version_type}[:\s]*([^\n,]+)"
                match = re.search(pattern, text)
                if match:
                    return match.group(1).strip()
            
            return ""
            
        except Exception as e:
            logger.debug(f"Error extracting {version_type}: {e}")
            return ""
    
    def _extract_content_data(self, container) -> Dict[str, Any]:
        """컨텐츠 데이터 추출 (텍스트 + 이미지) - 개선된 버전"""
        if not container:
            return {'text': '', 'images': [], 'has_code': False}
        
        try:
            # 원본 HTML 텍스트 추출
            raw_text = container.get_text(strip=True, separator=' ')
            
            # 응답 정리 (새로운 클리너 사용)
            from core.oppadu_response_cleaner import OppaduResponseCleaner
            cleaner = OppaduResponseCleaner()
            cleaned_result = cleaner.clean_response(str(container))
            
            # 정리된 텍스트 사용
            text_content = cleaned_result['cleaned_response']
            
            # 이미지 URL 추출
            images = []
            img_tags = container.find_all('img')
            for img in img_tags:
                img_src = img.get('src') or img.get('data-src')
                if img_src and not any(skip in img_src for skip in ['icon', 'emoji', 'button']):
                    full_img_url = urljoin(self.base_url, img_src)
                    images.append(full_img_url)
            
            # 코드 블록 확인 (개선된 로직)
            has_code = cleaned_result['has_excel_content']
            
            return {
                'text': text_content,
                'images': images,
                'has_code': has_code,
                'word_count': len(text_content.split()),
                'excel_formulas': cleaned_result['excel_formulas'],
                'explanation': cleaned_result['explanation']
            }
            
        except Exception as e:
            logger.error(f"컨텐츠 데이터 추출 중 오류: {e}")
            # 기본 방식으로 폴백
            text_content = container.get_text(strip=True, separator=' ') if container else ''
            return {
                'text': text_content,
                'images': [],
                'has_code': '=' in text_content,
                'word_count': len(text_content.split()),
                'excel_formulas': [],
                'explanation': text_content
            }
    
    def _calculate_korean_quality_score(self, post_data: Dict[str, Any]) -> float:
        """한국 데이터 특성을 반영한 품질 점수 계산"""
        score = 5.0  # 기본 점수
        
        try:
            question = post_data.get('question', {})
            answer = post_data.get('answer', {})
            
            # 텍스트 길이 점수
            q_word_count = question.get('word_count', 0)
            a_word_count = answer.get('word_count', 0)
            
            if q_word_count >= 10:
                score += 1.0
            if a_word_count >= 15:
                score += 1.5
            
            # 시스템 정보 있으면 가산점
            if post_data.get('metadata', {}).get('excel_version'):
                score += 0.5
            if post_data.get('metadata', {}).get('os_version'):
                score += 0.5
            
            # 코드나 수식 있으면 가산점
            if question.get('has_code') or answer.get('has_code'):
                score += 1.0
            
            # 이미지 있으면 가산점
            if question.get('images') or answer.get('images'):
                score += 0.5
            
            # 한국어 특성 키워드 보너스
            korean_keywords = ['엑셀', '함수', '수식', '셀', '워크시트', '차트', '피벗']
            text_combined = f"{question.get('text', '')} {answer.get('text', '')}"
            keyword_count = sum(1 for keyword in korean_keywords if keyword in text_combined)
            score += keyword_count * 0.2
            
            return min(score, 10.0)  # 최대 10점
            
        except:
            return 5.0
    
    def _extract_post_id(self, url: str) -> str:
        """URL에서 게시글 ID 추출"""
        try:
            # URL 패턴에 따라 ID 추출
            parts = url.split('/')
            for part in reversed(parts):
                if part.isdigit():
                    return part
            # 숫자 ID가 없으면 URL 해시 사용
            return str(hash(url))
        except:
            return str(hash(url))
    
    async def _check_next_page(self, current_url: str) -> bool:
        """다음 페이지 존재 여부 확인"""
        try:
            html_content = await self._fetch_with_cloudscraper(current_url)
            if not html_content:
                return False
            
            soup = BeautifulSoup(html_content, 'html.parser')
            pagination = soup.find(class_='oppadu-pagination')
            
            if pagination:
                # 다음 페이지 링크나 버튼 확인
                next_link = pagination.find('a', string=re.compile('다음|>|Next'))
                return next_link is not None
            
            return False
            
        except:
            return False
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """수집 통계 반환"""
        return {
            'source': 'oppadu',
            'total_collected': 0,  # 실제 구현에서는 추적
            'last_collection': datetime.now().isoformat()
        }