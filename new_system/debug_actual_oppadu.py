#!/usr/bin/env python3
"""
실제 오빠두 URL로 콘텐츠 추출 디버깅
"""

import asyncio
import logging
from pathlib import Path
import sys
from bs4 import BeautifulSoup

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from collectors.oppadu_crawler import OppaduCrawler
from core.cache import APICache, LocalCache

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def debug_actual_oppadu_urls():
    """실제 오빠두 URL로 콘텐츠 추출 디버깅"""
    
    logger.info("🔍 실제 오빠두 URL 디버깅")
    logger.info("=" * 50)
    
    try:
        local_cache = LocalCache(db_path=Path("/tmp/debug_actual_oppadu.db"))
        cache = APICache(local_cache)
        crawler = OppaduCrawler(cache)
        
        # 1단계: 실제 답변 완료 게시글 URL 수집
        logger.info("📄 실제 답변 완료 게시글 URL 수집 중...")
        answered_posts = await crawler._get_answered_posts(crawler.community_url)
        
        if answered_posts:
            logger.info(f"✅ {len(answered_posts)}개의 답변 완료 게시글 발견")
            
            # 첫 번째 게시글로 테스트
            test_url = answered_posts[0]
            logger.info(f"🧪 테스트 URL: {test_url}")
            
            # HTML 수집
            html_content = await crawler._fetch_with_cloudscraper(test_url)
            
            if html_content:
                logger.info(f"✅ HTML 수집 성공: {len(html_content)} 문자")
                
                # BeautifulSoup으로 구조 분석
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # HTML 구조 찾기
                logger.info("\n🔍 HTML 구조 분석:")
                
                # 주요 컨테이너들 찾기
                containers = [
                    ('post-content', soup.find(class_='post-content')),
                    ('board-content', soup.find(class_='board-content')),
                    ('content', soup.find(class_='content')),
                    ('xe_content', soup.find(class_='xe_content')),
                    ('article-content', soup.find(class_='article-content')),
                    ('question-content', soup.find(class_='question-content')),
                    ('view-content', soup.find(class_='view-content')),
                ]
                
                for name, element in containers:
                    if element:
                        text = element.get_text(strip=True)
                        logger.info(f"   ✅ {name}: {text[:100]}... (길이: {len(text)})")
                
                # options-container 관련 찾기
                logger.info("\n🔍 옵션 컨테이너 찾기:")
                options_patterns = [
                    ('post-options-display', soup.find(class_='post-options-display')),
                    ('options-container', soup.find(class_='options-container')),
                    ('option-item', soup.find_all(class_='option-item')),
                    ('post-meta', soup.find(class_='post-meta')),
                    ('meta-info', soup.find(class_='meta-info')),
                ]
                
                for name, element in options_patterns:
                    if element:
                        if isinstance(element, list):
                            logger.info(f"   ✅ {name}: {len(element)}개 발견")
                            for i, item in enumerate(element[:3]):
                                logger.info(f"     {i+1}: {item.get_text(strip=True)[:50]}...")
                        else:
                            logger.info(f"   ✅ {name}: {element.get_text(strip=True)[:100]}...")
                
                # 답변 관련 찾기
                logger.info("\n🔍 답변 요소 찾기:")
                answer_patterns = [
                    ('selected-answer-badge', soup.find(class_='selected-answer-badge')),
                    ('answer-complete-badge', soup.find(class_='answer-complete-badge')),
                    ('best-answer', soup.find(class_='best-answer')),
                    ('accepted-answer', soup.find(class_='accepted-answer')),
                    ('answer-content', soup.find(class_='answer-content')),
                    ('reply-content', soup.find(class_='reply-content')),
                ]
                
                for name, element in answer_patterns:
                    if element:
                        text = element.get_text(strip=True)
                        logger.info(f"   ✅ {name}: {text[:100]}... (길이: {len(text)})")
                
                # 모든 클래스 분석 (오빠두 특화)
                logger.info("\n📊 발견된 모든 클래스:")
                all_classes = set()
                for element in soup.find_all(True):
                    if element.get('class'):
                        all_classes.update(element.get('class'))
                
                # 관련 클래스만 필터링
                relevant_classes = [cls for cls in all_classes if any(keyword in cls.lower() 
                                  for keyword in ['content', 'post', 'answer', 'title', 'option', 'excel', 'question', 'reply', 'view', 'meta'])]
                
                logger.info(f"   관련 클래스들: {sorted(relevant_classes)}")
                
                # 실제 파싱 테스트
                logger.info("\n🧪 실제 파싱 테스트:")
                post_data = crawler._parse_post_detail(html_content, test_url)
                
                if post_data:
                    logger.info(f"   ✅ 파싱 성공!")
                    logger.info(f"     제목: {post_data.get('title', 'N/A')}")
                    logger.info(f"     Excel 버전: {post_data.get('metadata', {}).get('excel_version', 'N/A')}")
                    logger.info(f"     OS 버전: {post_data.get('metadata', {}).get('os_version', 'N/A')}")
                    
                    question = post_data.get('question', {})
                    answer = post_data.get('answer', {})
                    
                    logger.info(f"     질문 텍스트 길이: {len(question.get('text', ''))} 문자")
                    logger.info(f"     답변 텍스트 길이: {len(answer.get('text', ''))} 문자")
                    
                    if question.get('text'):
                        logger.info(f"     질문 미리보기: {question.get('text')[:200]}...")
                    if answer.get('text'):
                        logger.info(f"     답변 미리보기: {answer.get('text')[:200]}...")
                else:
                    logger.error("   ❌ 파싱 실패")
                
            else:
                logger.error(f"❌ HTML 수집 실패: {test_url}")
        else:
            logger.error("❌ 답변 완료 게시글을 찾을 수 없음")
            
    except Exception as e:
        logger.error(f"❌ 디버깅 중 오류: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(debug_actual_oppadu_urls())