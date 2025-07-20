#!/usr/bin/env python3
"""
오빠두 콘텐츠 추출 디버깅
실제 HTML 구조를 분석하여 문제점 찾기
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

async def debug_oppadu_content_extraction():
    """오빠두 콘텐츠 추출 디버깅"""
    
    logger.info("🔍 오빠두 콘텐츠 추출 디버깅")
    logger.info("=" * 50)
    
    try:
        local_cache = LocalCache(db_path=Path("/tmp/debug_oppadu.db"))
        cache = APICache(local_cache)
        crawler = OppaduCrawler(cache)
        
        # 단일 게시글 상세 페이지 직접 수집
        test_url = "https://www.oppadu.com?board_id=1&action=view&uid=79620&pg=1"
        
        logger.info(f"📡 테스트 URL: {test_url}")
        
        # 페이지 HTML 수집
        html_content = await crawler._fetch_with_cloudscraper(test_url)
        
        if html_content:
            logger.info(f"✅ HTML 수집 성공: {len(html_content)} 문자")
            
            # HTML 구조 분석
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 제목 찾기
            logger.info("\n🔍 제목 요소 찾기:")
            title_candidates = [
                soup.find('h1'),
                soup.find(class_='post-title'),
                soup.find('title'),
                soup.find(class_='board_title'),
                soup.find(class_='subject')
            ]
            
            for i, candidate in enumerate(title_candidates):
                if candidate:
                    logger.info(f"   제목 후보 {i+1}: {candidate.get_text(strip=True)[:100]}...")
            
            # options-container 찾기
            logger.info("\n🔍 옵션 컨테이너 찾기:")
            options_display = soup.find(class_='post-options-display')
            if options_display:
                logger.info("   ✅ post-options-display 발견")
                options_container = options_display.find(class_='options-container')
                if options_container:
                    logger.info("   ✅ options-container 발견")
                    option_items = options_container.find_all(class_='option-item')
                    logger.info(f"   📊 option-item 개수: {len(option_items)}")
                    
                    for item in option_items:
                        label = item.find(class_='option-label')
                        value = item.find(class_='option-value')
                        if label and value:
                            logger.info(f"     {label.get_text(strip=True)}: {value.get_text(strip=True)}")
                else:
                    logger.warning("   ⚠️ options-container 없음")
            else:
                logger.warning("   ⚠️ post-options-display 없음")
            
            # post-content 찾기
            logger.info("\n🔍 게시글 콘텐츠 찾기:")
            content_candidates = [
                soup.find(class_='post-content'),
                soup.find(class_='board_content'),
                soup.find(class_='content'),
                soup.find(class_='xe_content'),
                soup.find(class_='article-content')
            ]
            
            for i, candidate in enumerate(content_candidates):
                if candidate:
                    text = candidate.get_text(strip=True)
                    logger.info(f"   콘텐츠 후보 {i+1}: {text[:200]}... (길이: {len(text)})")
            
            # selected-answer-badge 찾기
            logger.info("\n🔍 채택된 답변 찾기:")
            answer_candidates = [
                soup.find(class_='selected-answer-badge'),
                soup.find(class_='answer-complete-badge'),
                soup.find(class_='best-answer'),
                soup.find(class_='accepted-answer')
            ]
            
            for i, candidate in enumerate(answer_candidates):
                if candidate:
                    logger.info(f"   답변 배지 후보 {i+1}: {candidate}")
                    
                    # 주변 요소 탐색
                    parent = candidate.find_parent()
                    if parent:
                        answer_text = parent.get_text(strip=True)
                        logger.info(f"     부모 요소 텍스트: {answer_text[:200]}...")
            
            # 전체 HTML 구조 일부 출력 (디버깅용)
            logger.info("\n📄 HTML 구조 샘플:")
            body = soup.find('body')
            if body:
                # 주요 클래스들 찾기
                all_classes = set()
                for element in body.find_all(True):
                    if element.get('class'):
                        all_classes.update(element.get('class'))
                
                logger.info(f"   발견된 CSS 클래스 개수: {len(all_classes)}")
                relevant_classes = [cls for cls in all_classes if any(keyword in cls.lower() 
                                  for keyword in ['content', 'post', 'answer', 'title', 'option'])]
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
            logger.error("❌ HTML 수집 실패")
            
    except Exception as e:
        logger.error(f"❌ 디버깅 중 오류: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(debug_oppadu_content_extraction())