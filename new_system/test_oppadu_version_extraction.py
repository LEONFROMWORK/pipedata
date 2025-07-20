#!/usr/bin/env python3
"""
오빠두 버전 정보 추출 테스트
실제 HTML 구조 기반으로 테스트
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

def test_html_parsing():
    """HTML 파싱 테스트"""
    
    # 실제 오빠두 HTML 구조 시뮬레이션
    test_html = """
    <html>
    <head><title>Test Post</title></head>
    <body>
        <h1>테스트 질문 제목</h1>
        
        <div class="post-options-display">
            <div class="options-container">
                <div class="option-item">
                    <span class="option-label">엑셀버전</span>
                    <span class="option-value">M365</span>
                </div>
                <div class="option-item">
                    <span class="option-label">OS버전</span>
                    <span class="option-value">윈도우11</span>
                </div>
            </div>
        </div>
        
        <div class="post-content">
            <p>이것은 테스트 질문 내용입니다. Excel에서 VLOOKUP 함수를 사용하고 싶습니다.</p>
            <p>수식: =VLOOKUP(A1, 데이터!A:B, 2, FALSE)</p>
        </div>
        
        <div class="answer-section">
            <div class="selected-answer-badge">채택된 답변</div>
            <div class="answer-content">
                <p>VLOOKUP 함수 사용법은 다음과 같습니다:</p>
                <p>=VLOOKUP(찾을값, 범위, 열번호, 정확히일치)</p>
                <p>예시: =VLOOKUP(A1, Sheet2!A:B, 2, 0)</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    logger.info("🧪 HTML 파싱 테스트 시작")
    logger.info("=" * 40)
    
    try:
        # BeautifulSoup으로 파싱
        soup = BeautifulSoup(test_html, 'html.parser')
        
        # OppaduCrawler 인스턴스 생성
        local_cache = LocalCache(db_path=Path("/tmp/test_oppadu.db"))
        cache = APICache(local_cache)
        crawler = OppaduCrawler(cache)
        
        # 파싱 테스트
        post_data = crawler._parse_post_detail(test_html, "http://test.com/post/123")
        
        if post_data:
            logger.info("✅ 파싱 성공!")
            logger.info(f"   제목: {post_data.get('title', 'N/A')}")
            logger.info(f"   Excel 버전: {post_data.get('metadata', {}).get('excel_version', 'N/A')}")
            logger.info(f"   OS 버전: {post_data.get('metadata', {}).get('os_version', 'N/A')}")
            
            question = post_data.get('question', {})
            answer = post_data.get('answer', {})
            
            logger.info(f"   질문 텍스트 길이: {len(question.get('text', ''))} 문자")
            logger.info(f"   답변 텍스트 길이: {len(answer.get('text', ''))} 문자")
            logger.info(f"   질문에 코드 포함: {question.get('has_code', False)}")
            logger.info(f"   답변에 코드 포함: {answer.get('has_code', False)}")
            logger.info(f"   품질 점수: {post_data.get('quality_score', 0):.2f}")
            
            # 상세 내용 출력
            logger.info("\n📝 상세 내용:")
            logger.info(f"   질문: {question.get('text', 'N/A')[:100]}...")
            logger.info(f"   답변: {answer.get('text', 'N/A')[:100]}...")
            
        else:
            logger.error("❌ 파싱 실패")
            
    except Exception as e:
        logger.error(f"❌ 테스트 중 오류: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

async def test_live_oppadu_parsing():
    """실제 오빠두 웹사이트 파싱 테스트 (매우 제한적)"""
    
    logger.info("\n🌐 실제 오빠두 웹사이트 테스트")
    logger.info("=" * 40)
    
    try:
        local_cache = LocalCache(db_path=Path("/tmp/test_oppadu_live.db"))
        cache = APICache(local_cache)
        crawler = OppaduCrawler(cache)
        
        # 매우 제한적인 테스트 (1페이지, 최대 1개 항목)
        logger.info("📡 오빠두 웹사이트에서 샘플 데이터 수집 중...")
        
        results = await crawler.collect_oppadu_questions(max_pages=1)
        
        if results:
            logger.info(f"✅ 실제 수집 성공: {len(results)}개 항목")
            
            first_item = results[0]
            logger.info(f"   제목: {first_item.get('title', 'N/A')[:50]}...")
            logger.info(f"   Excel 버전: {first_item.get('metadata', {}).get('excel_version', 'N/A')}")
            logger.info(f"   OS 버전: {first_item.get('metadata', {}).get('os_version', 'N/A')}")
            logger.info(f"   품질 점수: {first_item.get('quality_score', 0):.2f}")
            
        else:
            logger.warning("⚠️ 실제 수집 실패 또는 데이터 없음")
            
    except Exception as e:
        logger.error(f"❌ 실제 테스트 중 오류: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

async def main():
    """메인 테스트 함수"""
    
    # 1. HTML 파싱 테스트
    test_html_parsing()
    
    # 2. 실제 웹사이트 테스트 (선택적)
    await test_live_oppadu_parsing()
    
    logger.info("\n🎉 테스트 완료!")

if __name__ == "__main__":
    asyncio.run(main())