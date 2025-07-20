#!/usr/bin/env python3
"""
수정된 오빠두 크롤러 전체 테스트
실제 Q&A 데이터를 여러 개 수집해서 검증
"""

import asyncio
import logging
from pathlib import Path
import sys
import json

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from collectors.oppadu_crawler import OppaduCrawler
from core.cache import APICache, LocalCache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_fixed_oppadu_crawler():
    """수정된 오빠두 크롤러 전체 테스트"""
    
    logger.info("🧪 수정된 오빠두 크롤러 전체 테스트")
    logger.info("=" * 80)
    
    try:
        # 캐시 및 크롤러 초기화
        local_cache = LocalCache(db_path=Path("/tmp/test_fixed_oppadu.db"))
        cache = APICache(local_cache)
        crawler = OppaduCrawler(cache)
        
        # 1. 소량 데이터 수집 (최대 3개 게시글)
        logger.info("📊 소량 Q&A 데이터 수집 테스트 (최대 1페이지)")
        collected_data = await crawler.collect_oppadu_questions(max_pages=1)
        
        if collected_data:
            logger.info(f"✅ {len(collected_data)}개의 Q&A 수집 완료")
            
            # 수집된 데이터 검증
            logger.info("\n📋 수집된 데이터 검증:")
            
            valid_count = 0
            for i, item in enumerate(collected_data, 1):
                logger.info(f"\n--- Q&A {i} ---")
                logger.info(f"제목: {item.get('title', 'N/A')}")
                logger.info(f"URL: {item.get('url', 'N/A')}")
                logger.info(f"Excel 버전: {item.get('metadata', {}).get('excel_version', 'N/A')}")
                logger.info(f"OS 버전: {item.get('metadata', {}).get('os_version', 'N/A')}")
                
                question = item.get('question', {})
                answer = item.get('answer', {})
                
                q_text_len = len(question.get('text', ''))
                a_text_len = len(answer.get('text', ''))
                
                logger.info(f"질문 길이: {q_text_len} 문자")
                logger.info(f"답변 길이: {a_text_len} 문자")
                logger.info(f"품질 점수: {item.get('quality_score', 'N/A')}")
                
                # 질문 미리보기
                if q_text_len > 0:
                    logger.info(f"질문 미리보기: {question.get('text', '')[:100]}...")
                
                # 답변 미리보기
                if a_text_len > 0:
                    logger.info(f"답변 미리보기: {answer.get('text', '')[:100]}...")
                
                # 유효성 검증
                is_valid = (
                    item.get('title', '').strip() != '' and
                    q_text_len > 10 and
                    a_text_len > 5 and
                    item.get('url', '').startswith('https://www.oppadu.com/community/question/?')
                )
                
                if is_valid:
                    valid_count += 1
                    logger.info("✅ 유효한 Q&A 데이터")
                else:
                    logger.info("❌ 데이터 품질 문제")
            
            # 전체 결과 요약
            logger.info(f"\n" + "=" * 80)
            logger.info("📊 수집 결과 요약")
            logger.info("=" * 80)
            logger.info(f"총 수집: {len(collected_data)}개")
            logger.info(f"유효 데이터: {valid_count}개")
            logger.info(f"성공률: {valid_count/len(collected_data)*100:.1f}%" if collected_data else "0%")
            
            if valid_count > 0:
                logger.info("🎉 오빠두 크롤러 수정 성공!")
                logger.info("   - URL 구성 문제 해결")
                logger.info("   - 실제 Q&A 콘텐츠 추출")
                logger.info("   - 답변 추출 로직 개선")
                
                # 샘플 데이터 JSON으로 저장
                sample_data = collected_data[:2]  # 처음 2개만
                output_file = Path("/tmp/oppadu_sample_data.json")
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(sample_data, f, ensure_ascii=False, indent=2)
                logger.info(f"📁 샘플 데이터 저장: {output_file}")
            else:
                logger.error("❌ 유효한 데이터가 수집되지 않음")
        else:
            logger.error("❌ 데이터 수집 실패")
            
    except Exception as e:
        logger.error(f"❌ 테스트 중 오류: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(test_fixed_oppadu_crawler())