#!/usr/bin/env python3
"""
오빠두 통합 테스트
- 크롤링 기능 테스트
- 품질 평가 테스트  
- 데이터셋 생성 테스트
"""

import asyncio
import logging
from pathlib import Path
import sys
import json

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from collectors.oppadu_crawler import OppaduCrawler
from quality.korean_oppadu_scorer import KoreanOppaduScorer
from output.oppadu_dataset_generator import OppaduDatasetGenerator
from core.cache import APICache, LocalCache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_oppadu_integration():
    """오빠두 통합 테스트"""
    
    logger.info("🇰🇷 오빠두 통합 테스트 시작")
    logger.info("=" * 50)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/oppadu_test.db"))
    cache = APICache(local_cache)
    
    try:
        # 1. 오빠두 크롤러 테스트
        logger.info("📡 1단계: 오빠두 크롤러 테스트")
        crawler = OppaduCrawler(cache)
        
        # 매우 제한적인 테스트 (1페이지만)
        oppadu_data = await crawler.collect_oppadu_questions(max_pages=1)
        
        if oppadu_data:
            logger.info(f"   ✅ 크롤링 성공: {len(oppadu_data)}개 항목 수집")
            
            # 첫 번째 항목 정보 출력
            first_item = oppadu_data[0]
            logger.info(f"   📝 샘플 제목: {first_item.get('title', '')[:50]}...")
            logger.info(f"   🔧 Excel 버전: {first_item.get('metadata', {}).get('excel_version', 'N/A')}")
            logger.info(f"   💻 OS 버전: {first_item.get('metadata', {}).get('os_version', 'N/A')}")
            
        else:
            logger.warning("   ⚠️ 크롤링 실패 또는 데이터 없음")
            return
        
        # 2. 한국 품질 평가 테스트
        logger.info("\n🎯 2단계: 한국 품질 평가 테스트")
        scorer = KoreanOppaduScorer()
        
        quality_results = scorer.score_batch(oppadu_data)
        
        if quality_results:
            logger.info(f"   ✅ 품질 평가 완료: {len(quality_results)}개 항목")
            
            # 통계 정보
            stats = scorer.get_batch_statistics(quality_results)
            logger.info(f"   📊 평균 점수: {stats.get('average_score', 0):.2f}")
            logger.info(f"   🏢 한국 비즈니스 포스트: {stats.get('korean_business_posts', 0)}개")
            logger.info(f"   🔧 고급 포스트: {stats.get('advanced_posts', 0)}개")
            
            # 품질 필터링
            threshold = 5.5
            filtered_data = scorer.filter_by_quality(oppadu_data, quality_results, threshold)
            logger.info(f"   🔍 품질 필터링 (임계값 {threshold}): {len(filtered_data)}개 통과")
            
        else:
            logger.warning("   ⚠️ 품질 평가 실패")
            return
        
        # 3. 한국 데이터셋 생성 테스트
        logger.info("\n📁 3단계: 한국 데이터셋 생성 테스트")
        generator = OppaduDatasetGenerator()
        
        if filtered_data:
            dataset_path = generator.generate_oppadu_dataset(
                filtered_data,
                metadata={'test_execution': True}
            )
            
            if dataset_path and Path(dataset_path).exists():
                logger.info(f"   ✅ 데이터셋 생성 성공: {dataset_path}")
                
                # 파일 검증
                validation_result = generator.validate_korean_dataset(dataset_path)
                logger.info(f"   🔍 검증 결과: {validation_result.get('valid_lines', 0)}개 유효 라인")
                logger.info(f"   🇰🇷 한국어 콘텐츠: {validation_result.get('korean_content_lines', 0)}개 라인 ({validation_result.get('korean_content_percentage', 0):.1f}%)")
                
                # 샘플 데이터 출력
                with open(dataset_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    if first_line:
                        sample = json.loads(first_line)
                        logger.info(f"   📝 샘플 질문: {sample.get('user_question', '')[:50]}...")
                        logger.info(f"   💡 비즈니스 도메인: {sample.get('metadata', {}).get('business_domain', 'N/A')}")
                        logger.info(f"   🔧 Excel 함수: {len(sample.get('metadata', {}).get('functions', []))}개")
                
            else:
                logger.warning("   ⚠️ 데이터셋 생성 실패")
                return
        
        logger.info("\n🎉 오빠두 통합 테스트 완료!")
        logger.info("=" * 50)
        
        # 요약 정보
        logger.info("📊 테스트 요약:")
        logger.info(f"   크롤링: {len(oppadu_data)}개 수집")
        logger.info(f"   품질 평가: {len(quality_results)}개 평가")
        logger.info(f"   최종 데이터: {len(filtered_data)}개 통과")
        logger.info(f"   데이터셋: {dataset_path}")
        
        return {
            'crawled_count': len(oppadu_data),
            'quality_assessed': len(quality_results),
            'final_count': len(filtered_data),
            'dataset_path': dataset_path
        }
        
    except Exception as e:
        logger.error(f"❌ 오빠두 통합 테스트 실패: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

if __name__ == "__main__":
    asyncio.run(test_oppadu_integration())