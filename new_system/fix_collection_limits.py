#!/usr/bin/env python3
"""
수집 한계 문제 해결 및 설정 최적화
"""
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from collectors.stackoverflow_collector import StackOverflowCollector
from collectors.reddit_collector import RedditCollector
from core.cache import APICache, LocalCache
from core.dedup_tracker import get_global_tracker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def reset_deduplication_for_fresh_collection():
    """중복 추적기를 리셋하여 새로운 수집 허용"""
    
    logger.info("🔄 중복 추적기 리셋")
    logger.info("=" * 50)
    
    tracker = get_global_tracker()
    
    # 기존 통계 확인
    stats = tracker.get_collection_stats(days=1)
    if stats:
        so_count = stats.get('stackoverflow', {}).get('total_collected', 0)
        reddit_count = stats.get('reddit', {}).get('total_collected', 0)
        logger.info(f"   📊 기존 수집량: SO {so_count}개, Reddit {reddit_count}개")
    
    # 오래된 레코드 정리 (1일 이상)
    deleted_count = tracker.cleanup_old_records(days=1)
    logger.info(f"   🗑️  정리된 레코드: {deleted_count}개")
    
    # 새로운 통계 확인
    new_stats = tracker.get_collection_stats(days=1)
    if new_stats:
        new_so_count = new_stats.get('stackoverflow', {}).get('total_collected', 0)
        new_reddit_count = new_stats.get('reddit', {}).get('total_collected', 0)
        logger.info(f"   ✅ 리셋 후: SO {new_so_count}개, Reddit {new_reddit_count}개")

async def test_optimized_collection():
    """최적화된 설정으로 수집 테스트"""
    
    logger.info("\n🎯 최적화된 수집 테스트")
    logger.info("=" * 50)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/optimized_collection.db"))
    cache = APICache(local_cache)
    
    # 1. Stack Overflow 테스트
    logger.info("📚 Stack Overflow 최적화 테스트:")
    so_collector = StackOverflowCollector(cache)
    
    # 더 넓은 기간으로 테스트 (7일)
    from_date = datetime.now() - timedelta(days=7)
    
    try:
        so_data = await so_collector.collect_excel_questions(
            from_date=from_date,
            max_pages=5  # 적은 페이지로 테스트
        )
        logger.info(f"   ✅ Stack Overflow: {len(so_data)}개 수집")
        
        if len(so_data) > 0:
            sample = so_data[0]
            logger.info(f"   📝 샘플: {sample.get('title', 'No title')[:50]}...")
        
    except Exception as e:
        logger.error(f"   ❌ Stack Overflow 테스트 실패: {e}")
    
    # 2. Reddit 테스트  
    logger.info("\n🟠 Reddit 최적화 테스트:")
    reddit_collector = RedditCollector(cache)
    
    try:
        reddit_data = await reddit_collector.collect_excel_discussions(
            from_date=from_date,
            max_submissions=10  # 적은 수로 테스트
        )
        logger.info(f"   ✅ Reddit: {len(reddit_data)}개 수집")
        
        if len(reddit_data) > 0:
            sample = reddit_data[0]
            logger.info(f"   📝 샘플: {sample.submission.get('title', 'No title')[:50]}...")
            
    except Exception as e:
        logger.error(f"   ❌ Reddit 테스트 실패: {e}")

async def create_small_dataset():
    """작은 데이터셋으로 전체 파이프라인 테스트"""
    
    logger.info("\n📦 소규모 데이터셋 생성")
    logger.info("=" * 50)
    
    # 기존 데이터 활용하여 최소 데이터셋 생성
    from output.dataset_generator import JSONLDatasetGenerator
    
    # 간단한 테스트 데이터
    test_qa_pairs = [
        {
            'id': 'test_1',
            'source': 'stackoverflow',
            'question': {
                'title': 'Test Excel Question 1',
                'body_markdown': 'How to use VLOOKUP?',
                'question_id': 99999999,
                'tags': ['excel', 'excel-formula'],
                'score': 5
            },
            'answer': {
                'body': 'Use =VLOOKUP(lookup_value, table_array, col_index_num, FALSE)',
                'answer_id': 88888888,
                'score': 10,
                'is_accepted': True
            },
            'quality_metrics': {
                'overall_score': 8.5,
                'raw_question_score': 5.0,
                'raw_answer_score': 10.0
            },
            'has_accepted_answer': True
        },
        {
            'id': 'test_2', 
            'source': 'reddit',
            'question': {
                'title': 'Test Excel Question 2',
                'text': 'Need help with INDEX MATCH formula',
                'reddit_id': 'test123',
                'flair': 'solved'
            },
            'answer': {
                'text': 'Try =INDEX(return_array, MATCH(lookup_value, lookup_array, 0))',
                'reddit_id': 'reply123'
            },
            'quality_metrics': {
                'overall_score': 7.5,
                'raw_question_score': 4.0,
                'raw_answer_score': 8.0
            }
        }
    ]
    
    try:
        generator = JSONLDatasetGenerator()
        
        # 데이터셋 생성
        output_path = generator.generate_dataset(
            test_qa_pairs,
            data_sources=['stackoverflow', 'reddit']
        )
        
        logger.info(f"   ✅ 테스트 데이터셋 생성: {output_path}")
        
        # 검증
        validation_result = generator.validate_dataset(output_path)
        if validation_result['valid']:
            logger.info(f"   ✅ 데이터셋 검증 통과: {validation_result['valid_lines']}개 항목")
        else:
            logger.warning(f"   ⚠️  데이터셋 검증 실패: {validation_result.get('errors', [])}")
            
        return output_path
        
    except Exception as e:
        logger.error(f"   ❌ 데이터셋 생성 실패: {e}")
        return None

async def suggest_dashboard_settings():
    """대시보드 설정 권장사항"""
    
    logger.info("\n⚙️  대시보드 설정 권장사항")
    logger.info("=" * 50)
    
    logger.info("1. 수집 매개변수 조정:")
    logger.info("   • target_count: 100 → 20 (작게 시작)")
    logger.info("   • max_pages: 50 → 10 (API 한도 고려)")
    logger.info("   • collection_period: 7일 → 30일 (더 많은 데이터)")
    
    logger.info("\n2. Rate Limiting 강화:")
    logger.info("   • Stack Overflow: 2초 → 3초 간격")
    logger.info("   • Reddit: 1초 → 2초 간격")
    logger.info("   • 동시 수집 → 순차 수집")
    
    logger.info("\n3. 중복 방지 최적화:")
    logger.info("   • 일일 자동 정리 활성화")
    logger.info("   • 수집 전 중복 체크 로그 개선")
    logger.info("   • 새로운 데이터 우선 수집")
    
    logger.info("\n4. 오류 처리 강화:")
    logger.info("   • API 한도 도달 시 graceful 중단")
    logger.info("   • 부분 성공 시 결과 저장")
    logger.info("   • 재시도 로직 개선")

async def main():
    """메인 수정 루틴"""
    logger.info("🔧 수집 한계 문제 해결")
    logger.info("=" * 60)
    
    # 1. 중복 추적기 리셋
    await reset_deduplication_for_fresh_collection()
    
    # 2. 최적화된 수집 테스트
    await test_optimized_collection()
    
    # 3. 소규모 데이터셋 생성
    test_dataset = await create_small_dataset()
    
    # 4. 대시보드 설정 권장사항
    await suggest_dashboard_settings()
    
    # 최종 요약
    logger.info("\n" + "🎉" * 30)
    logger.info("수집 한계 문제 해결 완료!")
    logger.info("")
    logger.info("다음 단계:")
    logger.info("1. 대시보드에서 작은 target_count(20)로 테스트")
    logger.info("2. 수집 기간을 30일로 설정") 
    logger.info("3. Rate limiting 간격 증가")
    logger.info("4. 성공 시 점진적으로 증가")

if __name__ == "__main__":
    asyncio.run(main())