#!/usr/bin/env python3
"""
Reddit 수집 문제 진단 및 테스트
"""
import asyncio
import logging
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from collectors.reddit_collector import RedditCollector
from core.cache import APICache, LocalCache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def test_reddit_basic_collection():
    """기본 Reddit 수집 테스트"""
    
    logger.info("🟠 Reddit 기본 수집 테스트")
    logger.info("=" * 70)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/reddit_test.db"))
    cache = APICache(local_cache)
    
    # Reddit collector 초기화
    reddit_collector = RedditCollector(cache)
    
    try:
        # 기본 설정 확인
        logger.info(f"   📝 Target subreddit: r/{reddit_collector.config['subreddit']}")
        
        # PRAW Reddit 객체 직접 테스트
        logger.info("   🔍 직접 Reddit API 테스트...")
        
        subreddit = reddit_collector.reddit.subreddit('excel')
        
        # 최신 포스트 몇 개 가져와서 확인
        submission_count = 0
        solved_count = 0
        
        logger.info("   📚 최신 포스트 분석 중...")
        
        for submission in subreddit.new(limit=10):
            submission_count += 1
            
            flair = submission.link_flair_text or "No Flair"
            score = submission.score
            num_comments = submission.num_comments
            
            logger.info(f"      [{submission_count}] {submission.title[:50]}...")
            logger.info(f"           플레어: {flair} | 점수: {score} | 댓글: {num_comments}")
            
            if flair == 'Solved':
                solved_count += 1
        
        logger.info(f"\n   📊 Reddit r/excel 현황:")
        logger.info(f"      • 총 포스트 확인: {submission_count}개")
        logger.info(f"      • 'Solved' 플레어: {solved_count}개")
        logger.info(f"      • 기타 플레어: {submission_count - solved_count}개")
        
        # 실제 수집기로 테스트
        logger.info("\n   ⚙️  RedditCollector로 실제 수집 테스트...")
        
        result = await reddit_collector.collect_excel_discussions(max_submissions=5)
        
        logger.info(f"   ✅ 수집 결과: {len(result)}개")
        
        if result:
            for i, item in enumerate(result, 1):
                logger.info(f"      [{i}] {item.submission.get('title', 'Unknown')[:50]}...")
        else:
            logger.warning("   ⚠️  수집된 데이터가 없습니다.")
            
            # 필터 조건 확인
            logger.info("\n   🔍 필터 조건 분석...")
            
            test_submission = next(subreddit.new(limit=1))
            filter_result = reddit_collector._passes_submission_filter(test_submission)
            
            logger.info(f"      테스트 포스트: {test_submission.title[:40]}...")
            logger.info(f"      필터 통과: {filter_result}")
            logger.info(f"      점수: {test_submission.score}")
            logger.info(f"      댓글 수: {test_submission.num_comments}")
            logger.info(f"      플레어: {test_submission.link_flair_text}")
        
        return len(result)
        
    except Exception as e:
        logger.error(f"   ❌ Reddit 수집 테스트 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0

async def test_reddit_filter_adjustment():
    """Reddit 필터 조정 테스트"""
    
    logger.info("\n🔧 Reddit 필터 조정 테스트")
    logger.info("=" * 70)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/reddit_filter_test.db"))
    cache = APICache(local_cache)
    
    # Reddit collector 초기화
    reddit_collector = RedditCollector(cache)
    
    try:
        subreddit = reddit_collector.reddit.subreddit('excel')
        
        passed_count = 0
        failed_count = 0
        
        logger.info("   📊 포스트별 필터 결과 분석...")
        
        for i, submission in enumerate(subreddit.new(limit=20), 1):
            filter_result = reddit_collector._passes_submission_filter(submission)
            
            if filter_result:
                passed_count += 1
                logger.info(f"      ✅ [{i}] PASS: {submission.title[:40]}... (점수:{submission.score}, 댓글:{submission.num_comments})")
            else:
                failed_count += 1
                logger.info(f"      ❌ [{i}] FAIL: {submission.title[:40]}... (점수:{submission.score}, 댓글:{submission.num_comments})")
        
        logger.info(f"\n   📈 필터 통계:")
        logger.info(f"      • 통과: {passed_count}개")
        logger.info(f"      • 실패: {failed_count}개")
        logger.info(f"      • 통과율: {(passed_count / (passed_count + failed_count) * 100):.1f}%")
        
        return passed_count, failed_count
        
    except Exception as e:
        logger.error(f"   ❌ 필터 테스트 실패: {e}")
        return 0, 0

if __name__ == "__main__":
    logger.info("🚀 Reddit 수집 문제 진단 시작")
    logger.info("=" * 80)
    
    # 기본 수집 테스트
    basic_result = asyncio.run(test_reddit_basic_collection())
    
    # 필터 조정 테스트
    passed, failed = asyncio.run(test_reddit_filter_adjustment())
    
    logger.info("\n" + "🏁" * 30)
    logger.info("📊 Reddit 수집 진단 완료")
    logger.info(f"   • 기본 수집 결과: {basic_result}개")
    logger.info(f"   • 필터 통과율: {(passed / (passed + failed) * 100) if (passed + failed) > 0 else 0:.1f}%")
    
    if basic_result == 0:
        logger.warning("⚠️  Reddit 수집 문제 발견 - 필터 조건을 완화해야 합니다.")
    else:
        logger.info("✅ Reddit 수집 정상 작동")