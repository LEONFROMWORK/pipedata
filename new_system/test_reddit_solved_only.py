#!/usr/bin/env python3
"""
Reddit 'solved' 플레어만 수집하는 테스트
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

async def test_reddit_solved_only():
    """Reddit 'solved' 플레어만 수집 테스트"""
    
    logger.info("🟠 Reddit 'solved' 플레어만 수집 테스트")
    logger.info("=" * 70)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/reddit_solved_test.db"))
    cache = APICache(local_cache)
    
    # Reddit collector 초기화
    reddit_collector = RedditCollector(cache)
    
    try:
        # 최신 포스트에서 'solved' 플레어 찾기
        logger.info("   🔍 r/excel에서 'solved' 플레어 포스트 검색 중...")
        
        subreddit = reddit_collector.reddit.subreddit('excel')
        
        solved_count = 0
        total_checked = 0
        solved_posts = []
        
        # 많은 포스트를 확인해서 'solved' 찾기
        for submission in subreddit.new(limit=50):
            total_checked += 1
            flair = submission.link_flair_text or "No Flair"
            
            if flair.lower() == 'solved':
                solved_count += 1
                solved_posts.append({
                    'title': submission.title,
                    'flair': flair,
                    'score': submission.score,
                    'comments': submission.num_comments,
                    'id': submission.id
                })
                logger.info(f"      ✅ [{solved_count}] {submission.title[:50]}... (점수:{submission.score}, 댓글:{submission.num_comments})")
        
        logger.info(f"\n   📊 'solved' 플레어 현황:")
        logger.info(f"      • 총 확인한 포스트: {total_checked}개")
        logger.info(f"      • 'solved' 플레어: {solved_count}개")
        logger.info(f"      • 발견율: {(solved_count / total_checked * 100):.1f}%")
        
        # 실제 수집기로 테스트
        logger.info(f"\n   ⚙️  RedditCollector로 'solved'만 수집 테스트...")
        
        result = await reddit_collector.collect_excel_discussions(max_submissions=5)
        
        logger.info(f"   ✅ 수집 결과: {len(result)}개")
        
        # 수집된 결과 확인
        for i, item in enumerate(result, 1):
            submission = item.submission
            title = submission.get('title', 'Unknown')
            flair = submission.get('link_flair_text', 'No Flair')
            logger.info(f"      [{i}] {title[:50]}... (플레어: {flair})")
        
        return len(result), solved_count
        
    except Exception as e:
        logger.error(f"   ❌ Reddit 'solved' 수집 테스트 실패: {e}")
        return 0, 0

if __name__ == "__main__":
    logger.info("🚀 Reddit 'solved' 플레어 전용 수집 테스트")
    logger.info("=" * 80)
    
    collected, available = asyncio.run(test_reddit_solved_only())
    
    logger.info("\n" + "🏁" * 30)
    logger.info("📊 Reddit 'solved' 수집 테스트 완료")
    logger.info(f"   • 수집된 'solved' 포스트: {collected}개")
    logger.info(f"   • 사용 가능한 'solved' 포스트: {available}개")
    
    if collected > 0:
        logger.info("✅ Reddit 'solved' 플레어 수집 성공!")
    else:
        logger.warning("⚠️  'solved' 플레어 포스트를 찾지 못했습니다.")
        logger.info("   💡 r/excel에서 'solved' 플레어가 적거나 다른 표기법을 사용할 수 있습니다.")