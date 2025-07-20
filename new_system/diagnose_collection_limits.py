#!/usr/bin/env python3
"""
수집 한계 진단 도구 - 왜 20개에서 멈추는지 확인
"""
import asyncio
import logging
from pathlib import Path
import sys
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from collectors.stackoverflow_collector import StackOverflowCollector
from collectors.reddit_collector import RedditCollector
from core.cache import APICache, LocalCache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def diagnose_stackoverflow_limits():
    """Stack Overflow 수집 한계 진단"""
    
    logger.info("📚 Stack Overflow 수집 한계 진단")
    logger.info("=" * 70)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/so_limit_test.db"))
    cache = APICache(local_cache)
    
    so_collector = StackOverflowCollector(cache)
    
    # 최근 5년간으로 범위 확대 (사용자 요청)
    from_date = datetime.now() - timedelta(days=365*5)
    
    logger.info(f"   📅 수집 기간: {from_date.strftime('%Y-%m-%d')} ~ 현재")
    logger.info(f"   🎯 최대 페이지: 20 (진단용)")
    
    try:
        # 더 많은 페이지로 테스트
        so_data = await so_collector.collect_excel_questions(
            from_date=from_date,
            max_pages=20  # 더 많이 시도
        )
        
        logger.info(f"   ✅ Stack Overflow 수집 결과: {len(so_data)}개")
        
        # 각 페이지별 수집량 시뮬레이션
        page_sizes = []
        total = 0
        for i in range(1, min(len(so_data) // 2 + 1, 11)):  # 대략적인 페이지 추정
            page_size = min(len(so_data) - total, 100)
            if page_size <= 0:
                break
            page_sizes.append(page_size)
            total += page_size
            logger.info(f"      페이지 {i}: ~{page_size}개")
        
        if len(so_data) < 20:
            logger.warning(f"   ⚠️  Stack Overflow에서 {len(so_data)}개만 수집됨 (20개 미달)")
            logger.info("      원인: 최근 Excel 질문이 적거나 API 제한")
        
        return len(so_data)
        
    except Exception as e:
        logger.error(f"   ❌ Stack Overflow 진단 실패: {e}")
        return 0

async def diagnose_reddit_limits():
    """Reddit 수집 한계 진단"""
    
    logger.info("\n🟠 Reddit 수집 한계 진단")
    logger.info("=" * 70)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/reddit_limit_test.db"))
    cache = APICache(local_cache)
    
    reddit_collector = RedditCollector(cache)
    
    logger.info(f"   🎯 'solved' 플레어만 수집 중...")
    
    try:
        # Reddit 직접 확인
        subreddit = reddit_collector.reddit.subreddit('excel')
        
        solved_count = 0
        total_checked = 0
        
        # 최근 100개 포스트에서 'solved' 찾기
        for submission in subreddit.new(limit=100):
            total_checked += 1
            flair = submission.link_flair_text or "No Flair"
            
            if flair.lower() == 'solved':
                solved_count += 1
                if solved_count <= 5:  # 처음 5개만 출력
                    logger.info(f"      [{solved_count}] {submission.title[:40]}... (점수:{submission.score})")
        
        logger.info(f"   📊 'solved' 현황:")
        logger.info(f"      • 확인한 포스트: {total_checked}개")
        logger.info(f"      • 'solved' 플레어: {solved_count}개")
        logger.info(f"      • 비율: {(solved_count / total_checked * 100):.1f}%")
        
        # 실제 수집 테스트
        reddit_data = await reddit_collector.collect_excel_discussions(max_submissions=20)
        
        logger.info(f"   ✅ Reddit 수집 결과: {len(reddit_data)}개")
        
        if len(reddit_data) < 10:
            logger.warning(f"   ⚠️  Reddit에서 {len(reddit_data)}개만 수집됨 (적음)")
            logger.info("      원인: 'solved' 플레어만 수집하도록 제한됨")
            logger.info("      해결책: 다른 플레어도 포함하거나 필터 완화")
        
        return len(reddit_data), solved_count
        
    except Exception as e:
        logger.error(f"   ❌ Reddit 진단 실패: {e}")
        return 0, 0

async def suggest_solutions():
    """해결책 제안"""
    
    logger.info("\n💡 수집량 증대 해결책")
    logger.info("=" * 70)
    
    logger.info("1. 📅 수집 기간 확대:")
    logger.info("   • 현재: 최근 7일")
    logger.info("   • 개선: 최근 5년 (사용자 요청으로 적용)")
    logger.info("   • 권장: 최근 1-2년 (데이터 신선도 유지)")
    
    logger.info("\n2. 🟠 Reddit 필터 유지:")
    logger.info("   • 현재: 'solved' 플레어만 (사용자 요청)")
    logger.info("   • 검색 기간: 5년으로 확대하여 더 많은 solved 포스트 확보")
    
    logger.info("\n3. 📚 Stack Overflow 태그 확대:")
    logger.info("   • 현재: 'excel-formula'만")
    logger.info("   • 개선: 'excel', 'vba', 'excel-vba' 추가")
    
    logger.info("\n4. ⚙️  대시보드 설정 조정:")
    logger.info("   • max_pages: 10 → 50")
    logger.info("   • target_count: 100 → 500")

async def main():
    """Main diagnostic routine"""
    logger.info("🔍 수집 한계 진단 시작")
    logger.info("=" * 80)
    
    # Stack Overflow 진단
    so_count = await diagnose_stackoverflow_limits()
    
    # Reddit 진단
    reddit_count, reddit_available = await diagnose_reddit_limits()
    
    # 해결책 제안
    await suggest_solutions()
    
    # 최종 요약
    total_collected = so_count + reddit_count
    
    logger.info("\n" + "📊" * 30)
    logger.info("진단 결과 요약:")
    logger.info(f"   • Stack Overflow: {so_count}개")
    logger.info(f"   • Reddit: {reddit_count}개 (사용가능: {reddit_available}개)")
    logger.info(f"   • 총 수집: {total_collected}개")
    
    if total_collected < 50:
        logger.warning("⚠️  수집량이 부족합니다. 위 해결책을 적용하세요.")
    else:
        logger.info("✅ 수집량이 충분합니다.")

if __name__ == "__main__":
    asyncio.run(main())