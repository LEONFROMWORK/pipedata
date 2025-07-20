#!/usr/bin/env python3
"""
API 한도 및 수집 중단 원인 분석 도구
"""
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys
import json

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

async def analyze_stackoverflow_limits():
    """Stack Overflow API 한도 분석"""
    
    logger.info("📊 Stack Overflow API 한도 분석")
    logger.info("=" * 70)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/so_limit_analysis.db"))
    cache = APICache(local_cache)
    
    so_collector = StackOverflowCollector(cache)
    
    # 현재 rate limiting 상태 확인
    logger.info(f"   📈 현재 요청 수: {so_collector.requests_today}")
    logger.info(f"   📅 일일 할당량 리셋: {so_collector.daily_quota_reset}")
    logger.info(f"   ⏰ 마지막 요청 시간: {so_collector.last_request_time}")
    
    # API 한도 설정 확인
    rate_config = so_collector.rate_config
    logger.info(f"   🔄 분당 요청 제한: {rate_config['requests_per_minute']}")
    logger.info(f"   📊 일일 요청 제한: {rate_config['max_requests_per_day']}")
    
    # 실제 API 테스트 (1페이지만)
    try:
        from_date = datetime.now() - timedelta(days=1)  # 최근 1일만
        
        logger.info(f"   🧪 API 테스트 시작...")
        test_data = await so_collector.collect_excel_questions(
            from_date=from_date,
            max_pages=1  # 1페이지만 테스트
        )
        
        logger.info(f"   ✅ API 테스트 성공: {len(test_data)}개 수집")
        logger.info(f"   📈 테스트 후 요청 수: {so_collector.requests_today}")
        
        return {
            'api_working': True,
            'requests_today': so_collector.requests_today,
            'test_collection_count': len(test_data),
            'daily_limit': rate_config['max_requests_per_day'],
            'per_minute_limit': rate_config['requests_per_minute']
        }
        
    except Exception as e:
        logger.error(f"   ❌ API 테스트 실패: {e}")
        
        # Rate limit 에러인지 확인
        if "rate limit" in str(e).lower() or "quota" in str(e).lower():
            logger.warning("   🚫 Rate limit에 도달한 것으로 보임")
            return {
                'api_working': False,
                'error_type': 'rate_limit',
                'error_message': str(e),
                'requests_today': so_collector.requests_today
            }
        else:
            return {
                'api_working': False,
                'error_type': 'other',
                'error_message': str(e)
            }

async def analyze_reddit_limits():
    """Reddit API 한도 분석"""
    
    logger.info("\n🟠 Reddit API 한도 분석")
    logger.info("=" * 70)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/reddit_limit_analysis.db"))
    cache = APICache(local_cache)
    
    reddit_collector = RedditCollector(cache)
    
    # Reddit API 정보
    logger.info(f"   🤖 User Agent: {reddit_collector.reddit.config.user_agent}")
    logger.info(f"   📊 현재 요청 수: {reddit_collector.requests_today}")
    logger.info(f"   ⏰ 마지막 요청 시간: {reddit_collector.last_request_time}")
    
    # Reddit API 테스트
    try:
        logger.info(f"   🧪 Reddit API 테스트 시작...")
        
        # 간단한 subreddit 확인
        subreddit = reddit_collector.reddit.subreddit('excel')
        logger.info(f"   📍 Subreddit: {subreddit.display_name}")
        logger.info(f"   👥 구독자: {subreddit.subscribers:,}")
        
        # 최근 'solved' 포스트 몇 개만 확인
        solved_count = 0
        total_checked = 0
        
        for submission in subreddit.new(limit=50):  # 50개만 확인
            total_checked += 1
            flair = submission.link_flair_text or "No Flair"
            
            if flair.lower() == 'solved':
                solved_count += 1
                if solved_count >= 5:  # 5개만 찾으면 중단
                    break
        
        logger.info(f"   ✅ Reddit API 테스트 성공")
        logger.info(f"   📊 확인한 포스트: {total_checked}개")
        logger.info(f"   🎯 'solved' 포스트: {solved_count}개")
        
        return {
            'api_working': True,
            'subscribers': subreddit.subscribers,
            'solved_posts_found': solved_count,
            'posts_checked': total_checked,
            'solved_ratio': solved_count / total_checked if total_checked > 0 else 0
        }
        
    except Exception as e:
        logger.error(f"   ❌ Reddit API 테스트 실패: {e}")
        
        # Rate limit 에러인지 확인
        if "rate limit" in str(e).lower() or "429" in str(e):
            logger.warning("   🚫 Reddit Rate limit에 도달한 것으로 보임")
            return {
                'api_working': False,
                'error_type': 'rate_limit',
                'error_message': str(e)
            }
        else:
            return {
                'api_working': False,
                'error_type': 'other',
                'error_message': str(e)
            }

async def analyze_collection_patterns():
    """수집 패턴 분석"""
    
    logger.info("\n📈 수집 패턴 분석")
    logger.info("=" * 70)
    
    # 중복 추적기에서 통계 확인
    tracker = get_global_tracker()
    stats = tracker.get_collection_stats(days=1)  # 최근 1일
    
    logger.info("   📊 최근 수집 통계:")
    if stats:
        so_stats = stats.get('stackoverflow', {})
        reddit_stats = stats.get('reddit', {})
        
        logger.info(f"      Stack Overflow: {so_stats.get('total_collected', 0)}개")
        logger.info(f"      Reddit: {reddit_stats.get('total_collected', 0)}개")
        
        if so_stats.get('last_collected'):
            logger.info(f"      마지막 SO 수집: {so_stats['last_collected']}")
        if reddit_stats.get('last_collected'):
            logger.info(f"      마지막 Reddit 수집: {reddit_stats['last_collected']}")
    else:
        logger.info("      통계 데이터 없음")
    
    # 현재 수집된 파일 확인
    output_file = Path("/Users/kevin/bigdata/data/output/year=2025/month=07/day=18/combined_20250718.jsonl")
    
    if output_file.exists():
        with open(output_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        logger.info(f"   📄 현재 출력 파일: {len(lines)}개 항목")
        
        # 소스별 분석
        stackoverflow_count = 0
        reddit_count = 0
        
        for line in lines:
            try:
                data = json.loads(line.strip())
                source = data.get('metadata', {}).get('source', 'unknown')
                if source == 'stackoverflow':
                    stackoverflow_count += 1
                elif source == 'reddit':
                    reddit_count += 1
            except:
                continue
        
        logger.info(f"      - Stack Overflow: {stackoverflow_count}개")
        logger.info(f"      - Reddit: {reddit_count}개")
        
        return {
            'total_items': len(lines),
            'stackoverflow_items': stackoverflow_count,
            'reddit_items': reddit_count
        }
    else:
        logger.info("   📄 출력 파일 없음")
        return {'total_items': 0}

async def suggest_api_solutions():
    """API 한도 해결책 제안"""
    
    logger.info("\n💡 API 한도 해결책")
    logger.info("=" * 70)
    
    logger.info("1. 📊 Stack Overflow API 최적화:")
    logger.info("   • 페이지 크기 감소: 100 → 50개")
    logger.info("   • 수집 간격 증가: 2초 → 5초")
    logger.info("   • 일일 할당량 모니터링 강화")
    
    logger.info("\n2. 🟠 Reddit API 최적화:")
    logger.info("   • PRAW 대신 Async PRAW 사용 고려")
    logger.info("   • 요청 간격 증가")
    logger.info("   • User-Agent 최적화")
    
    logger.info("\n3. 🔄 수집 전략 변경:")
    logger.info("   • 배치 수집 → 점진적 수집")
    logger.info("   • 시간대별 분산 수집")
    logger.info("   • 캐시 활용 극대화")
    
    logger.info("\n4. ⚙️  설정 조정:")
    logger.info("   • max_pages: 50 → 20")
    logger.info("   • target_count: 100 → 50")
    logger.info("   • rate_limit_delay: 2초 → 5초")

async def main():
    """메인 분석 루틴"""
    logger.info("🔍 API 한도 및 수집 중단 원인 분석")
    logger.info("=" * 80)
    
    # Stack Overflow 분석
    so_analysis = await analyze_stackoverflow_limits()
    
    # Reddit 분석
    reddit_analysis = await analyze_reddit_limits()
    
    # 수집 패턴 분석
    collection_analysis = await analyze_collection_patterns()
    
    # 해결책 제안
    await suggest_api_solutions()
    
    # 최종 진단
    logger.info("\n" + "🎯" * 30)
    logger.info("진단 결과:")
    
    if not so_analysis.get('api_working', True):
        logger.warning("❌ Stack Overflow API 문제 감지")
        if so_analysis.get('error_type') == 'rate_limit':
            logger.warning("   원인: Rate limit 초과")
        else:
            logger.warning(f"   원인: {so_analysis.get('error_message', 'Unknown')}")
    else:
        logger.info("✅ Stack Overflow API 정상")
    
    if not reddit_analysis.get('api_working', True):
        logger.warning("❌ Reddit API 문제 감지")
        if reddit_analysis.get('error_type') == 'rate_limit':
            logger.warning("   원인: Rate limit 초과")
        else:
            logger.warning(f"   원인: {reddit_analysis.get('error_message', 'Unknown')}")
    else:
        logger.info("✅ Reddit API 정상")
    
    total_items = collection_analysis.get('total_items', 0)
    if total_items < 20:
        logger.warning(f"⚠️  수집량 부족: {total_items}개만 수집됨")
        logger.info("   권장사항: API 한도 설정 조정 필요")
    else:
        logger.info(f"✅ 수집량 적절: {total_items}개")

if __name__ == "__main__":
    asyncio.run(main())