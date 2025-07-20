#!/usr/bin/env python3
"""
Reddit 이미지 403 우회 테스트
무조건 성공시키는 것이 목표
"""
import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from processors.image_processor import ImageProcessor
from core.cache import APICache, LocalCache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def test_reddit_403_bypass():
    """Reddit 403 우회 테스트 - 무조건 성공시키기"""
    
    # Initialize cache and processor
    local_cache = LocalCache(db_path=Path("/tmp/reddit_bypass_test.db"))
    cache = APICache(local_cache)
    processor = ImageProcessor(cache)
    
    # 실제 Reddit 이미지 URLs (이전에 실패했던 것들)
    reddit_test_urls = [
        'https://preview.redd.it/76mukstfxhdf1.png',
        'https://preview.redd.it/some_test_image.jpg?width=640&crop=smart&auto=webp&s=test123',
        'https://external-preview.redd.it/test_image.png?format=pjpg&auto=webp&s=example'
    ]
    
    logger.info("🚀 Reddit 이미지 403 우회 테스트 시작")
    logger.info("=" * 80)
    
    total_attempts = 0
    successful_downloads = 0
    
    for i, image_url in enumerate(reddit_test_urls, 1):
        logger.info(f"[{i}/{len(reddit_test_urls)}] Reddit 이미지 테스트")
        logger.info(f"URL: {image_url}")
        logger.info("-" * 60)
        
        total_attempts += 1
        
        try:
            # 고급 우회 기법으로 다운로드 시도
            image_path = await processor._download_image(image_url)
            
            if image_path and Path(image_path).exists():
                file_size = Path(image_path).stat().st_size
                successful_downloads += 1
                
                logger.info(f"✅ 성공! 이미지 다운로드 완료")
                logger.info(f"   • 저장 경로: {image_path}")
                logger.info(f"   • 파일 크기: {file_size:,} bytes")
                
                # 임시 파일 정리
                Path(image_path).unlink()
                
            else:
                logger.error(f"❌ 실패: 이미지 다운로드 불가")
                
        except Exception as e:
            logger.error(f"❌ 예외 발생: {e}")
            import traceback
            logger.debug(traceback.format_exc())
        
        logger.info("=" * 60)
    
    # Reddit 우회 통계 출력
    if hasattr(processor, 'reddit_bypasser'):
        reddit_stats = processor.reddit_bypasser.get_detailed_stats()
        logger.info("📊 Reddit 우회 상세 통계:")
        logger.info(f"   • Reddit 전용 성공률: {reddit_stats['success_rate']:.1f}%")
        logger.info(f"   • Reddit 전용 시도: {reddit_stats['total_attempts']}")
        logger.info(f"   • Reddit 전용 성공: {reddit_stats['successful_downloads']}")
        logger.info(f"   • 방법별 성공률: {reddit_stats['method_breakdown']}")
    
    # 전체 테스트 통계
    overall_success_rate = (successful_downloads / total_attempts * 100) if total_attempts > 0 else 0
    logger.info("")
    logger.info("🎯 전체 테스트 결과:")
    logger.info(f"   • 전체 성공률: {overall_success_rate:.1f}%")
    logger.info(f"   • 전체 시도: {total_attempts}")
    logger.info(f"   • 전체 성공: {successful_downloads}")
    
    if overall_success_rate >= 80:
        logger.info("🎉 Reddit 403 우회 성공! 목표 달성!")
    elif overall_success_rate >= 50:
        logger.warning("⚠️  부분적 성공 - 추가 개선 필요")
    else:
        logger.error("💥 우회 실패 - 더 강력한 방법 필요")
    
    return overall_success_rate

async def test_combined_image_download():
    """Stack Overflow + Reddit 통합 테스트"""
    
    local_cache = LocalCache(db_path=Path("/tmp/combined_test.db"))
    cache = APICache(local_cache)
    processor = ImageProcessor(cache)
    
    # 다양한 이미지 소스 테스트
    test_images = [
        # Stack Overflow (이미 검증됨)
        {
            "url": "https://i.sstatic.net/AJr6APk8.png",
            "source": "stackoverflow",
            "expected_success": True
        },
        {
            "url": "https://i.sstatic.net/TpgieF6J.png",
            "source": "stackoverflow", 
            "expected_success": True
        },
        # Reddit (새로운 우회 기법)
        {
            "url": "https://preview.redd.it/76mukstfxhdf1.png",
            "source": "reddit",
            "expected_success": True  # 목표
        }
    ]
    
    logger.info("🔥 Stack Overflow + Reddit 통합 이미지 다운로드 테스트")
    logger.info("=" * 80)
    
    results = {}
    
    for test_case in test_images:
        url = test_case["url"]
        source = test_case["source"]
        
        logger.info(f"🎯 {source.upper()} 이미지 테스트: {url}")
        
        try:
            image_path = await processor._download_image(url)
            
            if image_path and Path(image_path).exists():
                file_size = Path(image_path).stat().st_size
                logger.info(f"✅ 성공! {file_size:,} bytes")
                results[source] = results.get(source, 0) + 1
                
                # 정리
                Path(image_path).unlink()
            else:
                logger.error(f"❌ 실패")
                
        except Exception as e:
            logger.error(f"❌ 오류: {e}")
        
        logger.info("-" * 40)
    
    logger.info("📊 소스별 성공 통계:")
    for source, count in results.items():
        logger.info(f"   • {source}: {count}개 성공")
    
    total_success = sum(results.values())
    total_tests = len(test_images)
    overall_rate = (total_success / total_tests * 100) if total_tests > 0 else 0
    
    logger.info(f"   • 전체 성공률: {overall_rate:.1f}% ({total_success}/{total_tests})")
    
    return overall_rate

if __name__ == "__main__":
    # Reddit 전용 테스트
    logger.info("Phase 1: Reddit 전용 403 우회 테스트")
    reddit_rate = asyncio.run(test_reddit_403_bypass())
    
    print("\n" + "=" * 80)
    
    # 통합 테스트  
    logger.info("Phase 2: Stack Overflow + Reddit 통합 테스트")
    combined_rate = asyncio.run(test_combined_image_download())
    
    print("\n" + "=" * 80)
    logger.info("🏁 최종 결과:")
    logger.info(f"   • Reddit 우회 성공률: {reddit_rate:.1f}%")
    logger.info(f"   • 통합 테스트 성공률: {combined_rate:.1f}%")
    
    if reddit_rate >= 80 and combined_rate >= 80:
        logger.info("🎊 완벽한 성공! 모든 이미지 소스 우회 완료!")
    else:
        logger.warning("⚠️  일부 개선 필요")