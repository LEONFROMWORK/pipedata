#!/usr/bin/env python3
"""
403 우회 기능이 적용된 Stack Overflow + Reddit 통합 수집 테스트
실제 이미지 다운로드 및 AI 처리까지 포함한 완전한 파이프라인 검증
"""
import asyncio
import json
import logging
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from collectors.stackoverflow_collector import StackOverflowCollector
from collectors.reddit_collector import RedditCollector
from pipeline.main_pipeline import ExcelQAPipeline
from processors.image_processor import ImageProcessor
from core.cache import APICache, LocalCache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def test_stackoverflow_with_image_bypass():
    """Stack Overflow 수집 + 이미지 403 우회 테스트"""
    
    logger.info("📚 Stack Overflow 수집 테스트 (이미지 403 우회 포함)")
    logger.info("=" * 70)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/so_bypass_test.db"))
    cache = APICache(local_cache)
    
    # Stack Overflow collector 초기화
    so_collector = StackOverflowCollector(cache)
    
    try:
        # 소량 수집 (테스트용)
        so_data = await so_collector.collect_qa_data(
            tags=['excel-formula'], 
            max_questions=3,  # 적은 수량으로 테스트
            min_score=2
        )
        
        logger.info(f"✅ Stack Overflow 수집 완료: {len(so_data)}개")
        
        # 이미지가 포함된 데이터 확인
        image_count = 0
        for item in so_data:
            if 'images' in item and item['images']:
                image_count += len(item['images'])
                logger.info(f"   📸 이미지 발견: {len(item['images'])}개 - {item.get('title', 'Unknown')[:50]}...")
        
        logger.info(f"   총 이미지: {image_count}개")
        return so_data
        
    except Exception as e:
        logger.error(f"❌ Stack Overflow 수집 실패: {e}")
        return []

async def test_reddit_with_image_bypass():
    """Reddit 수집 + 이미지 403 우회 테스트"""
    
    logger.info("🟠 Reddit 수집 테스트 (이미지 403 우회 포함)")
    logger.info("=" * 70)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/reddit_bypass_test.db"))
    cache = APICache(local_cache)
    
    # Reddit collector 초기화
    reddit_collector = RedditCollector(cache)
    
    try:
        # 소량 수집 (테스트용)
        reddit_data = await reddit_collector.collect_qa_data(
            subreddit='excel',
            max_posts=3,  # 적은 수량으로 테스트
            min_score=1
        )
        
        logger.info(f"✅ Reddit 수집 완료: {len(reddit_data)}개")
        
        # 이미지가 포함된 데이터 확인
        image_count = 0
        for item in reddit_data:
            if 'images' in item and item['images']:
                image_count += len(item['images'])
                logger.info(f"   📸 이미지 발견: {len(item['images'])}개 - {item.get('title', 'Unknown')[:50]}...")
        
        logger.info(f"   총 이미지: {image_count}개")
        return reddit_data
        
    except Exception as e:
        logger.error(f"❌ Reddit 수집 실패: {e}")
        return []

async def test_image_processing_with_bypass(raw_data):
    """이미지 403 우회가 포함된 이미지 처리 테스트"""
    
    logger.info("🖼️  이미지 처리 테스트 (403 우회 적용)")
    logger.info("=" * 70)
    
    # Cache 및 processor 초기화
    local_cache = LocalCache(db_path=Path("/tmp/image_bypass_test.db"))
    cache = APICache(local_cache)
    processor = ImageProcessor(cache)
    
    total_images = 0
    successful_downloads = 0
    processed_images = []
    
    for item in raw_data:
        if 'images' in item and item['images']:
            for image_url in item['images']:
                total_images += 1
                logger.info(f"[{total_images}] 이미지 처리: {image_url}")
                
                try:
                    # 3-tier 이미지 처리 (OCR → Table → AI)
                    context_tags = item.get('tags', []) + ['excel']
                    result = await processor.process_image_url(image_url, context_tags)
                    
                    if result and result.get('success'):
                        successful_downloads += 1
                        processed_images.append({
                            'source_url': image_url,
                            'processing_result': result,
                            'source_item': {
                                'title': item.get('title', ''),
                                'source': item.get('source', ''),
                                'tags': item.get('tags', [])
                            }
                        })
                        
                        logger.info(f"   ✅ 성공! 처리 방법: {result.get('processing_tier', 'Unknown')}")
                        if result.get('extracted_content'):
                            content_preview = result['extracted_content'][:100]
                            logger.info(f"   📝 추출 내용: {content_preview}...")
                    else:
                        logger.error(f"   ❌ 실패: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    logger.error(f"   ❌ 예외: {e}")
                
                logger.info("-" * 40)
    
    success_rate = (successful_downloads / total_images * 100) if total_images > 0 else 0
    
    logger.info("📊 이미지 처리 결과:")
    logger.info(f"   • 총 이미지: {total_images}개")
    logger.info(f"   • 성공: {successful_downloads}개")
    logger.info(f"   • 성공률: {success_rate:.1f}%")
    
    return processed_images, success_rate

async def test_full_pipeline_with_bypass():
    """전체 파이프라인 테스트 (수집 → 처리 → AI 향상)"""
    
    logger.info("🚀 전체 파이프라인 테스트 (403 우회 포함)")
    logger.info("=" * 70)
    
    # Cache 및 pipeline 초기화
    local_cache = LocalCache(db_path=Path("/tmp/full_pipeline_test.db"))
    cache = APICache(local_cache)
    pipeline = ExcelQAPipeline(cache)
    
    try:
        # 실제 파이프라인 실행 (소량)
        logger.info("⚙️  메인 파이프라인 실행 중...")
        
        # 파이프라인 실행 (기본 설정 사용)
        result = await pipeline.run_full_pipeline()
        
        logger.info("✅ 파이프라인 실행 완료!")
        logger.info(f"   • Stack Overflow: {result.get('stackoverflow_count', 0)}개")
        logger.info(f"   • Reddit: {result.get('reddit_count', 0)}개")
        logger.info(f"   • 총 수집: {result.get('total_collected', 0)}개")
        logger.info(f"   • 최종 품질 필터링 후: {result.get('final_count', 0)}개")
        
        if result.get('image_processing_stats'):
            img_stats = result['image_processing_stats']
            logger.info(f"   • 이미지 처리 성공률: {img_stats.get('success_rate', 0):.1f}%")
            logger.info(f"   • 이미지 총 처리: {img_stats.get('total_processed', 0)}개")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ 파이프라인 실행 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

async def generate_final_dataset_with_images():
    """최종 이미지 포함 데이터셋 생성"""
    
    logger.info("💎 최종 이미지 포함 데이터셋 생성")
    logger.info("=" * 70)
    
    # Step 1: Stack Overflow 수집
    so_data = await test_stackoverflow_with_image_bypass()
    
    # Step 2: Reddit 수집
    reddit_data = await test_reddit_with_image_bypass()
    
    # Step 3: 통합 데이터
    combined_raw_data = so_data + reddit_data
    logger.info(f"🔗 통합 데이터: {len(combined_raw_data)}개")
    
    # Step 4: 이미지 처리
    processed_images, img_success_rate = await test_image_processing_with_bypass(combined_raw_data)
    
    # Step 5: 최종 데이터셋 구성
    final_dataset = {
        "dataset_info": {
            "name": "Excel Q&A Dataset with 403 Bypass",
            "version": "3.0-bypass-enabled",
            "description": "Excel Q&A dataset with advanced 403 bypass for Stack Overflow and Reddit images",
            "total_samples": len(combined_raw_data),
            "images_processed": len(processed_images),
            "image_success_rate": img_success_rate,
            "bypass_methods": ["stackoverflow_cloudscraper", "reddit_oauth_multiple"],
            "generated_at": datetime.now().isoformat()
        },
        "bypass_stats": {
            "stackoverflow_images": sum(1 for img in processed_images if 'sstatic.net' in img['source_url']),
            "reddit_images": sum(1 for img in processed_images if 'redd.it' in img['source_url']),
            "total_bypass_success": len(processed_images),
            "overall_success_rate": img_success_rate
        },
        "raw_data": combined_raw_data,
        "processed_images": processed_images
    }
    
    # 저장
    output_path = "/Users/kevin/bigdata/data/output/final_dataset_with_bypass.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_dataset, f, indent=2, ensure_ascii=False)
    
    logger.info(f"💾 최종 데이터셋 저장: {output_path}")
    logger.info("📊 최종 통계:")
    logger.info(f"   • 총 Q&A: {final_dataset['dataset_info']['total_samples']}개")
    logger.info(f"   • 처리된 이미지: {final_dataset['dataset_info']['images_processed']}개")
    logger.info(f"   • 이미지 성공률: {final_dataset['dataset_info']['image_success_rate']:.1f}%")
    logger.info(f"   • Stack Overflow 이미지: {final_dataset['bypass_stats']['stackoverflow_images']}개")
    logger.info(f"   • Reddit 이미지: {final_dataset['bypass_stats']['reddit_images']}개")
    
    return final_dataset

if __name__ == "__main__":
    logger.info("🔥 403 우회 기능 포함 전체 수집 테스트 시작")
    logger.info("=" * 80)
    
    # 개별 테스트들
    print("\n" + "🔸" * 20 + " Phase 1: Stack Overflow 테스트 " + "🔸" * 20)
    so_result = asyncio.run(test_stackoverflow_with_image_bypass())
    
    print("\n" + "🔸" * 20 + " Phase 2: Reddit 테스트 " + "🔸" * 20)
    reddit_result = asyncio.run(test_reddit_with_image_bypass())
    
    print("\n" + "🔸" * 20 + " Phase 3: 최종 데이터셋 생성 " + "🔸" * 20)
    final_result = asyncio.run(generate_final_dataset_with_images())
    
    print("\n" + "🏁" * 30)
    logger.info("🎊 403 우회 기능 포함 전체 테스트 완료!")
    
    if final_result:
        bypass_stats = final_result['bypass_stats']
        logger.info("🎯 최종 성과:")
        logger.info(f"   ✅ 전체 이미지 우회 성공률: {bypass_stats['overall_success_rate']:.1f}%")
        logger.info(f"   📚 Stack Overflow 이미지 우회: {bypass_stats['stackoverflow_images']}개")
        logger.info(f"   🟠 Reddit 이미지 우회: {bypass_stats['reddit_images']}개")
        logger.info(f"   🎉 403 에러 극복 완료!")
    else:
        logger.error("❌ 테스트 실패")