#!/usr/bin/env python3
"""
간단한 403 우회 테스트 - Stack Overflow + Reddit 수집 및 이미지 처리
"""
import asyncio
import json
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from collectors.stackoverflow_collector import StackOverflowCollector
from collectors.reddit_collector import RedditCollector
from processors.image_processor import ImageProcessor
from core.cache import APICache, LocalCache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def simple_collection_test():
    """간단한 수집 및 이미지 우회 테스트"""
    
    logger.info("🚀 간단한 403 우회 테스트 시작")
    logger.info("=" * 70)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/simple_test.db"))
    cache = APICache(local_cache)
    
    # 결과 저장
    all_results = {
        "stackoverflow": {"data": [], "images": []},
        "reddit": {"data": [], "images": []},
        "image_processing": {"total": 0, "success": 0, "results": []}
    }
    
    # 1. Stack Overflow 수집
    logger.info("📚 Stack Overflow 수집 중...")
    try:
        so_collector = StackOverflowCollector(cache)
        so_data = await so_collector.collect_excel_questions(
            max_pages=1  # 소량 테스트 - 1페이지만
        )
        
        all_results["stackoverflow"]["data"] = so_data
        logger.info(f"✅ Stack Overflow: {len(so_data)}개 수집")
        
        # 이미지 URL 추출
        for item in so_data:
            if 'images' in item and item['images']:
                all_results["stackoverflow"]["images"].extend(item['images'])
                
        logger.info(f"   📸 Stack Overflow 이미지: {len(all_results['stackoverflow']['images'])}개")
        
    except Exception as e:
        logger.error(f"❌ Stack Overflow 수집 실패: {e}")
    
    # 2. Reddit 수집
    logger.info("🟠 Reddit 수집 중...")
    try:
        reddit_collector = RedditCollector(cache)
        reddit_data = await reddit_collector.collect_excel_discussions(
            max_submissions=2  # 소량 테스트
        )
        
        all_results["reddit"]["data"] = reddit_data
        logger.info(f"✅ Reddit: {len(reddit_data)}개 수집")
        
        # 이미지 URL 추출
        for item in reddit_data:
            if 'images' in item and item['images']:
                all_results["reddit"]["images"].extend(item['images'])
                
        logger.info(f"   📸 Reddit 이미지: {len(all_results['reddit']['images'])}개")
        
    except Exception as e:
        logger.error(f"❌ Reddit 수집 실패: {e}")
    
    # 3. 이미지 처리 (403 우회 포함)
    logger.info("🖼️  이미지 403 우회 테스트...")
    
    all_images = all_results["stackoverflow"]["images"] + all_results["reddit"]["images"]
    processor = ImageProcessor(cache)
    
    for i, image_url in enumerate(all_images, 1):
        all_results["image_processing"]["total"] += 1
        
        logger.info(f"[{i}/{len(all_images)}] 이미지 처리: {image_url}")
        
        try:
            # 이미지 다운로드 및 처리
            result = await processor.process_image_url(image_url, ['excel'])
            
            if result and result.get('success'):
                all_results["image_processing"]["success"] += 1
                all_results["image_processing"]["results"].append({
                    "url": image_url,
                    "success": True,
                    "processing_tier": result.get('processing_tier', ''),
                    "content_length": len(result.get('extracted_content', '')),
                    "source": "stackoverflow" if "sstatic.net" in image_url else "reddit"
                })
                
                logger.info(f"   ✅ 성공! {result.get('processing_tier', 'Unknown')}")
                if result.get('extracted_content'):
                    preview = result['extracted_content'][:80].replace('\n', ' ')
                    logger.info(f"   📝 추출: {preview}...")
            else:
                all_results["image_processing"]["results"].append({
                    "url": image_url,
                    "success": False,
                    "error": result.get('error', 'Unknown'),
                    "source": "stackoverflow" if "sstatic.net" in image_url else "reddit"
                })
                logger.error(f"   ❌ 실패: {result.get('error', 'Unknown')}")
                
        except Exception as e:
            logger.error(f"   ❌ 예외: {e}")
            all_results["image_processing"]["results"].append({
                "url": image_url,
                "success": False,
                "error": str(e),
                "source": "stackoverflow" if "sstatic.net" in image_url else "reddit"
            })
        
        logger.info("-" * 50)
    
    # 4. 결과 정리 및 저장
    success_rate = (all_results["image_processing"]["success"] / 
                   all_results["image_processing"]["total"] * 100) if all_results["image_processing"]["total"] > 0 else 0
    
    # 소스별 성공률 계산
    so_success = sum(1 for r in all_results["image_processing"]["results"] 
                    if r["source"] == "stackoverflow" and r["success"])
    so_total = sum(1 for r in all_results["image_processing"]["results"] 
                  if r["source"] == "stackoverflow")
    
    reddit_success = sum(1 for r in all_results["image_processing"]["results"] 
                        if r["source"] == "reddit" and r["success"])
    reddit_total = sum(1 for r in all_results["image_processing"]["results"] 
                      if r["source"] == "reddit")
    
    so_rate = (so_success / so_total * 100) if so_total > 0 else 0
    reddit_rate = (reddit_success / reddit_total * 100) if reddit_total > 0 else 0
    
    # 최종 보고서
    final_report = {
        "test_summary": {
            "total_qa_collected": len(all_results["stackoverflow"]["data"]) + len(all_results["reddit"]["data"]),
            "stackoverflow_qa": len(all_results["stackoverflow"]["data"]),
            "reddit_qa": len(all_results["reddit"]["data"]),
            "total_images": all_results["image_processing"]["total"],
            "successful_images": all_results["image_processing"]["success"],
            "overall_success_rate": success_rate
        },
        "bypass_effectiveness": {
            "stackoverflow": {
                "images_tested": so_total,
                "images_successful": so_success,
                "success_rate": so_rate
            },
            "reddit": {
                "images_tested": reddit_total,
                "images_successful": reddit_success,
                "success_rate": reddit_rate
            }
        },
        "detailed_results": all_results
    }
    
    # 파일로 저장
    output_path = "/Users/kevin/bigdata/data/output/bypass_test_results.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_report, f, indent=2, ensure_ascii=False)
    
    # 결과 출력
    logger.info("=" * 70)
    logger.info("📊 최종 테스트 결과:")
    logger.info(f"   • 총 Q&A 수집: {final_report['test_summary']['total_qa_collected']}개")
    logger.info(f"     - Stack Overflow: {final_report['test_summary']['stackoverflow_qa']}개")
    logger.info(f"     - Reddit: {final_report['test_summary']['reddit_qa']}개")
    logger.info(f"   • 총 이미지 테스트: {final_report['test_summary']['total_images']}개")
    logger.info(f"   • 이미지 처리 성공: {final_report['test_summary']['successful_images']}개")
    logger.info(f"   • 전체 성공률: {final_report['test_summary']['overall_success_rate']:.1f}%")
    
    logger.info("")
    logger.info("🎯 소스별 403 우회 성과:")
    logger.info(f"   📚 Stack Overflow: {final_report['bypass_effectiveness']['stackoverflow']['success_rate']:.1f}% ({final_report['bypass_effectiveness']['stackoverflow']['images_successful']}/{final_report['bypass_effectiveness']['stackoverflow']['images_tested']})")
    logger.info(f"   🟠 Reddit: {final_report['bypass_effectiveness']['reddit']['success_rate']:.1f}% ({final_report['bypass_effectiveness']['reddit']['images_successful']}/{final_report['bypass_effectiveness']['reddit']['images_tested']})")
    
    logger.info(f"\n💾 상세 결과 저장: {output_path}")
    
    if success_rate >= 80:
        logger.info("🎉 403 우회 성공! 목표 달성!")
    elif success_rate >= 60:
        logger.info("✅ 403 우회 양호한 성과!")
    elif success_rate >= 40:
        logger.warning("⚠️  403 우회 부분적 성공")
    else:
        logger.error("❌ 403 우회 개선 필요")
    
    return final_report

if __name__ == "__main__":
    result = asyncio.run(simple_collection_test())