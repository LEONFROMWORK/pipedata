#!/usr/bin/env python3
"""
실제 tesseract와 함께 이미지 처리 테스트 (OpenRouter 없이 OCR/테이블만)
"""
import asyncio
import json
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

class MockImageProcessor(ImageProcessor):
    """ImageProcessor with OpenRouter disabled for testing OCR/table only"""
    
    def _should_use_ai_enhancement(self, ocr_result, table_result, context_tags):
        """Always return False to skip AI enhancement for testing"""
        return False

async def test_real_image_processing():
    """실제 tesseract와 img2table로 이미지 처리 테스트"""
    
    # Initialize cache and processor
    local_cache = LocalCache(db_path=Path("/tmp/real_image_test_cache.db"))
    cache = APICache(local_cache)
    processor = MockImageProcessor(cache)
    
    # 실제 스택오버플로우 이미지 URLs (403 우회 검증된)
    test_images = [
        {
            "url": "https://i.sstatic.net/AJr6APk8.png",
            "description": "Excel 복잡한 공식 결과 스크린샷",
            "expected_type": "formula_result"
        },
        {
            "url": "https://i.sstatic.net/TpgieF6J.png", 
            "description": "Excel 파스칼 삼각형 배열 공식",
            "expected_type": "table_data"
        },
        {
            "url": "https://i.sstatic.net/82NP1kgT.png",
            "description": "Excel 필터 공식 결과",
            "expected_type": "filtered_data"
        }
    ]
    
    logger.info("🔬 실제 tesseract OCR + img2table 테스트 시작")
    logger.info("=" * 70)
    
    processed_results = []
    
    for i, test_image in enumerate(test_images, 1):
        logger.info(f"[{i}/{len(test_images)}] 처리 중: {test_image['description']}")
        logger.info(f"URL: {test_image['url']}")
        
        try:
            # Excel 관련 컨텍스트 태그
            context_tags = ["excel", "formula", "table", test_image["expected_type"]]
            
            # 이미지 처리 실행
            result = await processor.process_image_url(test_image["url"], context_tags)
            
            # 결과 저장
            processed_result = {
                "test_info": {
                    "url": test_image["url"],
                    "description": test_image["description"],
                    "expected_type": test_image["expected_type"]
                },
                "processing_result": result
            }
            processed_results.append(processed_result)
            
            # 결과 출력
            logger.info("=" * 50)
            if result['success']:
                logger.info("✅ 처리 성공!")
                logger.info(f"   • 처리 단계: {result.get('processing_steps', [])}")
                logger.info(f"   • 처리 티어: {result.get('processing_tier', 'Unknown')}")
                logger.info(f"   • 콘텐츠 타입: {result.get('extracted_content_type', 'None')}")
                logger.info(f"   • 추출된 텍스트 길이: {len(result.get('extracted_content', ''))} 문자")
                
                # 추출된 내용 미리보기 (처음 200자)
                content = result.get('extracted_content', '')
                if content:
                    preview = content[:200] + "..." if len(content) > 200 else content
                    logger.info(f"   • 내용 미리보기: {repr(preview)}")
                else:
                    logger.warning("   • 추출된 내용 없음")
            else:
                logger.error(f"❌ 처리 실패: {result.get('error', 'Unknown error')}")
            
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"❌ 예외 발생: {e}")
            processed_results.append({
                "test_info": test_image,
                "processing_result": {"success": False, "error": str(e)}
            })
    
    # 최종 결과 JSON 생성
    final_result = {
        "test_metadata": {
            "test_name": "Real Image Processing with tesseract + img2table",
            "test_date": "2025-07-18",
            "total_images": len(test_images),
            "processing_pipeline": "Tier 1 (tesseract) + Tier 2 (img2table) only",
            "ai_enhancement": "disabled_for_testing"
        },
        "results": processed_results,
        "summary": {
            "total_processed": len(processed_results),
            "successful": sum(1 for r in processed_results if r["processing_result"].get("success", False)),
            "failed": sum(1 for r in processed_results if not r["processing_result"].get("success", False))
        }
    }
    
    # JSON 출력
    logger.info("\n" + "=" * 70)
    logger.info("📋 최종 처리 결과 JSON:")
    logger.info("=" * 70)
    print(json.dumps(final_result, indent=2, ensure_ascii=False))
    
    # 파일 저장
    output_path = "/Users/kevin/bigdata/data/output/real_image_processing_results.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_result, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\n💾 결과 저장: {output_path}")
    
    # 요약 통계
    summary = final_result["summary"]
    success_rate = (summary["successful"] / summary["total_processed"]) * 100 if summary["total_processed"] > 0 else 0
    
    logger.info(f"\n📊 처리 통계:")
    logger.info(f"   • 총 처리: {summary['total_processed']}개")
    logger.info(f"   • 성공: {summary['successful']}개")
    logger.info(f"   • 실패: {summary['failed']}개")
    logger.info(f"   • 성공률: {success_rate:.1f}%")
    
    return final_result

if __name__ == "__main__":
    asyncio.run(test_real_image_processing())