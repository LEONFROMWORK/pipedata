#!/usr/bin/env python3
"""
실제 이미지 포함 데이터 수집 및 AI 변환 테스트
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
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def test_image_processing_pipeline():
    """실제 이미지가 있는 Q&A 데이터 처리 테스트"""
    
    # Initialize cache and processor
    local_cache = LocalCache(db_path=Path("/tmp/image_test_cache.db"))
    cache = APICache(local_cache)
    processor = ImageProcessor(cache)
    
    # 현재 데이터셋에서 실제 이미지가 있는 데이터 (확인된 이미지 URL들)
    sample_qa_with_images = {
        "id": "excel_qa_test_image",
        "user_question": "Excel array formula that references previously calculated values within the array",
        "user_context": "I want to create Pascal's Triangle in Excel using array formulas...",
        "assistant_response": """
<p>The formula for Pascal's triangle is:</p>
<pre><code>=LET(N,5,REDUCE(SEQUENCE(,N,1,0),SEQUENCE(N-1),
    LAMBDA(y,z,VSTACK(y,SCAN(0,TAKE(y,-1),LAMBDA(a,x,a+x))))))
</code></pre>
<p><a href="https://i.sstatic.net/TpgieF6J.png" rel="nofollow noreferrer"><img src="https://i.sstatic.net/TpgieF6J.png" alt="enter image description here" /></a></p>
""",
        "image_urls": ["https://i.sstatic.net/TpgieF6J.png"],
        "metadata": {
            "difficulty": "advanced",
            "functions": ["LET", "LAMBDA", "REDUCE", "SEQUENCE"],
            "source": "stackoverflow"
        }
    }
    
    logger.info("🔄 실제 이미지 포함 Q&A 데이터 처리 시작")
    logger.info("=" * 60)
    
    # Process each image in the Q&A
    processed_images = []
    
    for image_url in sample_qa_with_images["image_urls"]:
        logger.info(f"📸 이미지 처리 중: {image_url}")
        
        try:
            # Excel 관련 태그 추가 (AI 처리 힌트)
            context_tags = sample_qa_with_images["metadata"]["functions"] + ["excel", "formula"]
            
            # 3-tier 이미지 처리 파이프라인 실행
            result = await processor.process_image_url(image_url, context_tags)
            
            processed_images.append({
                "original_url": image_url,
                "processing_result": result
            })
            
            # 처리 결과 요약
            if result['success']:
                logger.info(f"✅ 처리 성공:")
                logger.info(f"   • 처리 단계: {result['processing_tier']}")
                logger.info(f"   • 콘텐츠 타입: {result['extracted_content_type']}")
                logger.info(f"   • 추출된 텍스트 길이: {len(result['extracted_content'])} 문자")
                if result.get('ai_model_used'):
                    logger.info(f"   • 사용된 AI 모델: {result['ai_model_used']}")
                    logger.info(f"   • 토큰 사용량: {result.get('tokens_used', 0)}")
            else:
                logger.error(f"❌ 처리 실패: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ 이미지 처리 중 오류: {e}")
            processed_images.append({
                "original_url": image_url,
                "processing_result": {"success": False, "error": str(e)}
            })
    
    # 최종 JSON 형식으로 결합
    final_qa_with_processed_images = {
        "id": sample_qa_with_images["id"],
        "user_question": sample_qa_with_images["user_question"],
        "user_context": sample_qa_with_images["user_context"],
        "assistant_response": sample_qa_with_images["assistant_response"],
        "code_blocks": ["=LET(N,5,REDUCE(SEQUENCE(,N,1,0),SEQUENCE(N-1),", "    LAMBDA(y,z,VSTACK(y,SCAN(0,TAKE(y,-1),LAMBDA(a,x,a+x))))))"],
        "image_contexts": [],  # AI로 추출된 이미지 컨텍스트
        "metadata": sample_qa_with_images["metadata"]
    }
    
    # 이미지 처리 결과를 image_contexts에 추가
    for processed_image in processed_images:
        if processed_image["processing_result"]["success"]:
            image_context = {
                "source_url": processed_image["original_url"],
                "extracted_content": processed_image["processing_result"]["extracted_content"],
                "content_type": processed_image["processing_result"]["extracted_content_type"],
                "processing_method": processed_image["processing_result"]["processing_tier"],
                "ai_model": processed_image["processing_result"].get("ai_model_used"),
                "confidence": "high" if processed_image["processing_result"].get("ai_model_used") else "medium"
            }
            final_qa_with_processed_images["image_contexts"].append(image_context)
    
    logger.info("=" * 60)
    logger.info("📋 최종 JSON 결과:")
    print(json.dumps(final_qa_with_processed_images, indent=2, ensure_ascii=False))
    
    # 파일로도 저장
    output_path = "/Users/kevin/bigdata/data/output/sample_with_processed_images.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_qa_with_processed_images, f, indent=2, ensure_ascii=False)
    
    logger.info(f"💾 결과 저장됨: {output_path}")
    
    return final_qa_with_processed_images

if __name__ == "__main__":
    asyncio.run(test_image_processing_pipeline())