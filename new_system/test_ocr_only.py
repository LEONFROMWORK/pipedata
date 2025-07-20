#!/usr/bin/env python3
"""
OCR만으로 실제 이미지 텍스트 추출 테스트
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

class OCROnlyImageProcessor(ImageProcessor):
    """OCR만 사용하는 간단한 이미지 프로세서"""
    
    def _should_use_ai_enhancement(self, ocr_result, table_result, context_tags):
        """AI 향상 비활성화"""
        return False
    
    async def _extract_tables_with_img2table(self, image_path):
        """img2table 비활성화 - 빈 결과 반환"""
        return {'tables_found': 0, 'markdown_content': '', 'raw_tables': []}

async def test_ocr_only_processing():
    """OCR만으로 이미지 텍스트 추출 테스트"""
    
    # Initialize cache and processor
    local_cache = LocalCache(db_path=Path("/tmp/ocr_test_cache.db"))
    cache = APICache(local_cache)
    processor = OCROnlyImageProcessor(cache)
    
    # 실제 스택오버플로우 이미지 URLs
    test_images = [
        {
            "url": "https://i.sstatic.net/AJr6APk8.png",
            "description": "Excel 복잡한 공식 결과",
            "qa_context": {
                "question": "Find all rows that have 2 out of 3 columns in common",
                "formula": "=LET(_data,A2:C7,TEXTSPLIT(...))"
            }
        },
        {
            "url": "https://i.sstatic.net/TpgieF6J.png", 
            "description": "Excel 파스칼 삼각형 배열",
            "qa_context": {
                "question": "Excel array formula that references previously calculated values",
                "formula": "=LET(N,5,REDUCE(SEQUENCE(,N,1,0)...))"
            }
        },
        {
            "url": "https://i.sstatic.net/82NP1kgT.png",
            "description": "Excel 필터 공식 결과",
            "qa_context": {
                "question": "Filter a table with a list of value with dynamic array formula",
                "formula": "=FILTER(E2#, ISNUMBER(MATCH(TAKE(E2#,,1),B2#,0)))"
            }
        }
    ]
    
    logger.info("🔍 OCR 전용 이미지 텍스트 추출 테스트")
    logger.info("=" * 70)
    
    final_qa_samples = []
    
    for i, test_image in enumerate(test_images, 1):
        logger.info(f"[{i}/{len(test_images)}] {test_image['description']}")
        logger.info(f"URL: {test_image['url']}")
        
        try:
            context_tags = ["excel", "formula", "screenshot"]
            result = await processor.process_image_url(test_image["url"], context_tags)
            
            if result['success'] and result.get('extracted_content'):
                # Q&A 샘플 생성
                qa_sample = {
                    "id": f"excel_qa_with_ocr_{i:03d}",
                    "user_question": test_image["qa_context"]["question"],
                    "user_context": f"I have this Excel screenshot showing the result. Please help me understand the formula: {test_image['qa_context']['formula']}",
                    "assistant_response": f"Looking at your screenshot, I can see the Excel formula result. The formula {test_image['qa_context']['formula']} produces the output shown in the image.",
                    "code_blocks": [test_image["qa_context"]["formula"]],
                    "image_contexts": [
                        {
                            "source_url": test_image["url"],
                            "extracted_content": result["extracted_content"],
                            "content_type": result["extracted_content_type"],
                            "processing_method": result["processing_tier"],
                            "word_count": len(result["extracted_content"].split()),
                            "confidence": "medium"  # OCR만으로는 medium
                        }
                    ],
                    "metadata": {
                        "difficulty": "advanced",
                        "functions": ["LET", "FILTER", "MATCH", "ARRAY"],
                        "quality_score": 7.5,
                        "source": "stackoverflow",
                        "has_images": True,
                        "processing_cost": 0.0  # OCR는 무료
                    }
                }
                
                final_qa_samples.append(qa_sample)
                
                logger.info("✅ 성공!")
                logger.info(f"   • 추출 텍스트 길이: {len(result['extracted_content'])} 문자")
                logger.info(f"   • 단어 수: {len(result['extracted_content'].split())} 개")
                logger.info(f"   • 처리 방법: {result['processing_tier']}")
                
                # OCR 결과 미리보기
                text_preview = result['extracted_content'][:150].replace('\n', ' ')
                logger.info(f"   • 내용 미리보기: '{text_preview}...'")
                
            else:
                logger.error(f"❌ 실패: {result.get('error', 'No content extracted')}")
                
        except Exception as e:
            logger.error(f"❌ 예외 발생: {e}")
        
        logger.info("-" * 50)
    
    # 최종 데이터셋 구성
    final_dataset = {
        "dataset_info": {
            "name": "Excel Q&A Dataset with OCR-Extracted Images",
            "version": "1.0-ocr",
            "description": "Excel Q&A pairs with OCR-extracted image content",
            "total_samples": len(final_qa_samples),
            "processing_pipeline": "Tier 1 (tesseract OCR) only",
            "generated_at": "2025-07-18T07:35:00Z"
        },
        "processing_summary": {
            "images_processed": len(test_images),
            "successful_extractions": len(final_qa_samples),
            "success_rate": (len(final_qa_samples) / len(test_images) * 100) if test_images else 0,
            "average_text_length": sum(len(sample["image_contexts"][0]["extracted_content"]) for sample in final_qa_samples) / len(final_qa_samples) if final_qa_samples else 0,
            "total_processing_cost": 0.0
        },
        "samples": final_qa_samples
    }
    
    # 결과 출력
    logger.info("=" * 70)
    logger.info("📋 최종 OCR 기반 Q&A 데이터셋:")
    logger.info("=" * 70)
    print(json.dumps(final_dataset, indent=2, ensure_ascii=False))
    
    # 파일 저장
    output_path = "/Users/kevin/bigdata/data/output/ocr_based_qa_dataset.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_dataset, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\n💾 데이터셋 저장: {output_path}")
    
    # 통계 요약
    summary = final_dataset["processing_summary"]
    logger.info(f"\n📊 OCR 추출 통계:")
    logger.info(f"   • 성공률: {summary['success_rate']:.1f}%")
    logger.info(f"   • 평균 텍스트 길이: {summary['average_text_length']:.0f} 문자")
    logger.info(f"   • 총 Q&A 샘플: {final_dataset['dataset_info']['total_samples']}개")
    logger.info(f"   • 처리 비용: ${summary['total_processing_cost']}")
    
    return final_dataset

if __name__ == "__main__":
    asyncio.run(test_ocr_only_processing())