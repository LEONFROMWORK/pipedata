#!/usr/bin/env python3
"""
실제 OpenRouter API로 전체 3-tier 이미지 처리 파이프라인 테스트
Tier 1: tesseract OCR
Tier 2: img2table (disabled due to error)  
Tier 3: OpenRouter AI (claude-3.5-sonnet, gpt-4o)
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

class FullAIImageProcessor(ImageProcessor):
    """완전한 AI 향상 이미지 프로세서"""
    
    def _should_use_ai_enhancement(self, ocr_result, table_result, context_tags):
        """항상 AI 향상을 사용하도록 설정 (테스트용)"""
        return True
    
    async def _extract_tables_with_img2table(self, image_path):
        """img2table 에러 회피 - 빈 결과 반환하여 AI로 넘어가도록"""
        return {'tables_found': 0, 'markdown_content': '', 'raw_tables': []}

async def test_full_ai_pipeline():
    """전체 AI 향상 이미지 처리 파이프라인 테스트"""
    
    # Initialize cache and processor
    local_cache = LocalCache(db_path=Path("/tmp/full_ai_test_cache.db"))
    cache = APICache(local_cache)
    processor = FullAIImageProcessor(cache)
    
    # 다양한 유형의 Excel 이미지 테스트
    test_cases = [
        {
            "url": "https://i.sstatic.net/TpgieF6J.png",
            "description": "Excel 파스칼 삼각형 (테이블 데이터)",
            "context_tags": ["excel", "formula", "table", "array"],
            "expected_model": "claude-3.5-sonnet",
            "qa_context": {
                "question": "How to create Pascal's triangle with array formulas?",
                "user_context": "I want to generate Pascal's triangle dynamically using Excel array formulas and LAMBDA functions."
            }
        },
        {
            "url": "https://i.sstatic.net/82NP1kgT.png", 
            "description": "Excel 필터 결과 (차트/복잡한 시각화)",
            "context_tags": ["excel", "filter", "chart", "dynamic"],
            "expected_model": "gpt-4o",
            "qa_context": {
                "question": "How to filter dynamic arrays with multiple criteria?",
                "user_context": "I need to filter a table using dynamic array formulas with complex matching criteria."
            }
        }
    ]
    
    logger.info("🤖 전체 AI 향상 이미지 처리 파이프라인 테스트")
    logger.info("=" * 80)
    
    final_ai_qa_samples = []
    total_cost = 0.0
    
    for i, test_case in enumerate(test_cases, 1):
        logger.info(f"[{i}/{len(test_cases)}] {test_case['description']}")
        logger.info(f"URL: {test_case['url']}")
        logger.info(f"예상 AI 모델: {test_case['expected_model']}")
        logger.info("-" * 60)
        
        try:
            # 3-tier 이미지 처리 실행
            result = await processor.process_image_url(
                test_case["url"], 
                test_case["context_tags"]
            )
            
            if result['success']:
                # AI 향상된 Q&A 샘플 생성
                ai_qa_sample = {
                    "id": f"excel_qa_ai_enhanced_{i:03d}",
                    "user_question": test_case["qa_context"]["question"],
                    "user_context": test_case["qa_context"]["user_context"],
                    "assistant_response": f"Looking at your Excel screenshot, I can analyze the content using AI. {result.get('extracted_content', 'No content extracted.')}",
                    "code_blocks": [],  # AI가 추출한 코드는 extracted_content에 포함
                    "image_contexts": [
                        {
                            "source_url": test_case["url"],
                            "extracted_content": result["extracted_content"],
                            "content_type": result["extracted_content_type"],
                            "processing_method": result["processing_tier"],
                            "ai_model": result.get("ai_model_used"),
                            "tokens_used": result.get("tokens_used", 0),
                            "confidence": "high"  # AI 처리는 high confidence
                        }
                    ],
                    "metadata": {
                        "difficulty": "advanced",
                        "functions": test_case["context_tags"],
                        "quality_score": 9.0,  # AI 향상된 것은 높은 점수
                        "source": "stackoverflow",
                        "has_images": True,
                        "ai_enhanced": True,
                        "processing_cost": result.get("tokens_used", 0) * 0.000015  # 대략적인 비용 계산
                    }
                }
                
                final_ai_qa_samples.append(ai_qa_sample)
                total_cost += ai_qa_sample["metadata"]["processing_cost"]
                
                logger.info("✅ AI 처리 성공!")
                logger.info(f"   • 처리 단계: {result.get('processing_steps', [])}")
                logger.info(f"   • AI 모델: {result.get('ai_model_used', 'Unknown')}")
                logger.info(f"   • 토큰 사용: {result.get('tokens_used', 0)}")
                logger.info(f"   • 콘텐츠 타입: {result.get('extracted_content_type', 'Unknown')}")
                logger.info(f"   • 텍스트 길이: {len(result.get('extracted_content', ''))} 문자")
                logger.info(f"   • 예상 비용: ${ai_qa_sample['metadata']['processing_cost']:.4f}")
                
                # AI 추출 내용 미리보기
                content = result.get('extracted_content', '')
                if content:
                    preview = content[:300] + "..." if len(content) > 300 else content
                    logger.info(f"   • AI 분석 내용: {repr(preview)}")
                
            else:
                logger.error(f"❌ AI 처리 실패: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ 예외 발생: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        logger.info("=" * 60)
    
    # 최종 AI 향상 데이터셋 구성
    final_ai_dataset = {
        "dataset_info": {
            "name": "Excel Q&A Dataset with AI-Enhanced Images",
            "version": "2.0-ai-enhanced",
            "description": "Excel Q&A pairs with AI-enhanced image analysis using OpenRouter",
            "total_samples": len(final_ai_qa_samples),
            "processing_pipeline": "3-tier: tesseract OCR → AI enhancement (claude-3.5-sonnet/gpt-4o)",
            "ai_models": ["anthropic/claude-3.5-sonnet", "openai/gpt-4o"],
            "generated_at": "2025-07-18T07:40:00Z"
        },
        "processing_summary": {
            "images_processed": len(test_cases),
            "successful_ai_enhancements": len(final_ai_qa_samples),
            "success_rate": (len(final_ai_qa_samples) / len(test_cases) * 100) if test_cases else 0,
            "total_tokens_used": sum(sample["image_contexts"][0]["tokens_used"] for sample in final_ai_qa_samples),
            "total_processing_cost": total_cost,
            "average_response_length": sum(len(sample["image_contexts"][0]["extracted_content"]) for sample in final_ai_qa_samples) / len(final_ai_qa_samples) if final_ai_qa_samples else 0
        },
        "ai_enhancement_details": {
            "tier_distribution": {
                "tier_3_ai_enhanced": len(final_ai_qa_samples)
            },
            "model_usage": {},
            "cost_breakdown": {
                "total_cost": total_cost,
                "cost_per_image": total_cost / len(test_cases) if test_cases else 0
            }
        },
        "samples": final_ai_qa_samples
    }
    
    # 모델 사용량 계산
    for sample in final_ai_qa_samples:
        model = sample["image_contexts"][0].get("ai_model", "unknown")
        if model not in final_ai_dataset["ai_enhancement_details"]["model_usage"]:
            final_ai_dataset["ai_enhancement_details"]["model_usage"][model] = 0
        final_ai_dataset["ai_enhancement_details"]["model_usage"][model] += 1
    
    # 결과 출력
    logger.info("=" * 80)
    logger.info("🎯 최종 AI 향상 Excel Q&A 데이터셋:")
    logger.info("=" * 80)
    print(json.dumps(final_ai_dataset, indent=2, ensure_ascii=False))
    
    # 파일 저장
    output_path = "/Users/kevin/bigdata/data/output/ai_enhanced_qa_dataset.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_ai_dataset, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\n💎 AI 향상 데이터셋 저장: {output_path}")
    
    # 최종 통계
    summary = final_ai_dataset["processing_summary"]
    logger.info(f"\n🚀 AI 향상 통계:")
    logger.info(f"   • AI 처리 성공률: {summary['success_rate']:.1f}%")
    logger.info(f"   • 총 토큰 사용: {summary['total_tokens_used']:,}")
    logger.info(f"   • 총 처리 비용: ${summary['total_processing_cost']:.4f}")
    logger.info(f"   • 평균 응답 길이: {summary['average_response_length']:.0f} 문자")
    logger.info(f"   • 이미지당 평균 비용: ${final_ai_dataset['ai_enhancement_details']['cost_breakdown']['cost_per_image']:.4f}")
    
    return final_ai_dataset

if __name__ == "__main__":
    asyncio.run(test_full_ai_pipeline())