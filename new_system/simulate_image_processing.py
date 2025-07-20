#!/usr/bin/env python3
"""
이미지 AI 변환 결과 시뮬레이션 - 실제 프로덕션 환경 결과 예시
"""
import json

def simulate_image_processing_results():
    """실제 프로덕션 환경에서 생성될 JSON 형식 시뮬레이션"""
    
    # 실제 이미지 처리 결과 시뮬레이션 (3가지 케이스)
    
    # Case 1: Excel 표/데이터 이미지 (Tier 2 - claude-3.5-sonnet)
    excel_table_qa = {
        "id": "excel_qa_with_table_image",
        "user_question": "How to create a pivot table summary from this data?",
        "user_context": "I have sales data and want to create a summary...",
        "assistant_response": "Here's how to create the pivot table: <img src='https://i.sstatic.net/example1.png'>",
        "code_blocks": ["=SUMIF(A:A,\"Product\",B:B)"],
        "image_contexts": [
            {
                "source_url": "https://i.sstatic.net/example1.png",
                "extracted_content": "| Product | Sales | Region |\n|:-------|:------|:-------|\n| Laptop | $1200 | North |\n| Mouse | $25 | South |\n| Keyboard | $75 | East |\n| Monitor | $300 | West |",
                "content_type": "markdown_table",
                "processing_method": "Tier 2 (claude-3.5-sonnet)",
                "ai_model": "anthropic/claude-3.5-sonnet",
                "tokens_used": 245,
                "confidence": "high"
            }
        ],
        "metadata": {
            "difficulty": "intermediate",
            "functions": ["SUMIF", "PIVOT"],
            "quality_score": 8.5,
            "source": "stackoverflow",
            "has_images": True,
            "image_processing_cost": 0.0024  # USD
        }
    }
    
    # Case 2: Excel 차트/그래프 이미지 (Tier 3 - gpt-4o)
    excel_chart_qa = {
        "id": "excel_qa_with_chart_image", 
        "user_question": "How to create this type of dynamic chart in Excel?",
        "user_context": "I need to recreate this chart with dynamic data ranges...",
        "assistant_response": "This is a combination chart: <img src='https://i.sstatic.net/chart_example.png'>",
        "code_blocks": ["=OFFSET($A$1,0,0,COUNTA($A:$A),1)"],
        "image_contexts": [
            {
                "source_url": "https://i.sstatic.net/chart_example.png",
                "extracted_content": "The image shows a combination chart with:\n\n1. **Chart Type**: Column chart with line overlay\n2. **Primary Axis**: Sales data (columns) showing Q1: $45K, Q2: $62K, Q3: $58K, Q4: $71K\n3. **Secondary Axis**: Growth rate (line) showing 15%, 22%, 18%, 25%\n4. **Excel Features Visible**:\n   - Chart title: 'Quarterly Sales & Growth'\n   - Legend positioning: bottom\n   - Data labels enabled on line series\n   - Custom color scheme: blue columns, red line\n5. **Formula Bar Shows**: =OFFSET($A$1,0,0,COUNTA($A:$A),1)\n\nThis demonstrates dynamic range creation using OFFSET and COUNTA functions for automatically expanding chart data ranges.",
                "content_type": "chart_description",
                "processing_method": "Tier 3 (gpt-4o)",
                "ai_model": "openai/gpt-4o",
                "tokens_used": 387,
                "confidence": "high"
            }
        ],
        "metadata": {
            "difficulty": "advanced",
            "functions": ["OFFSET", "COUNTA", "CHART"],
            "quality_score": 9.2,
            "source": "stackoverflow", 
            "has_images": True,
            "image_processing_cost": 0.0058  # USD
        }
    }
    
    # Case 3: 단순 텍스트 이미지 (Tier 1 - OCR만)
    simple_text_qa = {
        "id": "excel_qa_with_text_image",
        "user_question": "What's wrong with this formula error message?",
        "user_context": "Excel is showing an error and I don't understand...",
        "assistant_response": "The error message shows: <img src='https://i.sstatic.net/error_msg.png'>",
        "code_blocks": ["=VLOOKUP(A1,Sheet2!A:B,2,FALSE)"],
        "image_contexts": [
            {
                "source_url": "https://i.sstatic.net/error_msg.png", 
                "extracted_content": "#N/A Error\n\nThe formula =VLOOKUP(A1,Sheet2!A:B,2,FALSE) returned an error because the lookup value was not found in the first column of the lookup range.\n\nSuggested fixes:\n- Check if the lookup value exists\n- Verify the range reference\n- Use IFERROR to handle missing values",
                "content_type": "enhanced_text",
                "processing_method": "Tier 1 (pytesseract) + AI enhancement",
                "ai_model": "anthropic/claude-3.5-sonnet",
                "tokens_used": 156,
                "confidence": "high"
            }
        ],
        "metadata": {
            "difficulty": "beginner",
            "functions": ["VLOOKUP", "IFERROR"],
            "quality_score": 7.8,
            "source": "reddit",
            "has_images": True,
            "image_processing_cost": 0.0019  # USD
        }
    }
    
    # 전체 데이터셋 메타데이터
    dataset_with_images = {
        "dataset_info": {
            "name": "Excel Q&A Dataset with AI-Processed Images",
            "version": "1.1.0",
            "total_samples": 3,
            "images_processed": 3,
            "processing_pipeline": "3-tier: OCR → Table → AI enhancement",
            "ai_models_used": ["anthropic/claude-3.5-sonnet", "openai/gpt-4o"],
            "total_processing_cost": 0.0101,  # USD
            "generated_at": "2025-07-18T07:30:00Z"
        },
        "quality_metrics": {
            "image_processing_success_rate": 100.0,
            "average_quality_score": 8.5,
            "tier_distribution": {
                "tier_1_ocr": 0,
                "tier_2_table": 1, 
                "tier_3_chart": 1,
                "tier_1_enhanced": 1
            }
        },
        "samples": [excel_table_qa, excel_chart_qa, simple_text_qa]
    }
    
    return dataset_with_images

if __name__ == "__main__":
    # 시뮬레이션 실행
    result = simulate_image_processing_results()
    
    print("🎯 실제 프로덕션 환경에서 생성될 JSON 형식:")
    print("=" * 70)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    # 파일로 저장
    output_path = "/Users/kevin/bigdata/data/output/simulated_image_processing_results.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 시뮬레이션 결과 저장: {output_path}")
    
    # 요약 정보
    print(f"\n📊 처리 결과 요약:")
    print(f"   • 총 샘플: {result['dataset_info']['total_samples']}개")
    print(f"   • 이미지 처리 성공률: {result['quality_metrics']['image_processing_success_rate']}%")
    print(f"   • 평균 품질 점수: {result['quality_metrics']['average_quality_score']}")
    print(f"   • 총 처리 비용: ${result['dataset_info']['total_processing_cost']}")
    print(f"   • 사용된 AI 모델: {', '.join(result['dataset_info']['ai_models_used'])}")