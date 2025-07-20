#!/usr/bin/env python3
"""
기존 JSON 데이터셋의 형식 문제 해결
- HTML 태그 제거
- Excel 공식 정확한 추출
- 일관된 텍스트 포맷 적용
"""
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from processors.text_cleaner import TextCleaner
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_dataset_format(input_path: str, output_path: str) -> None:
    """데이터셋 형식 문제 수정"""
    
    cleaner = TextCleaner()
    
    logger.info(f"📄 데이터셋 로드: {input_path}")
    
    # JSON 파일이 JSONL 형식인지 확인
    is_jsonl = input_path.endswith('.jsonl')
    
    if is_jsonl:
        # JSONL 파일 처리
        samples = []
        with open(input_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    samples.append(json.loads(line))
    else:
        # 일반 JSON 파일 처리
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            samples = data.get('samples', [data])  # 단일 객체인 경우도 처리
    
    logger.info(f"🔧 {len(samples)}개 샘플 처리 시작")
    
    fixed_samples = []
    total_formulas = 0
    
    for i, sample in enumerate(samples, 1):
        if 'assistant_response' in sample:
            # 원본 응답 정리
            original_response = sample['assistant_response']
            cleaned = cleaner.clean_qa_response(original_response)
            
            # 업데이트된 샘플 생성
            fixed_sample = sample.copy()
            fixed_sample['assistant_response'] = cleaned['clean_text']
            fixed_sample['code_blocks'] = cleaned['extracted_formulas']
            
            # 메타데이터 업데이트
            if 'metadata' not in fixed_sample:
                fixed_sample['metadata'] = {}
            
            fixed_sample['metadata']['has_code'] = cleaned['has_code']
            fixed_sample['metadata']['formula_count'] = len(cleaned['extracted_formulas'])
            fixed_sample['metadata']['text_cleaned'] = True
            
            fixed_samples.append(fixed_sample)
            total_formulas += len(cleaned['extracted_formulas'])
            
            # 진행상황 출력
            if i % 1 == 0:  # 모든 샘플에 대해 출력 (작은 데이터셋이므로)
                logger.info(f"  [{i}/{len(samples)}] 처리 완료 - 공식 {len(cleaned['extracted_formulas'])}개 추출")
        else:
            # assistant_response가 없는 경우 그대로 유지
            fixed_samples.append(sample)
    
    # 수정된 데이터셋 구성
    if is_jsonl:
        # JSONL 형식으로 저장
        with open(output_path, 'w', encoding='utf-8') as f:
            for sample in fixed_samples:
                f.write(json.dumps(sample, ensure_ascii=False) + '\n')
    else:
        # JSON 형식으로 저장
        fixed_dataset = {
            "dataset_info": {
                "name": "Fixed Excel Q&A Dataset",
                "version": "2.0-cleaned",
                "description": "Excel Q&A dataset with cleaned HTML tags and extracted formulas",
                "total_samples": len(fixed_samples),
                "total_formulas_extracted": total_formulas,
                "processing_notes": [
                    "HTML tags removed from assistant responses",
                    "Excel formulas accurately extracted and normalized",
                    "Text formatting standardized"
                ],
                "generated_at": "2025-07-18T07:50:00Z"
            },
            "processing_summary": {
                "samples_processed": len(fixed_samples),
                "formulas_extracted": total_formulas,
                "average_formulas_per_sample": total_formulas / len(fixed_samples) if fixed_samples else 0
            },
            "samples": fixed_samples
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(fixed_dataset, f, indent=2, ensure_ascii=False)
    
    logger.info(f"✅ 수정 완료!")
    logger.info(f"   • 처리된 샘플: {len(fixed_samples)}개")
    logger.info(f"   • 추출된 공식: {total_formulas}개")
    logger.info(f"   • 저장 경로: {output_path}")

def demonstrate_fixes():
    """수정 전후 비교 예시"""
    cleaner = TextCleaner()
    
    # 실제 문제가 있던 응답들
    problematic_responses = [
        "<p>Use MAXIFS()</p>\n<pre><code>=MAXIFS(B:B,A:A,&quot;Apples&quot;)\n</code></pre>\n<p>Make sure to format the output as a date.</p>",
        
        "<p>&quot;I was wondering if there is a way in Excel to have an array function that, for some cells in the array, references the values in other cells within the array&quot; - can be only possible with the REDUCE function in combination with the VSTACK or HSTACK function.</p>\n<p>The formula for Pascal's triangle is:</p>\n<pre><code>=LET(N,5,REDUCE(SEQUENCE(,N,1,0),SEQUENCE(N-1),\n    LAMBDA(y,z,VSTACK(y,SCAN(0,TAKE(y,-1),LAMBDA(a,x,a+x))))))\n</code></pre>",
        
        "<p><a href=\"https://i.sstatic.net/AJr6APk8.png\" rel=\"noreferrer\"><img src=\"https://i.sstatic.net/AJr6APk8.png\" alt=\"enter image description here\" /></a></p>\n<p>Formula in <code>E2</code>:</p>\n<p><code>=LET(_data,A2:C7,TEXTSPLIT(TEXTAFTER(UNIQUE(BYROW(DROP(REDUCE(0,SEQUENCE(ROWS(_data)),LAMBDA(_main,_iter,LET(_index,INDEX(_data,_iter,),_4th,BYROW(FILTER(_data,BYROW(_data,LAMBDA(_row,SUM(COUNTIF(_index,_row))=2))),LAMBDA(_row,FILTER(_row,COUNTIF(_index,_row)=0))),IF(@ISERR(_4th),_main,VSTACK(_main,IF({1,0,0,0},_4th,HSTACK(0,_index))))))),1),LAMBDA(_row,TEXTJOIN(0,0,,SORT(_row,,,1))))),0,{1,2,3,4}),0))</code></p>"
    ]
    
    print("🔍 수정 전후 비교:")
    print("=" * 80)
    
    for i, response in enumerate(problematic_responses, 1):
        print(f"\n[예시 {i}]")
        print("🔴 수정 전:")
        print(f"  길이: {len(response)} 문자")
        print(f"  내용: {repr(response[:100])}...")
        
        cleaned = cleaner.clean_qa_response(response)
        
        print("🟢 수정 후:")
        print(f"  길이: {len(cleaned['clean_text'])} 문자")
        print(f"  내용: {repr(cleaned['clean_text'][:100])}...")
        print(f"  추출된 공식: {len(cleaned['extracted_formulas'])}개")
        for j, formula in enumerate(cleaned['extracted_formulas'][:3], 1):  # 처음 3개만 표시
            print(f"    {j}. {formula}")
        if len(cleaned['extracted_formulas']) > 3:
            print(f"    ... (+{len(cleaned['extracted_formulas'])-3}개 더)")
        print("-" * 60)

if __name__ == "__main__":
    # 수정 예시 시연
    demonstrate_fixes()
    
    print("\n" + "=" * 80)
    
    # 실제 데이터셋 파일들 수정
    datasets_to_fix = [
        ("/Users/kevin/bigdata/data/output/year=2025/month=07/day=18/combined_20250718.jsonl", 
         "/Users/kevin/bigdata/data/output/cleaned_combined_20250718.jsonl"),
        
        ("/Users/kevin/bigdata/data/output/ai_enhanced_qa_dataset.json", 
         "/Users/kevin/bigdata/data/output/cleaned_ai_enhanced_qa_dataset.json"),
        
        ("/Users/kevin/bigdata/data/output/ocr_based_qa_dataset.json", 
         "/Users/kevin/bigdata/data/output/cleaned_ocr_based_qa_dataset.json")
    ]
    
    for input_path, output_path in datasets_to_fix:
        if Path(input_path).exists():
            try:
                fix_dataset_format(input_path, output_path)
            except Exception as e:
                logger.error(f"❌ {input_path} 처리 실패: {e}")
        else:
            logger.warning(f"⚠️  파일 없음: {input_path}")
    
    logger.info("\n🎉 모든 데이터셋 형식 수정 완료!")