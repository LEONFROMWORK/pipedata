#!/usr/bin/env python3
"""
수정된 Stack Overflow 답변 추출 테스트
"""
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from output.dataset_generator import JSONLDatasetGenerator

def test_fixed_stackoverflow_extraction():
    """수정된 Stack Overflow 답변 추출 테스트"""
    
    print("🔧 수정된 Stack Overflow 답변 추출 테스트")
    print("=" * 60)
    
    # 실제 bypass_test_results.json에서 가져온 데이터 구조
    real_qa_pair = {
        'question': {
            "tags": ["excel", "excel-formula"],
            "title": "excel - Find all rows that have 2 out of 3 columns in common, combine with the uncommon column of each to make 4 column rows",
            "body_markdown": "I am new to Excel. I have a spreadsheet containing 140 rows of 3 columns each, and trying to find the rows which share 2 of its columns with another row, and add the unique columns from each row to them. I can&#39;t seem to wrap my head around how to go about this.",
            "question_id": 79703263,
            "score": 4
        },
        'answer': {
            # 비어있는 상태 (문제가 있던 상황)
        },
        'accepted_answer': {
            "answer_id": 79703559,
            "score": 5,
            "is_accepted": True,
            "body": "<p><a href=\"https://i.sstatic.net/AJr6APk8.png\" rel=\"noreferrer\"><img src=\"https://i.sstatic.net/AJr6APk8.png\" alt=\"enter image description here\" /></a></p>\n<p>Formula in <code>E2</code>:</p>\n<p><code>=LET(_data,A2:C7,TEXTSPLIT(TEXTAFTER(UNIQUE(BYROW(DROP(REDUCE(0,SEQUENCE(ROWS(_data)),LAMBDA(_main,_iter,LET(_index,INDEX(_data,_iter,),_4th,BYROW(FILTER(_data,BYROW(_data,LAMBDA(_row,SUM(COUNTIF(_index,_row))=2))),LAMBDA(_row,FILTER(_row,COUNTIF(_index,_row)=0))),IF(@ISERR(_4th),_main,VSTACK(_main,IF({1,0,0,0},_4th,HSTACK(0,_index))))))),1),LAMBDA(_row,TEXTJOIN(0,0,,SORT(_row,,,1))))),0,{1,2,3,4}),0))</code></p>\n<p>If need be I can add some explanation.</p>",
            "body_markdown": "Formula in `E2`:\n\n`=LET(_data,A2:C7,TEXTSPLIT(TEXTAFTER(UNIQUE(BYROW(DROP(REDUCE(0,SEQUENCE(ROWS(_data)),LAMBDA(_main,_iter,LET(_index,INDEX(_data,_iter,),_4th,BYROW(FILTER(_data,BYROW(_data,LAMBDA(_row,SUM(COUNTIF(_index,_row))=2))),LAMBDA(_row,FILTER(_row,COUNTIF(_index,_row)=0))),IF(@ISERR(_4th),_main,VSTACK(_main,IF({1,0,0,0},_4th,HSTACK(0,_index))))))),1),LAMBDA(_row,TEXTJOIN(0,0,,SORT(_row,,,1))))),0,{1,2,3,4}),0))`\n\nIf need be I can add some explanation."
        },
        'source': 'stackoverflow',
        'quality_metrics': {
            'overall_score': 8.5,
            'raw_question_score': 4.0,
            'raw_answer_score': 5.0
        },
        'has_accepted_answer': True
    }
    
    print("📊 테스트 데이터 구조:")
    print(f"   질문 ID: {real_qa_pair['question']['question_id']}")
    print(f"   답변 ID: {real_qa_pair['accepted_answer']['answer_id']}")
    print(f"   답변 본문 길이: {len(real_qa_pair['accepted_answer']['body'])} 문자")
    
    # 변환 테스트
    generator = JSONLDatasetGenerator()
    
    try:
        print(f"\n🔄 변환 테스트 (수정된 로직):")
        result = generator._convert_to_new_format(real_qa_pair)
        
        print(f"   ✅ 변환 성공!")
        print(f"   ID: {result.id}")
        print(f"   사용자 질문: {result.user_question[:80]}...")
        print(f"   사용자 컨텍스트: {len(result.user_context)} 문자")
        print(f"   어시스턴트 답변: {len(result.assistant_response)} 문자")
        print(f"   코드 블록: {len(result.code_blocks)}개")
        
        if result.assistant_response:
            print(f"   📝 답변 내용: {result.assistant_response[:100]}...")
        else:
            print(f"   ❌ 답변이 여전히 비어있음!")
            
        if result.code_blocks:
            print(f"   💻 첫 번째 코드: {result.code_blocks[0][:60]}...")
        
        print(f"   📈 품질 점수: {result.metadata['quality_score']}")
        print(f"   🎯 난이도: {result.metadata['difficulty']}")
        print(f"   🔧 함수들: {result.metadata['functions'][:5]}")  # 처음 5개만
        
        return result
        
    except Exception as e:
        print(f"   ❌ 변환 실패: {e}")
        import traceback
        print(f"   스택 트레이스: {traceback.format_exc()}")
        return None

def test_full_dataset_generation():
    """전체 데이터셋 생성 테스트"""
    
    print(f"\n📦 전체 데이터셋 생성 테스트")
    print("=" * 40)
    
    # 여러 개의 테스트 데이터
    test_qa_pairs = []
    
    # 1. Stack Overflow (accepted_answer 구조)
    so_data = {
        'question': {
            "tags": ["excel", "excel-formula"],
            "title": "How to use VLOOKUP with multiple criteria",
            "body_markdown": "I need to lookup values with multiple criteria in Excel.",
            "question_id": 12345,
            "score": 3
        },
        'accepted_answer': {
            "answer_id": 54321,
            "score": 8,
            "body": "<p>Use <code>=VLOOKUP(criteria, range, column, FALSE)</code> for exact match.</p>",
            "body_markdown": "Use `=VLOOKUP(criteria, range, column, FALSE)` for exact match."
        },
        'source': 'stackoverflow',
        'quality_metrics': {'overall_score': 7.5}
    }
    test_qa_pairs.append(so_data)
    
    # 2. Reddit 데이터
    reddit_data = {
        'question': {
            'title': 'Help with INDEX MATCH',
            'text': 'How do I use INDEX MATCH instead of VLOOKUP?',
            'reddit_id': 'abc123'
        },
        'answer': {
            'text': 'Try =INDEX(return_range, MATCH(lookup_value, lookup_range, 0))',
            'reddit_id': 'def456'
        },
        'source': 'reddit',
        'quality_metrics': {'overall_score': 6.8}
    }
    test_qa_pairs.append(reddit_data)
    
    try:
        generator = JSONLDatasetGenerator()
        output_path = generator.generate_dataset(
            test_qa_pairs,
            data_sources=['stackoverflow', 'reddit']
        )
        
        print(f"   ✅ 데이터셋 생성 성공: {output_path}")
        
        # 결과 확인
        with open(output_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        print(f"   📊 생성된 항목: {len(lines)}개")
        
        for i, line in enumerate(lines, 1):
            data = json.loads(line.strip())
            response_length = len(data.get('assistant_response', ''))
            print(f"      [{i}] {data['metadata']['source']}: {response_length} 문자 답변")
            
            if response_length == 0:
                print(f"          ❌ 답변 누락!")
            else:
                print(f"          ✅ 답변 정상")
        
        return output_path
        
    except Exception as e:
        print(f"   ❌ 데이터셋 생성 실패: {e}")
        return None

if __name__ == "__main__":
    # 1. 개별 변환 테스트
    result = test_fixed_stackoverflow_extraction()
    
    # 2. 전체 데이터셋 생성 테스트
    if result:
        test_full_dataset_generation()
    
    print(f"\n🎉 테스트 완료!")
    print("실제 수집 파이프라인에서도 답변이 정상 추출될 것입니다.")