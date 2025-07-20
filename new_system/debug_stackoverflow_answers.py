#!/usr/bin/env python3
"""
Stack Overflow 답변 추출 디버깅
"""
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

def debug_stackoverflow_data():
    """실제 Stack Overflow 데이터 구조 분석"""
    
    print("🔍 Stack Overflow 데이터 구조 디버깅")
    print("=" * 60)
    
    # 테스트용 Stack Overflow 원시 데이터 (실제 API 응답 형태)
    test_data = {
        "tags": ["excel", "excel-formula"],
        "title": "excel - Find all rows that have 2 out of 3 columns in common",
        "body_markdown": "I am new to Excel. I have a spreadsheet containing 140 rows...",
        "question_id": 79703263,
        "is_answered": True,
        "score": 4,
        "accepted_answer": {
            "body": "<p><a href=\"https://i.sstatic.net/AJr6APk8.png\" rel=\"noreferrer\"><img src=\"https://i.sstatic.net/AJr6APk8.png\" alt=\"enter image description here\" /></a></p>\n<p>Formula in <code>E2</code>:</p>\n<p><code>=LET(_data,A2:C7,TEXTSPLIT(TEXTAFTER(UNIQUE(BYROW(DROP(REDUCE(0,SEQUENCE(ROWS(_data)),LAMBDA(_main,_iter,LET(_index,INDEX(_data,_iter,),_4th,BYROW(FILTER(_data,BYROW(_data,LAMBDA(_row,SUM(COUNTIF(_index,_row))=2))),LAMBDA(_row,FILTER(_row,COUNTIF(_index,_row)=0))),IF(@ISERR(_4th),_main,VSTACK(_main,IF({1,0,0,0},_4th,HSTACK(0,_index))))))),1),LAMBDA(_row,TEXTJOIN(0,0,,SORT(_row,,,1))))),0,{1,2,3,4}),0))</code></p>\n<p>If need be I can add some explanation.</p>",
            "body_markdown": "Formula in `E2`:\n\n`=LET(_data,A2:C7,TEXTSPLIT(TEXTAFTER(UNIQUE(BYROW(DROP(REDUCE(0,SEQUENCE(ROWS(_data)),LAMBDA(_main,_iter,LET(_index,INDEX(_data,_iter,),_4th,BYROW(FILTER(_data,BYROW(_data,LAMBDA(_row,SUM(COUNTIF(_index,_row))=2))),LAMBDA(_row,FILTER(_row,COUNTIF(_index,_row)=0))),IF(@ISERR(_4th),_main,VSTACK(_main,IF({1,0,0,0},_4th,HSTACK(0,_index))))))),1),LAMBDA(_row,TEXTJOIN(0,0,,SORT(_row,,,1))))),0,{1,2,3,4}),0))`\n\nIf need be I can add some explanation.",
            "answer_id": 79703559,
            "score": 5,
            "is_accepted": True
        }
    }
    
    print("📊 원시 데이터 구조:")
    print(f"   질문 제목: {test_data['title'][:50]}...")
    print(f"   답변 ID: {test_data['accepted_answer']['answer_id']}")
    print(f"   답변 점수: {test_data['accepted_answer']['score']}")
    
    print(f"\n🔍 답변 필드 분석:")
    answer = test_data['accepted_answer']
    print(f"   'body' 길이: {len(answer.get('body', ''))} 문자")
    print(f"   'body_markdown' 길이: {len(answer.get('body_markdown', ''))} 문자")
    
    print(f"\n📝 'body' 내용 (HTML):")
    body_html = answer.get('body', '')
    print(f"   {body_html[:100]}...")
    
    print(f"\n📝 'body_markdown' 내용:")
    body_md = answer.get('body_markdown', '')
    print(f"   {body_md[:100]}...")
    
    # HTML 정리 테스트
    print(f"\n🧹 HTML 정리 테스트:")
    from output.dataset_generator import JSONLDatasetGenerator
    
    generator = JSONLDatasetGenerator()
    
    # HTML 정리
    cleaned_html = generator._clean_html_content(body_html)
    print(f"   정리된 HTML: {cleaned_html[:100]}...")
    print(f"   정리된 길이: {len(cleaned_html)} 문자")
    
    # 새로운 형식으로 변환 테스트
    print(f"\n🔄 변환 테스트:")
    
    # 컬렉터에서 오는 형태로 시뮬레이션
    qa_pair = {
        'question': {
            'title': test_data['title'],
            'body_markdown': test_data['body_markdown'],
            'question_id': test_data['question_id'],
            'tags': test_data['tags'],
            'score': test_data['score']
        },
        'answer': test_data['accepted_answer'],  # 답변을 직접 넣음
        'source': 'stackoverflow',
        'quality_metrics': {
            'overall_score': 8.5,
            'raw_question_score': 4.0,
            'raw_answer_score': 5.0
        },
        'has_accepted_answer': True
    }
    
    try:
        result = generator._convert_to_new_format(qa_pair)
        print(f"   ✅ 변환 성공!")
        print(f"   사용자 질문: {result.user_question[:50]}...")
        print(f"   어시스턴트 답변: {result.assistant_response[:100]}...")
        print(f"   코드 블록: {len(result.code_blocks)}개")
        
        if result.code_blocks:
            print(f"   첫 번째 코드: {result.code_blocks[0][:50]}...")
        
    except Exception as e:
        print(f"   ❌ 변환 실패: {e}")
        import traceback
        print(f"   스택 트레이스: {traceback.format_exc()}")

if __name__ == "__main__":
    debug_stackoverflow_data()