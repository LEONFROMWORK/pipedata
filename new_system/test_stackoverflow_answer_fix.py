#!/usr/bin/env python3
"""
Stack Overflow 답변 추출 수정 테스트
"""
import sys
from pathlib import Path
import json

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from output.dataset_generator import JSONLDatasetGenerator
from core.cache import APICache, LocalCache

def test_stackoverflow_answer_extraction():
    """Stack Overflow 답변 추출 수정 테스트"""
    
    print("🔧 Stack Overflow 답변 추출 수정 테스트")
    print("=" * 70)
    
    # 테스트용 Stack Overflow 데이터 (원시 데이터에서 가져온 것)
    test_so_data = {
        "tags": ["excel", "excel-formula"],
        "title": "excel - Find all rows that have 2 out of 3 columns in common",
        "body_markdown": "I am new to Excel. I have a spreadsheet containing 140 rows...",
        "accepted_answer": {
            "body": "<p><a href=\"https://i.sstatic.net/AJr6APk8.png\" rel=\"noreferrer\"><img src=\"https://i.sstatic.net/AJr6APk8.png\" alt=\"enter image description here\" /></a></p>\n<p>Formula in <code>E2</code>:</p>\n<p><code>=LET(_data,A2:C7,TEXTSPLIT(TEXTAFTER(UNIQUE(BYROW(DROP(REDUCE(0,SEQUENCE(ROWS(_data)),LAMBDA(_main,_iter,LET(_index,INDEX(_data,_iter,),_4th,BYROW(FILTER(_data,BYROW(_data,LAMBDA(_row,SUM(COUNTIF(_index,_row))=2))),LAMBDA(_row,FILTER(_row,COUNTIF(_index,_row)=0))),IF(@ISERR(_4th),_main,VSTACK(_main,IF({1,0,0,0},_4th,HSTACK(0,_index))))))),1),LAMBDA(_row,TEXTJOIN(0,0,,SORT(_row,,,1))))),0,{1,2,3,4}),0))</code></p>\n<p>If need be I can add some explanation.</p>"
        },
        "question_id": 79703263,
        "is_answered": True,
        "score": 4
    }
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/test_answer_fix.db"))
    cache = APICache(local_cache)
    
    # JSONLDatasetGenerator 초기화
    generator = JSONLDatasetGenerator()
    
    print("📝 원시 HTML 답변:")
    html_answer = test_so_data["accepted_answer"]["body"]
    print(f"   길이: {len(html_answer)} 문자")
    print(f"   내용: {html_answer[:100]}...")
    
    print("\n🧹 HTML 정리 후 답변:")
    try:
        clean_answer = generator._clean_html_content(html_answer)
        print(f"   길이: {len(clean_answer)} 문자")
        print(f"   내용: {clean_answer}")
        
        if clean_answer:
            print("   ✅ 답변 추출 성공!")
        else:
            print("   ❌ 답변이 여전히 비어있음")
            
    except Exception as e:
        print(f"   ❌ HTML 정리 실패: {e}")
        # BeautifulSoup이 없는 경우 간단한 대안
        import re
        import html
        
        print("\n🔧 간단한 HTML 태그 제거로 대안 처리:")
        # 간단한 HTML 태그 제거
        clean_answer = re.sub(r'<[^>]+>', '', html_answer)
        clean_answer = html.unescape(clean_answer)
        clean_answer = re.sub(r'\s+', ' ', clean_answer).strip()
        
        print(f"   길이: {len(clean_answer)} 문자")
        print(f"   내용: {clean_answer}")
    
    print("\n📋 Excel 수식 추출 테스트:")
    try:
        formulas = generator._extract_excel_formulas(html_answer + " " + clean_answer)
        print(f"   추출된 수식: {len(formulas)}개")
        for i, formula in enumerate(formulas, 1):
            print(f"      [{i}] {formula[:60]}...")
            
        # 이미지 URL이 포함되었는지 확인
        has_image_url = any('sstatic.net' in formula or 'redd.it' in formula 
                           for formula in formulas)
        if has_image_url:
            print("   ❌ 이미지 URL이 수식으로 잘못 분류됨")
        else:
            print("   ✅ 이미지 URL이 수식에서 제외됨")
            
    except Exception as e:
        print(f"   ❌ 수식 추출 실패: {e}")
    
    print("\n" + "🏁" * 30)
    print("Stack Overflow 답변 추출 수정 테스트 완료")

if __name__ == "__main__":
    test_stackoverflow_answer_extraction()