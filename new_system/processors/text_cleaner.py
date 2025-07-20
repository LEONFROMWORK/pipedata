"""
Text cleaning and formatting utilities for Excel Q&A dataset
HTML 태그 제거, 코드 블록 정리, Excel 공식 추출
"""
import re
import html
from typing import List, Dict, Any
import logging

logger = logging.getLogger('pipeline.text_cleaner')

class TextCleaner:
    """텍스트 정리 및 Excel 공식 추출을 위한 유틸리티 클래스"""
    
    def __init__(self):
        # Excel 함수 패턴 (확장된 리스트)
        self.excel_functions = {
            'core': ['SUM', 'COUNT', 'AVERAGE', 'MAX', 'MIN', 'IF', 'AND', 'OR', 'NOT'],
            'lookup': ['VLOOKUP', 'HLOOKUP', 'XLOOKUP', 'INDEX', 'MATCH', 'LOOKUP'],
            'array': ['FILTER', 'SORT', 'UNIQUE', 'SEQUENCE', 'RANDARRAY', 'TRANSPOSE'],
            'lambda': ['LAMBDA', 'LET', 'REDUCE', 'SCAN', 'MAP', 'BYROW', 'BYCOL'],
            'text': ['CONCATENATE', 'TEXTJOIN', 'TEXTSPLIT', 'LEFT', 'RIGHT', 'MID'],
            'date': ['TODAY', 'NOW', 'DATE', 'YEAR', 'MONTH', 'DAY', 'WEEKDAY'],
            'math': ['ROUND', 'CEILING', 'FLOOR', 'ABS', 'POWER', 'SQRT', 'LOG'],
            'advanced': ['SUMIF', 'SUMIFS', 'COUNTIF', 'COUNTIFS', 'MAXIFS', 'MINIFS',
                        'IFERROR', 'ISNA', 'ISBLANK', 'CHOOSE', 'OFFSET', 'INDIRECT']
        }
        
        # 모든 Excel 함수를 하나의 리스트로 통합
        self.all_excel_functions = []
        for category in self.excel_functions.values():
            self.all_excel_functions.extend(category)
    
    def remove_html_tags(self, text: str) -> str:
        """HTML 태그를 제거하고 깔끔한 텍스트로 변환"""
        if not text:
            return ""
        
        # HTML 엔티티 디코딩
        text = html.unescape(text)
        
        # <pre><code> 블록 내용 임시 보존
        code_blocks = []
        def preserve_code_block(match):
            code_blocks.append(match.group(1))
            return f"__CODE_BLOCK_{len(code_blocks)-1}__"
        
        text = re.sub(r'<pre><code>(.*?)</code></pre>', preserve_code_block, text, flags=re.DOTALL)
        text = re.sub(r'<code>(.*?)</code>', preserve_code_block, text, flags=re.DOTALL)
        
        # HTML 태그 제거
        text = re.sub(r'<[^>]+>', '', text)
        
        # 코드 블록 복원
        for i, code_block in enumerate(code_blocks):
            clean_code = self._clean_code_block(code_block)
            text = text.replace(f"__CODE_BLOCK_{i}__", f"\n\n{clean_code}\n\n")
        
        # 불필요한 공백 정리
        text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)  # 연속된 빈 줄 제거
        text = re.sub(r'[ \t]+', ' ', text)  # 연속된 공백 제거
        text = text.strip()
        
        return text
    
    def _clean_code_block(self, code: str) -> str:
        """코드 블록 내용 정리"""
        if not code:
            return ""
        
        # HTML 엔티티 디코딩
        code = html.unescape(code)
        
        # 불필요한 공백 정리
        code = code.strip()
        
        # Excel 공식인 경우 = 로 시작하도록 정리
        if self._is_excel_formula(code):
            if not code.startswith('='):
                code = '=' + code
        
        return code
    
    def extract_excel_formulas(self, text: str) -> List[str]:
        """텍스트에서 Excel 공식 추출 (개선된 버전)"""
        if not text:
            return []
        
        formulas = []
        
        # HTML 태그 제거
        clean_text = self.remove_html_tags(text)
        
        # 패턴 1: = 로 시작하는 기본 공식
        basic_formulas = re.findall(r'=[\w\(\),\s:!#&"\.\$\-\+\*/]+', clean_text)
        formulas.extend(basic_formulas)
        
        # 패턴 2: Excel 함수가 포함된 텍스트 (= 없이도)
        for func in self.all_excel_functions:
            # 함수명(로 시작하는 패턴
            func_pattern = rf'{func}\s*\([^)]*\)'
            func_matches = re.findall(func_pattern, clean_text, re.IGNORECASE)
            for match in func_matches:
                if not match.startswith('='):
                    match = '=' + match
                formulas.append(match)
        
        # 패턴 3: 셀 참조가 포함된 수식
        cell_formulas = re.findall(r'=?[A-Z]+\d+[:\$\w\(\),\s]*', clean_text)
        formulas.extend([f for f in cell_formulas if self._is_excel_formula(f)])
        
        # 중복 제거 및 정리
        unique_formulas = []
        for formula in formulas:
            clean_formula = self._normalize_formula(formula)
            if clean_formula and clean_formula not in unique_formulas:
                unique_formulas.append(clean_formula)
        
        return unique_formulas
    
    def _is_excel_formula(self, text: str) -> bool:
        """텍스트가 Excel 공식인지 판단"""
        if not text:
            return False
        
        text = text.strip()
        
        # = 로 시작하거나 Excel 함수가 포함된 경우
        if text.startswith('=') or any(func in text.upper() for func in self.all_excel_functions):
            return True
        
        # 셀 참조 패턴이 있는 경우 (A1, B2:C5 등)
        if re.search(r'[A-Z]+\d+', text):
            return True
        
        return False
    
    def _normalize_formula(self, formula: str) -> str:
        """공식 정규화"""
        if not formula:
            return ""
        
        formula = formula.strip()
        
        # = 로 시작하도록 수정
        if not formula.startswith('=') and self._is_excel_formula(formula):
            formula = '=' + formula
        
        # HTML 엔티티 정리
        formula = html.unescape(formula)
        
        # 따옴표 정리
        formula = formula.replace('&quot;', '"')
        formula = formula.replace('&#39;', "'")
        
        return formula
    
    def clean_qa_response(self, response: str) -> Dict[str, Any]:
        """Q&A 응답 전체 정리"""
        if not response:
            return {
                "clean_text": "",
                "extracted_formulas": [],
                "has_code": False
            }
        
        # HTML 태그 제거
        clean_text = self.remove_html_tags(response)
        
        # Excel 공식 추출
        formulas = self.extract_excel_formulas(response)
        
        return {
            "clean_text": clean_text,
            "extracted_formulas": formulas,
            "has_code": len(formulas) > 0
        }

def clean_dataset_responses(dataset_path: str, output_path: str) -> None:
    """데이터셋의 모든 응답 정리"""
    import json
    
    cleaner = TextCleaner()
    
    with open(dataset_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    cleaned_samples = []
    
    for sample in data.get('samples', []):
        if 'assistant_response' in sample:
            # 응답 정리
            cleaned = cleaner.clean_qa_response(sample['assistant_response'])
            
            # 업데이트된 샘플 생성
            updated_sample = sample.copy()
            updated_sample['assistant_response'] = cleaned['clean_text']
            updated_sample['code_blocks'] = cleaned['extracted_formulas']
            
            # 메타데이터 업데이트
            if 'metadata' not in updated_sample:
                updated_sample['metadata'] = {}
            updated_sample['metadata']['has_code'] = cleaned['has_code']
            updated_sample['metadata']['code_count'] = len(cleaned['extracted_formulas'])
            
            cleaned_samples.append(updated_sample)
    
    # 정리된 데이터셋 저장
    cleaned_dataset = data.copy()
    cleaned_dataset['samples'] = cleaned_samples
    cleaned_dataset['dataset_info']['processing_notes'] = "HTML tags removed, Excel formulas extracted and normalized"
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(cleaned_dataset, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Dataset cleaned and saved to {output_path}")
    logger.info(f"Processed {len(cleaned_samples)} samples")

# 테스트용 함수
def test_text_cleaner():
    """텍스트 클리너 테스트"""
    cleaner = TextCleaner()
    
    # 테스트 케이스들
    test_cases = [
        # HTML 태그가 있는 응답
        "<p>Use MAXIFS()</p>\n<pre><code>=MAXIFS(B:B,A:A,&quot;Apples&quot;)\n</code></pre><p>Make sure to format the output as a date.</p>",
        
        # 복잡한 공식
        "<p>The formula for Pascal's triangle is:</p>\n<pre><code>=LET(N,5,REDUCE(SEQUENCE(,N,1,0),SEQUENCE(N-1),\n    LAMBDA(y,z,VSTACK(y,SCAN(0,TAKE(y,-1),LAMBDA(a,x,a+x))))))\n</code></pre>",
        
        # 인라인 코드
        "Use <code>VLOOKUP(A1,Sheet2!A:B,2,FALSE)</code> to find the value.",
    ]
    
    print("🧪 텍스트 클리너 테스트 결과:")
    print("=" * 60)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n[테스트 {i}]")
        print(f"원본: {repr(test_case[:100])}...")
        
        result = cleaner.clean_qa_response(test_case)
        print(f"정리된 텍스트: {repr(result['clean_text'][:100])}...")
        print(f"추출된 공식: {result['extracted_formulas']}")
        print(f"코드 포함 여부: {result['has_code']}")
        print("-" * 40)

if __name__ == "__main__":
    test_text_cleaner()