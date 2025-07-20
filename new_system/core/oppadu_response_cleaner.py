#!/usr/bin/env python3
"""
오빠두 응답 텍스트 정리 및 코드 블록 추출 개선
"""

import re
import logging
from typing import Dict, List, Any
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class OppaduResponseCleaner:
    """오빠두 응답 정리 클래스"""
    
    def __init__(self):
        # 제거할 HTML 패턴들
        self.html_patterns = [
            r'<[^>]+>',  # HTML 태그
            r'&lt;', r'&gt;', r'&amp;', r'&quot;',  # HTML 엔티티
            r'비주얼 텍스트',  # 오빠두 UI 텍스트
            r'파일 첨부', r'저장', r'취소',  # UI 버튼 텍스트
            r'\d+일 전', r'\d+시간 전',  # 시간 표시
            r'좋아요', r'삭제',  # 액션 버튼
            r'Lv\.\d+',  # 레벨 표시
        ]
        
        # Excel 함수 패턴
        self.excel_function_pattern = r'=([A-Z]+\(.*?\)|\w+\([^)]*\))'
        
        # 한국 Excel 함수들
        self.korean_excel_functions = {
            '합계', '평균', '개수', '최대값', '최소값', '조건부합계', '조건부개수',
            '찾기', '바꾸기', '연결', '왼쪽', '오른쪽', '가운데', '길이'
        }
        
    def clean_response(self, raw_response: str) -> Dict[str, Any]:
        """오빠두 응답 정리 및 코드 블록 추출"""
        
        # 1단계: HTML 및 메타데이터 제거
        cleaned_text = self._remove_html_and_metadata(raw_response)
        
        # 2단계: Excel 공식 추출
        excel_formulas = self._extract_excel_formulas(raw_response)
        
        # 3단계: 설명 텍스트 추출
        explanation = self._extract_explanation(cleaned_text, excel_formulas)
        
        # 4단계: 최종 응답 구성
        final_response = self._build_final_response(excel_formulas, explanation)
        
        # 5단계: 최종 HTML 엔티티 정리 (마지막 안전장치)
        final_response = self._final_html_cleanup(final_response)
        
        return {
            'cleaned_response': final_response,
            'excel_formulas': excel_formulas,
            'has_excel_content': len(excel_formulas) > 0,
            'explanation': explanation
        }
    
    def _remove_html_and_metadata(self, text: str) -> str:
        """HTML 태그 및 메타데이터 제거"""
        
        # BeautifulSoup으로 HTML 정리
        soup = BeautifulSoup(text, 'html.parser')
        
        # <pre> 태그의 내용은 보존
        pre_contents = []
        for pre in soup.find_all('pre'):
            content = pre.get_text()
            pre_contents.append(content)
            pre.replace_with(f"__PRE_CONTENT_{len(pre_contents)-1}__")
        
        # HTML 태그 제거
        clean_text = soup.get_text()
        
        # 보존된 <pre> 내용 복원
        for i, content in enumerate(pre_contents):
            clean_text = clean_text.replace(f"__PRE_CONTENT_{i}__", content)
        
        # HTML 엔티티 완전 제거
        clean_text = self._clean_html_entities(clean_text)
        
        # 기타 패턴 제거
        for pattern in self.html_patterns:
            clean_text = re.sub(pattern, ' ', clean_text)
        
        # 중복 공백 제거
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        
        return clean_text
    
    def _clean_html_entities(self, text: str) -> str:
        """HTML 엔티티 완전 제거 (최종 강화 버전)"""
        # 1차: 복합 엔티티 먼저 처리
        replacements_complex = {
            '&amp;lt;': '<',
            '&amp;gt;': '>',
            '&amp;amp;': '&',
            '&amp;quot;': '"',
            '&amp;nbsp;': ' ',
        }
        
        for old, new in replacements_complex.items():
            text = text.replace(old, new)
        
        # 2차: 기본 엔티티 처리
        replacements_basic = {
            '&gt;': '>',
            '&lt;': '<',
            '&amp;': '&',
            '&quot;': '"',
            '&#39;': "'",
            '&nbsp;': ' ',
            '&ldquo;': '"',
            '&rdquo;': '"',
            '&apos;': "'",
            '&lsquo;': "'",
            '&rsquo;': "'"
        }
        
        for old, new in replacements_basic.items():
            text = text.replace(old, new)
        
        # 3차: 숫자 엔티티 처리
        import re
        text = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
        text = re.sub(r'&#x([0-9a-fA-F]+);', lambda m: chr(int(m.group(1), 16)), text)
        
        return text
    
    def _extract_excel_formulas(self, text: str) -> List[str]:
        """Excel 공식 추출"""
        formulas = []
        
        # = 로 시작하는 Excel 공식 찾기
        excel_patterns = [
            r'=\s*[A-Z]+\([^)]*\)',  # 기본 함수: =SUM(A1:A10)
            r'=\s*[A-Z]+\([^)]*\([^)]*\)[^)]*\)',  # 중첩 함수: =IF(A1>0, SUM(B1:B10), 0)
            r'=\s*[가-힣]+\([^)]*\)',  # 한국어 함수: =합계(A1:A10)
            r'=\s*\w+\([^=]*?\)',  # 복잡한 공식
        ]
        
        for pattern in excel_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            formulas.extend(matches)
        
        # 중복 제거 및 정리
        unique_formulas = []
        for formula in formulas:
            # 공식 정리
            cleaned_formula = re.sub(r'\s+', ' ', formula.strip())
            if cleaned_formula and len(cleaned_formula) > 2:  # 최소 길이 체크
                if cleaned_formula not in unique_formulas:
                    unique_formulas.append(cleaned_formula)
        
        return unique_formulas
    
    def _extract_explanation(self, text: str, formulas: List[str]) -> str:
        """설명 텍스트 추출"""
        
        # 공식 부분 제거
        explanation = text
        for formula in formulas:
            explanation = explanation.replace(formula, '')
        
        # 답변자 정보 제거 (개선된 패턴)
        explanation = re.sub(r'(마법의손|원조백수|업무수동화|bona)\*{0,4}\s*Lv\.\d+', '', explanation)
        explanation = re.sub(r'(마법의손|원조백수|업무수동화|bona)\*{0,4}', '', explanation)
        
        # 중복 텍스트 제거
        explanation = re.sub(r'비주얼 텍스트.*', '', explanation)
        explanation = re.sub(r'파일 첨부.*', '', explanation)
        explanation = re.sub(r'\d+일?\s*전.*', '', explanation)
        
        # 특수 문자 및 불필요한 텍스트 제거
        explanation = re.sub(r'[^\w\s가-힣\.,\(\)\[\]]+', ' ', explanation)
        explanation = re.sub(r'\s+', ' ', explanation).strip()
        
        # 너무 짧은 설명은 기본 설명으로 대체
        if len(explanation.split()) < 3:
            if formulas:
                explanation = "Excel 공식을 사용한 데이터 처리 방법입니다."
            else:
                explanation = "Excel 사용법에 대한 답변입니다."
        
        return explanation
    
    def extract_answerer_info(self, text: str) -> dict:
        """답변자 정보 추출"""
        import re
        
        # 답변자 패턴 매칭 (레벨 포함)
        answerer_patterns = [
            r'(마법의손|원조백수|업무수동화|bona)\*{0,4}\s*Lv\.(\d+)',
            r'(마법의손|원조백수|업무수동화|bona)\*{0,4}',
        ]
        
        for pattern in answerer_patterns:
            match = re.search(pattern, text)
            if match:
                result = {"name": match.group(1)}
                if len(match.groups()) > 1 and match.group(2):
                    result["level"] = f"Lv.{match.group(2)}"
                return result
        
        return {}
    
    def _build_final_response(self, formulas: List[str], explanation: str) -> str:
        """최종 응답 구성"""
        
        # 공식이 없는 경우 설명만 반환
        if not formulas:
            if explanation and len(explanation.split()) >= 3:
                return explanation
            else:
                return "Excel 사용법에 대한 답변입니다."
        
        # 공식이 있는 경우
        response_parts = []
        
        # 가장 완전한 공식 선택 (= 로 시작하고 괄호가 매칭되는 것)
        best_formula = None
        for formula in formulas:
            if formula.startswith('=') and formula.count('(') == formula.count(')'):
                if not best_formula or len(formula) > len(best_formula):
                    best_formula = formula
        
        if best_formula:
            response_parts.append(best_formula)
        elif formulas:
            # 완전한 공식이 없으면 가장 긴 것
            response_parts.append(max(formulas, key=len))
        
        # 설명 추가 (공식과 중복되지 않는 유용한 정보만)
        if explanation and len(explanation.split()) >= 5:
            # 공식 관련 단어가 너무 많이 포함되어 있으면 제외
            formula_keywords = ['=', 'SUM', 'IF', 'VLOOKUP', 'INDEX', 'MATCH']
            explanation_words = explanation.split()
            formula_word_count = sum(1 for word in explanation_words 
                                   if any(keyword in word.upper() for keyword in formula_keywords))
            
            # 공식 키워드가 전체의 30% 미만일 때만 설명 포함
            if formula_word_count / len(explanation_words) < 0.3:
                response_parts.append(explanation)
        
        if not response_parts:
            return "Excel 공식을 사용한 데이터 처리 방법입니다."
        
        final_text = ' '.join(response_parts)
        
        # 답변자 정보 완전 제거 (마지막 단계)
        final_text = self._clean_answerer_prefix(final_text)
        
        return final_text
    
    def extract_clean_code_blocks(self, text: str) -> List[str]:
        """정확한 코드 블록 추출 (개선된 버전)"""
        code_blocks = []
        
        # HTML 엔티티 먼저 정리
        text = self._clean_html_entities(text)
        
        # Excel 공식 패턴 (개선된)
        formula_patterns = [
            r'=\s*[A-Z]+\([^=]*?\)',  # 기본 함수
            r'=\s*LET\([^=]*?\)',  # LET 함수
            r'=\s*SUMPRODUCT\([^=]*?\)',  # SUMPRODUCT
            r'=\s*GROUPBY\([^=]*?\)',  # GROUPBY
            r'=\s*MMULT\([^=]*?\)',  # MMULT
            r'=\s*IF\([^=]*?\)',  # IF 함수
            r'=\s*SUMIFS?\([^=]*?\)',  # SUMIF/SUMIFS
        ]
        
        # VBA 코드 블록 찾기
        vba_patterns = [
            r'Sub\s+\w+\([^)]*\).*?End\s+Sub',
            r'Function\s+\w+\([^)]*\).*?End\s+Function',
        ]
        
        # Excel 공식 추출
        for pattern in formula_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
            for match in matches:
                cleaned = self._clean_excel_formula(match)
                if cleaned and len(cleaned) > 3:
                    code_blocks.append(cleaned)
        
        # VBA 코드 추출
        for pattern in vba_patterns:
            matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
            for match in matches:
                cleaned = re.sub(r'\s+', ' ', match.strip())
                if len(cleaned) > 10:
                    code_blocks.append(cleaned)
        
        # 중복 제거 및 정렬 (길이순)
        unique_blocks = list(set(code_blocks))
        unique_blocks.sort(key=len, reverse=True)
        
        return unique_blocks
    
    def _clean_excel_formula(self, formula: str) -> str:
        """Excel 공식 정리"""
        if not formula:
            return ""
        
        # HTML 엔티티 정리
        formula = formula.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
        
        # 불필요한 텍스트 제거
        formula = re.sub(r'비주얼 텍스트.*', '', formula)
        formula = re.sub(r'파일 첨부.*', '', formula)
        formula = re.sub(r'\d+일?\s*전.*', '', formula)
        
        # 공식만 추출 (= 로 시작하는 부분)
        if '=' in formula:
            # = 이후 첫 번째 완전한 공식 추출
            parts = formula.split('=')
            if len(parts) > 1:
                for part in parts[1:]:  # = 이후 부분들
                    # 괄호가 균형잡힌 첫 번째 부분 찾기
                    balanced_part = self._extract_balanced_formula(part)
                    if balanced_part:
                        return '=' + balanced_part
        
        return formula.strip()
    
    def _extract_balanced_formula(self, text: str) -> str:
        """괄호가 균형잡힌 공식 부분 추출"""
        if not text:
            return ""
        
        depth = 0
        result = ""
        
        for char in text:
            result += char
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
                if depth == 0:
                    # 괄호가 균형잡히면 여기서 종료
                    break
            elif char in [' ', '\n', '\t'] and depth == 0:
                # 괄호 밖에서 공백이 나오면 공식 끝
                break
        
        return result.strip()
    
    def analyze_formula_complexity(self, formulas: List[str]) -> dict:
        """공식 복잡도 분석"""
        if not formulas:
            return {
                "difficulty": "beginner",
                "functions": [],
                "complexity_score": 1.0
            }
        
        # 난이도별 함수 분류
        advanced_functions = ['MMULT', 'SUMPRODUCT', 'LAMBDA', 'LET', 'GROUPBY', 'PIVOTBY', 'XLOOKUP', 'FILTER']
        intermediate_functions = ['SUMIFS', 'COUNTIFS', 'INDEX', 'MATCH', 'VLOOKUP', 'INDIRECT', 'TRANSPOSE']
        
        all_functions = []
        max_difficulty = "beginner"
        complexity_score = 1.0
        
        for formula in formulas:
            # 함수 추출
            functions = re.findall(r'([A-Z][A-Z0-9]*)\s*\(', formula.upper())
            all_functions.extend(functions)
            
            # 난이도 판정
            if any(func in formula.upper() for func in advanced_functions):
                max_difficulty = "advanced"
                complexity_score = max(complexity_score, 3.0)
            elif any(func in formula.upper() for func in intermediate_functions):
                if max_difficulty != "advanced":
                    max_difficulty = "intermediate"
                    complexity_score = max(complexity_score, 2.0)
            
            # 중첩 괄호로 복잡도 추가 평가
            nesting_level = formula.count('(')
            if nesting_level > 3:
                complexity_score = max(complexity_score, 2.5)
                if max_difficulty == "beginner":
                    max_difficulty = "intermediate"
        
        return {
            "difficulty": max_difficulty,
            "functions": list(set(all_functions)),
            "complexity_score": complexity_score
        }
    
    def process_korean_excel_qa(self, data: dict) -> dict:
        """완전한 한국 Excel Q&A 처리 파이프라인"""
        
        # 1. 원본 응답에서 답변자 정보 추출
        answerer = self.extract_answerer_info(data.get('assistant_response', ''))
        
        # 2. 응답 정리
        cleaned_result = self.clean_response(data.get('assistant_response', ''))
        data['assistant_response'] = cleaned_result['cleaned_response']
        
        # 3. 정확한 코드 블록 추출
        code_blocks = self.extract_clean_code_blocks(data.get('assistant_response', ''))
        data['code_blocks'] = code_blocks
        
        # 4. 복잡도 분석
        complexity_info = self.analyze_formula_complexity(code_blocks)
        
        # 5. 메타데이터 업데이트
        if 'metadata' not in data:
            data['metadata'] = {}
        
        data['metadata'].update({
            'difficulty': complexity_info['difficulty'],
            'functions': complexity_info['functions'],
            'complexity_score': complexity_info['complexity_score']
        })
        
        # 6. 답변자 정보 추가
        if answerer:
            data['metadata']['answerer'] = answerer
        
        # 7. Excel 관련 메타데이터
        data['metadata'].update({
            'has_excel_formulas': len(code_blocks) > 0,
            'formula_count': len(code_blocks),
            'uses_advanced_functions': complexity_info['difficulty'] == 'advanced'
        })
        
        return data
    
    def _final_html_cleanup(self, text: str) -> str:
        """최종 HTML 엔티티 정리 (마지막 안전장치)"""
        # 가장 마지막에 실행되는 완전한 HTML 엔티티 제거
        text = text.replace('&amp;lt;', '<').replace('&amp;gt;', '>')
        text = text.replace('&lt;', '<').replace('&gt;', '>')
        text = text.replace('&amp;', '&').replace('&quot;', '"')
        return text
    
    def _clean_answerer_prefix(self, text: str) -> str:
        """답변자 정보 완전 정리 (최종 강화 버전)"""
        import re
        
        # 1단계: 시작 부분 답변자 패턴들
        start_patterns = [
            r'^[가-힣\w\s]*님\s*',  # "고녱 님", "cynicalH 님" 등
            r'^cynicalH\s*님?\s*',
            r'^고녱\s*님?\s*',
            r'^마법의손\s*\*{0,4}\s*Lv\.\d+\s*',
            r'^원조백수\s*\*{0,4}\s*Lv\.\d+\s*',
            r'^업무수동화\s*\*{0,4}\s*Lv\.\d+\s*',
            r'^bona\s*\*{0,4}\s*Lv\.\d+\s*',
            r'^[가-힣]+\s*\*{0,4}\s*Lv\.\d+\s*',  # 일반적인 닉네임 + 레벨
        ]
        
        for pattern in start_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        # 2단계: 중간에 섞인 답변자 정보 제거 (강화)
        middle_patterns = [
            r'\s+님\s+위와\s+같이',  # " 님 위와 같이"
            r'\s+님\s+',  # 일반적인 " 님 " 패턴
            r'cynicalH\s*님?',
            r'고녱\s*님?',
            r'마법의손\s*\*{0,4}',
            r'원조백수\s*\*{0,4}',
            r'업무수동화\s*\*{0,4}',
            r'Lv\.\d+',
            r'\*{2,}',  # 연속된 별표들
        ]
        
        for pattern in middle_patterns:
            text = re.sub(pattern, ' ', text, flags=re.IGNORECASE)
        
        # 3단계: 중복 공백 정리
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def manual_formula_fixes(self, item_id: str, response: str) -> str:
        """특정 아이템의 불완전한 공식 수동 수정"""
        
        # korean_excel_qa_2c9a01e6 특별 처리
        if item_id == "korean_excel_qa_2c9a01e6" or "TRANSPOSE(C3:C8)" in response:
            # 원본 완전한 SUMPRODUCT 공식으로 복원
            corrected_formula = "=SUMPRODUCT((MMULT((ROW(INDIRECT(MIN(C3:D8)&\":\"&MAX(C3:D8)))>=TRANSPOSE(C3:C8))*(ROW(INDIRECT(MIN(C3:D8)&\":\"&MAX(C3:D8)))<=TRANSPOSE(D3:D8)),ROW(C3:D8)^0)<>0)*1)"
            
            # 기존 불완전한 공식 제거하고 완전한 공식으로 교체
            if "= TRANSPOSE(C3:C8)" in response:
                response = response.replace(
                    "= TRANSPOSE(C3:C8) MAX(C3 D8))) ) (ROW(INDIRECT(MIN(C3 D8) MAX(C3 D8))) 0) 1 )",
                    corrected_formula
                )
            
            return response
        
        return response


# 사용 예시
if __name__ == "__main__":
    cleaner = OppaduResponseCleaner()
    
    # 테스트 데이터
    test_response = """마법의손 Lv.30 =SUMPRODUCT( (MMULT( (ROW(INDIRECT(MIN(C3:D8)&\":\"&MAX(C3:D8))) >= TRANSPOSE(C3:C8))* (ROW(INDIRECT(MIN(C3:D8)&\":\"&MAX(C3:D8))) <= TRANSPOSE(D3:D8)), ROW(C3:D8)^0)<>0)*1 ) 비주얼 텍스트 <pre lang="vbnet" escaped="true">=SUMPRODUCT( (MMULT( (ROW(INDIRECT(MIN(C3:D8)&\":\"&MAX(C3:D8))) >= TRANSPOSE(C3:C8))*<br />(ROW(INDIRECT(MIN(C3:D8)&\":\"&MAX(C3:D8))) <= TRANSPOSE(D3:D8)), ROW(C3:D8)^0)<>0)*1 )</pre><br /> 파일 첨부 저장 취소 1일 전 3"""
    
    result = cleaner.clean_response(test_response)
    print("정리된 응답:", result['cleaned_response'])
    print("Excel 공식:", result['excel_formulas'])