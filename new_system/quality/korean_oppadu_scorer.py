"""
오빠두(한국 커뮤니티) 전용 품질 평가 시스템
한국 Excel 사용자 특성과 한국어 콘텐츠를 고려한 품질 평가
"""

import logging
import re
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger('pipeline.korean_oppadu_scorer')

class KoreanOppaduScorer:
    """
    오빠두 한국 커뮤니티 전용 품질 평가기
    - 한국어 콘텐츠 특성 반영
    - 한국 비즈니스 환경 고려
    - Excel 한국 버전 특화 기능 평가
    """
    
    def __init__(self):
        # 한국 Excel 함수명 (한글/영문 혼재)
        self.korean_excel_functions = {
            '합계', 'SUM', '평균', 'AVERAGE', '개수', 'COUNT', '최대값', 'MAX', '최소값', 'MIN',
            '조건부합계', 'SUMIF', '조건부개수', 'COUNTIF', '검색', 'VLOOKUP', 'HLOOKUP',
            '만약', 'IF', '그리고', 'AND', '또는', 'OR', '아니면', 'NOT',
            '찾기', 'FIND', '바꾸기', 'SUBSTITUTE', '연결', 'CONCATENATE',
            '날짜', 'DATE', '오늘', 'TODAY', '지금', 'NOW', '년', 'YEAR', '월', 'MONTH', '일', 'DAY',
            '반올림', 'ROUND', '올림', 'ROUNDUP', '내림', 'ROUNDDOWN',
            '절대값', 'ABS', '제곱근', 'SQRT', '거듭제곱', 'POWER'
        }
        
        # 한국 비즈니스 관련 키워드
        self.korean_business_terms = {
            '세무', '세금', '부가세', 'VAT', '소득세', '법인세', '원천세',
            '회계', '장부', '분개', '대차대조표', '손익계산서', '현금흐름표',
            '급여', '인사', '근태', '연차', '퇴직금', '4대보험',
            '매출', '매입', '재고', '자산', '부채', '자본',
            '예산', '결산', '감가상각', '대손충당금',
            '사업자등록번호', '주민등록번호', '법인번호'
        }
        
        # 한국 Excel 특화 기능
        self.korean_excel_features = {
            '한글정렬', '음성정렬', '초성검색', '한자변환',
            '원화표시', '천단위구분', '음수표시',
            '한글숫자', '한자숫자', '대소문자변환',
            '공휴일', '영업일', '근무일'
        }
        
        logger.info("KoreanOppaduScorer initialized for Korean Excel community")
    
    def score_single(self, post_data: Dict[str, Any]) -> Dict[str, Any]:
        """단일 오빠두 게시글 품질 평가"""
        try:
            scores = {
                'content_quality': self._evaluate_content_quality(post_data),
                'korean_relevance': self._evaluate_korean_relevance(post_data),
                'excel_expertise': self._evaluate_excel_expertise(post_data),
                'practical_value': self._evaluate_practical_value(post_data),
                'completeness': self._evaluate_completeness(post_data)
            }
            
            # 가중 평균 계산 (한국 특성 강화)
            weights = {
                'content_quality': 0.25,
                'korean_relevance': 0.20,  # 한국 특성 가중치 높임
                'excel_expertise': 0.25,
                'practical_value': 0.20,
                'completeness': 0.10
            }
            
            overall_score = sum(scores[key] * weights[key] for key in scores)
            
            return {
                'overall_score': round(overall_score, 2),
                'component_scores': scores,
                'korean_context': self._analyze_korean_context(post_data),
                'quality_tier': self._determine_quality_tier(overall_score)
            }
            
        except Exception as e:
            logger.error(f"Error scoring Oppadu post: {e}")
            return {
                'overall_score': 5.0,
                'component_scores': {},
                'korean_context': {},
                'quality_tier': 'medium'
            }
    
    def _evaluate_content_quality(self, post_data: Dict[str, Any]) -> float:
        """콘텐츠 품질 평가"""
        score = 5.0
        
        question = post_data.get('question', {})
        answer = post_data.get('answer', {})
        
        # 텍스트 길이 평가 (한국어 특성 고려)
        q_text = question.get('text', '')
        a_text = answer.get('text', '')
        
        # 한국어는 영어보다 정보 밀도가 높으므로 단어 수 기준 완화
        q_words = len(q_text.split())
        a_words = len(a_text.split())
        
        if q_words >= 8:  # 한국어 기준 완화
            score += 0.5
        if q_words >= 15:
            score += 0.5
        
        if a_words >= 10:  # 한국어 기준 완화
            score += 1.0
        if a_words >= 25:
            score += 1.0
        
        # 구체적인 설명 평가
        detail_indicators = ['예를 들어', '구체적으로', '단계별로', '방법은', '순서는']
        combined_text = f"{q_text} {a_text}"
        detail_count = sum(1 for indicator in detail_indicators if indicator in combined_text)
        score += detail_count * 0.3
        
        return min(score, 10.0)
    
    def _evaluate_korean_relevance(self, post_data: Dict[str, Any]) -> float:
        """한국 특화 관련성 평가"""
        score = 5.0
        
        question = post_data.get('question', {})
        answer = post_data.get('answer', {})
        metadata = post_data.get('metadata', {})
        
        combined_text = f"{question.get('text', '')} {answer.get('text', '')}"
        
        # 한국 비즈니스 용어 점수
        business_term_count = sum(1 for term in self.korean_business_terms 
                                if term in combined_text)
        score += min(business_term_count * 0.5, 2.0)
        
        # 한국 Excel 특화 기능 언급
        feature_count = sum(1 for feature in self.korean_excel_features 
                          if feature in combined_text)
        score += min(feature_count * 0.7, 2.0)
        
        # 시스템 정보 (한국 환경)
        excel_version = metadata.get('excel_version', '')
        os_version = metadata.get('os_version', '')
        
        # 한국어 버전 Excel 사용 여부
        korean_indicators = ['한국어', '한글', 'KOR', '2016', '2019', '2021', '365']
        if any(indicator in excel_version for indicator in korean_indicators):
            score += 0.5
        
        # 한국 통화/날짜 형식 언급
        korean_formats = ['원', '₩', 'yyyy-mm-dd', 'yyyy.mm.dd', 'yy/mm/dd']
        format_count = sum(1 for fmt in korean_formats if fmt in combined_text)
        score += min(format_count * 0.3, 1.0)
        
        return min(score, 10.0)
    
    def _evaluate_excel_expertise(self, post_data: Dict[str, Any]) -> float:
        """Excel 전문성 평가"""
        score = 5.0
        
        question = post_data.get('question', {})
        answer = post_data.get('answer', {})
        
        combined_text = f"{question.get('text', '')} {answer.get('text', '')}"
        
        # Excel 함수 사용 (한글/영문 혼재)
        function_count = sum(1 for func in self.korean_excel_functions 
                           if func in combined_text)
        score += min(function_count * 0.4, 3.0)
        
        # 수식/공식 존재
        if question.get('has_code') or answer.get('has_code'):
            score += 1.5
        
        # 고급 기능 언급
        advanced_features = ['피벗테이블', '피벗', '매크로', 'VBA', '파워쿼리', '파워피벗', 
                           '조건부서식', '데이터유효성', '시나리오', '목표값찾기']
        advanced_count = sum(1 for feature in advanced_features if feature in combined_text)
        score += min(advanced_count * 0.8, 2.0)
        
        # 수식 복잡도
        formula_patterns = [r'=\w+\(.*\)', r'IF\s*\(', r'VLOOKUP\s*\(', r'INDEX\s*\(.*MATCH']
        complex_formula_count = sum(1 for pattern in formula_patterns 
                                  if re.search(pattern, combined_text, re.IGNORECASE))
        score += min(complex_formula_count * 0.6, 2.0)
        
        return min(score, 10.0)
    
    def _evaluate_practical_value(self, post_data: Dict[str, Any]) -> float:
        """실용적 가치 평가"""
        score = 5.0
        
        question = post_data.get('question', {})
        answer = post_data.get('answer', {})
        
        combined_text = f"{question.get('text', '')} {answer.get('text', '')}"
        
        # 실무 관련 키워드
        practical_keywords = ['업무', '회사', '직장', '실무', '현업', '보고서', '양식', 
                            '템플릿', '자동화', '효율', '단축키', '빠르게']
        practical_count = sum(1 for keyword in practical_keywords if keyword in combined_text)
        score += min(practical_count * 0.4, 2.0)
        
        # 구체적인 예시나 스크린샷
        if question.get('images') or answer.get('images'):
            score += 1.5
        
        # 단계별 설명
        step_indicators = ['1단계', '2단계', '첫째', '둘째', '먼저', '다음에', '마지막으로']
        step_count = sum(1 for indicator in step_indicators if indicator in combined_text)
        score += min(step_count * 0.3, 1.5)
        
        # 대안 제시
        alternative_indicators = ['또는', '다른방법', '대신', '아니면', '방법1', '방법2']
        alt_count = sum(1 for indicator in alternative_indicators if indicator in combined_text)
        score += min(alt_count * 0.4, 1.0)
        
        return min(score, 10.0)
    
    def _evaluate_completeness(self, post_data: Dict[str, Any]) -> float:
        """답변 완성도 평가"""
        score = 5.0
        
        question = post_data.get('question', {})
        answer = post_data.get('answer', {})
        metadata = post_data.get('metadata', {})
        
        # 질문 구체성
        q_text = question.get('text', '')
        if len(q_text.split()) >= 10:
            score += 1.0
        
        # 답변 상세도
        a_text = answer.get('text', '')
        if len(a_text.split()) >= 15:
            score += 1.5
        
        # 시스템 정보 제공
        if metadata.get('excel_version'):
            score += 0.5
        if metadata.get('os_version'):
            score += 0.5
        
        # 감사 인사 (한국 커뮤니티 특성)
        gratitude_keywords = ['감사', '고맙', '도움', '해결', '완료']
        if any(keyword in f"{q_text} {a_text}" for keyword in gratitude_keywords):
            score += 0.5
        
        return min(score, 10.0)
    
    def _analyze_korean_context(self, post_data: Dict[str, Any]) -> Dict[str, Any]:
        """한국 특화 컨텍스트 분석"""
        question = post_data.get('question', {})
        answer = post_data.get('answer', {})
        metadata = post_data.get('metadata', {})
        
        combined_text = f"{question.get('text', '')} {answer.get('text', '')}"
        
        return {
            'has_korean_business_terms': any(term in combined_text for term in self.korean_business_terms),
            'uses_korean_excel_functions': any(func in combined_text for func in self.korean_excel_functions),
            'mentions_korean_formats': any(fmt in combined_text for fmt in ['원', '₩', 'yyyy-mm-dd']),
            'excel_version_korean': '한국어' in metadata.get('excel_version', '') or 'KOR' in metadata.get('excel_version', ''),
            'business_domain': self._identify_business_domain(combined_text),
            'complexity_level': self._assess_complexity_level(combined_text)
        }
    
    def _identify_business_domain(self, text: str) -> str:
        """비즈니스 도메인 식별"""
        domain_keywords = {
            'accounting': ['회계', '장부', '분개', '결산', '세무'],
            'hr': ['인사', '급여', '근태', '연차', '퇴직'],
            'sales': ['매출', '영업', '고객', '거래'],
            'finance': ['재무', '자금', '투자', '예산', '현금'],
            'inventory': ['재고', '창고', '입출고', '물류'],
            'general': ['일반', '기타']
        }
        
        for domain, keywords in domain_keywords.items():
            if any(keyword in text for keyword in keywords):
                return domain
        
        return 'general'
    
    def _assess_complexity_level(self, text: str) -> str:
        """복잡도 수준 평가"""
        # 고급 함수나 기능 확인
        advanced_patterns = [
            r'INDEX\s*\(.*MATCH', r'SUMPRODUCT', r'ARRAY',
            '피벗테이블', '매크로', 'VBA', '파워쿼리'
        ]
        
        if any(re.search(pattern, text, re.IGNORECASE) for pattern in advanced_patterns):
            return 'advanced'
        
        # 중급 함수 확인
        intermediate_functions = ['VLOOKUP', 'HLOOKUP', 'IF', 'SUMIF', 'COUNTIF']
        if any(func in text for func in intermediate_functions):
            return 'intermediate'
        
        return 'beginner'
    
    def _determine_quality_tier(self, score: float) -> str:
        """품질 등급 결정"""
        if score >= 8.5:
            return 'excellent'
        elif score >= 7.0:
            return 'good'
        elif score >= 5.5:
            return 'medium'
        else:
            return 'low'
    
    def score_batch(self, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """배치 품질 평가"""
        results = []
        
        for post in posts:
            try:
                score_result = self.score_single(post)
                results.append(score_result)
            except Exception as e:
                logger.error(f"Error scoring post in batch: {e}")
                results.append({
                    'overall_score': 5.0,
                    'component_scores': {},
                    'korean_context': {},
                    'quality_tier': 'medium'
                })
        
        return results
    
    def filter_by_quality(self, posts: List[Dict[str, Any]], 
                         scores: List[Dict[str, Any]], 
                         threshold: float = 6.0) -> List[Dict[str, Any]]:
        """품질 기준으로 필터링"""
        filtered_posts = []
        
        for post, score_result in zip(posts, scores):
            if score_result['overall_score'] >= threshold:
                # 스코어 정보를 게시글에 추가
                post['quality_metrics'] = score_result
                filtered_posts.append(post)
        
        logger.info(f"Oppadu quality filtering: {len(filtered_posts)}/{len(posts)} posts passed "
                   f"(threshold: {threshold})")
        
        return filtered_posts
    
    def get_batch_statistics(self, scores: List[Dict[str, Any]]) -> Dict[str, Any]:
        """배치 통계 정보"""
        if not scores:
            return {}
        
        overall_scores = [s['overall_score'] for s in scores]
        quality_tiers = [s['quality_tier'] for s in scores]
        
        return {
            'total_posts': len(scores),
            'average_score': sum(overall_scores) / len(overall_scores),
            'min_score': min(overall_scores),
            'max_score': max(overall_scores),
            'quality_distribution': {
                tier: quality_tiers.count(tier) for tier in set(quality_tiers)
            },
            'korean_business_posts': sum(1 for s in scores 
                                       if s.get('korean_context', {}).get('has_korean_business_terms', False)),
            'advanced_posts': sum(1 for s in scores 
                                if s.get('korean_context', {}).get('complexity_level') == 'advanced')
        }