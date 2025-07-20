"""
Simple and Practical Reddit Quality Scorer
AI 학습에 적합한 실용적 품질 평가 시스템
"""
import logging
from typing import Dict, Any, List
from dataclasses import dataclass

logger = logging.getLogger('pipeline.simple_reddit_scorer')

@dataclass
class SimpleRedditQuality:
    """간단한 Reddit 품질 평가 결과"""
    overall_score: float
    raw_question_score: float
    raw_answer_score: float
    meets_threshold: bool
    reason: str

class SimpleRedditScorer:
    """
    실용적이고 안정적인 Reddit 품질 평가기
    
    핵심 원칙:
    1. 텍스트가 있으면 기본적으로 유효
    2. 간단하고 명확한 기준
    3. AI 학습에 도움이 되는 데이터 우선
    4. 과도한 필터링 방지
    """
    
    def __init__(self):
        self.min_question_length = 10  # 최소 질문 길이
        self.min_answer_length = 10    # 최소 답변 길이
        self.base_score = 5.0          # 기본 점수
        
    def score_batch(self, reddit_pairs: List[Dict[str, Any]]) -> List[SimpleRedditQuality]:
        """Reddit 데이터 배치 점수 계산"""
        results = []
        
        for pair in reddit_pairs:
            quality = self.score_single_item(pair)
            results.append(quality)
            
        logger.info(f"Simple Reddit scoring: {len(results)} items processed")
        return results
    
    def score_single_item(self, pair: Dict[str, Any]) -> SimpleRedditQuality:
        """개별 Reddit 항목 점수 계산"""
        question_data = pair.get('question', {})
        answer_data = pair.get('answer', {})
        
        # 🚨 봇 응답 완전 차단 (최우선) - 고급 봇 감지 시스템
        answer_text = answer_data.get('text', '') or answer_data.get('body_markdown', '')
        from bot_detection.advanced_bot_detector import advanced_detector
        
        # 답변과 질문 둘 다 고급 봇 감지
        question_text = question_data.get('text', '') or question_data.get('body_markdown', '')
        
        # 답변 봇 감지
        answer_bot_result = advanced_detector.detect_bot_comprehensive({
            'body': answer_text,
            'author': answer_data.get('author', ''),
            'score': answer_data.get('score', 0),
            'created_utc': answer_data.get('created_utc', 0)
        })
        
        if answer_bot_result.is_bot:
            logger.warning(f"🚨 Bot response detected and rejected (confidence: {answer_bot_result.confidence:.2f}): {answer_text[:50]}...")
            return SimpleRedditQuality(
                overall_score=0.0,
                raw_question_score=0.0,
                raw_answer_score=0.0,
                meets_threshold=False,
                reason=f"Bot response rejected ({answer_bot_result.bot_type.value})"
            )
        
        # 질문 봇 감지
        question_bot_result = advanced_detector.detect_bot_comprehensive({
            'body': question_text,
            'author': question_data.get('author', ''),
            'score': question_data.get('score', 0),
            'created_utc': question_data.get('created_utc', 0)
        })
        
        if question_bot_result.is_bot:
            logger.warning(f"🚨 Bot question detected and rejected (confidence: {question_bot_result.confidence:.2f}): {question_text[:50]}...")
            return SimpleRedditQuality(
                overall_score=0.0,
                raw_question_score=0.0,
                raw_answer_score=0.0,
                meets_threshold=False,
                reason=f"Bot question rejected ({question_bot_result.bot_type.value})"
            )
        
        # 텍스트 추출 (이미 추출됨)
        # question_text = question_data.get('text', '')
        # answer_text = answer_data.get('text', '')
        
        # 위에서 발생한 발생시간을 재사용
        if not question_text:
            question_text = question_data.get('text', '')
        if not answer_text:
            answer_text = answer_data.get('text', '')
        
        # 기본 점수 계산
        question_score = self._score_text_quality(question_text, is_question=True)
        answer_score = self._score_text_quality(answer_text, is_question=False)
        
        # 전체 점수 (가중 평균)
        overall_score = (question_score * 0.4) + (answer_score * 0.6)
        
        # 통과 여부 결정
        meets_threshold = self._meets_quality_threshold(question_text, answer_text, overall_score)
        
        # 점수 이유
        reason = self._get_score_reason(question_text, answer_text, overall_score)
        
        return SimpleRedditQuality(
            overall_score=overall_score,
            raw_question_score=question_score,
            raw_answer_score=answer_score,
            meets_threshold=meets_threshold,
            reason=reason
        )
    
    def _score_text_quality(self, text: str, is_question: bool = True) -> float:
        """텍스트 품질 점수 계산"""
        if not text or not text.strip():
            return 0.0
        
        text = text.strip()
        score = self.base_score
        
        # 길이 기반 점수
        length = len(text)
        if length >= 200:
            score += 2.0  # 상세한 설명
        elif length >= 100:
            score += 1.0  # 적절한 길이
        elif length < self.min_question_length if is_question else self.min_answer_length:
            score -= 2.0  # 너무 짧음
        
        # 질문 특화 평가
        if is_question:
            if '?' in text:
                score += 1.0  # 명확한 질문
            if any(keyword in text.lower() for keyword in ['how', 'what', 'why', 'help', 'error', 'issue']):
                score += 0.5  # 질문 키워드
        
        # 답변 특화 평가
        else:
            if any(keyword in text.lower() for keyword in ['=', 'formula', 'try', 'use', 'solution']):
                score += 1.0  # 해결책 제시
            if len(text) > 50:  # 최소한의 답변 길이
                score += 0.5
        
        # 코드/공식 포함 여부
        if '=' in text or 'VLOOKUP' in text.upper() or 'SUMIF' in text.upper():
            score += 1.0  # Excel 관련 내용
        
        return max(0.0, min(10.0, score))  # 0-10 범위로 제한
    
    def _meets_quality_threshold(self, question_text: str, answer_text: str, overall_score: float) -> bool:
        """품질 임계값 통과 여부"""
        # 기본 조건: 둘 다 텍스트가 있어야 함
        if not question_text.strip() or not answer_text.strip():
            return False
        
        # 최소 길이 조건
        if len(question_text.strip()) < self.min_question_length:
            return False
        if len(answer_text.strip()) < self.min_answer_length:
            return False
        
        # 점수 기준 (매우 관대함)
        return overall_score >= 3.0
    
    def _get_score_reason(self, question_text: str, answer_text: str, score: float) -> str:
        """점수 산정 이유"""
        if not question_text.strip():
            return "빈 질문 텍스트"
        if not answer_text.strip():
            return "빈 답변 텍스트"
        if len(question_text.strip()) < self.min_question_length:
            return f"질문이 너무 짧음 ({len(question_text)}자)"
        if len(answer_text.strip()) < self.min_answer_length:
            return f"답변이 너무 짧음 ({len(answer_text)}자)"
        if score >= 7.0:
            return "고품질 Q&A"
        elif score >= 5.0:
            return "양호한 품질"
        elif score >= 3.0:
            return "기본 품질 충족"
        else:
            return "품질 기준 미달"
    
    def filter_by_quality(self, reddit_pairs: List[Dict[str, Any]], 
                         quality_results: List[SimpleRedditQuality]) -> List[Dict[str, Any]]:
        """품질 기준에 따른 필터링"""
        filtered = []
        
        for i, (pair, quality) in enumerate(zip(reddit_pairs, quality_results)):
            if quality.meets_threshold:
                # 품질 메트릭 추가
                pair['quality_metrics'] = {
                    'overall_score': quality.overall_score,
                    'raw_question_score': quality.raw_question_score,
                    'raw_answer_score': quality.raw_answer_score
                }
                filtered.append(pair)
                logger.debug(f"Item {i+1} passed: {quality.reason} (score: {quality.overall_score:.1f})")
            else:
                logger.debug(f"Item {i+1} filtered: {quality.reason} (score: {quality.overall_score:.1f})")
        
        return filtered
    
    def get_batch_statistics(self, quality_results: List[SimpleRedditQuality]) -> Dict[str, Any]:
        """배치 통계 정보"""
        if not quality_results:
            return {}
        
        scores = [q.overall_score for q in quality_results]
        passed = [q for q in quality_results if q.meets_threshold]
        
        return {
            'total_items': len(quality_results),
            'passed_items': len(passed),
            'pass_rate': len(passed) / len(quality_results) * 100,
            'average_score': sum(scores) / len(scores),
            'min_score': min(scores),
            'max_score': max(scores)
        }