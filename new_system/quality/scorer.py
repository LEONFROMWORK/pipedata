"""
Quality Scoring Algorithm Implementation
TRD Section 4: 정량적 품질 평가 기준 (Scoring Algorithm)

공식: QualityScore = (w_q * Norm_Q_Score) + (w_a * Norm_A_Score) + (w_c * Completion_Bonus)
가중치: w_q = 0.4, w_a = 0.5, w_c = 0.1
"""
import logging
import math
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import numpy as np

logger = logging.getLogger('pipeline.quality_scorer')

@dataclass
class QualityScoreComponents:
    """Individual components of quality score"""
    raw_question_score: float
    raw_answer_score: float
    completion_bonus: float
    normalized_question_score: float
    normalized_answer_score: float
    final_score: float
    
@dataclass
class QualityMetrics:
    """Complete quality assessment for a Q&A pair"""
    overall_score: float
    score_components: QualityScoreComponents
    meets_threshold: bool
    quality_tier: str  # 'excellent', 'good', 'fair', 'poor'
    
class QualityScorer:
    """
    Implementation of TRD Section 4 quality scoring algorithm
    
    Scoring Components:
    1. Question Score (Q_Score): log10(view_count + 1) + (score * 2) + log10(owner_reputation + 1)
    2. Answer Score (A_Score): (score * 2) + (is_accepted * 5) + log10(owner_reputation + 1)  
    3. Completion Bonus: Base(1) + Code blocks(2) + Image context(3)
    
    Normalization: Min-Max within batch for fairness
    """
    
    def __init__(self):
        self.config = Config.QUALITY_SCORING
        self.weights = self.config['weights']
        self.completion_config = self.config['completion_bonus']
        self.threshold = self.config['threshold']
        
        logger.info(f"QualityScorer initialized with threshold {self.threshold}")
    
    def score_batch(self, qa_pairs: List[Dict[str, Any]]) -> List[QualityMetrics]:
        """
        Score a batch of Q&A pairs with batch normalization
        
        Args:
            qa_pairs: List of Q&A data from Stack Overflow
            
        Returns:
            List of QualityMetrics for each Q&A pair
        """
        if not qa_pairs:
            return []
        
        logger.info(f"Scoring batch of {len(qa_pairs)} Q&A pairs")
        
        # Step 1: Calculate raw scores for all items
        raw_scores = []
        for qa_pair in qa_pairs:
            raw_q_score = self._calculate_question_score(qa_pair['question'])
            raw_a_score = self._calculate_answer_score(qa_pair.get('answer', {}))
            completion_bonus = self._calculate_completion_bonus(qa_pair)
            
            raw_scores.append({
                'q_score': raw_q_score,
                'a_score': raw_a_score,
                'completion': completion_bonus
            })
        
        # Step 2: Normalize scores within batch (TRD requirement)
        normalized_scores = self._normalize_scores_batch(raw_scores)
        
        # Step 3: Calculate final scores
        quality_metrics = []
        for i, qa_pair in enumerate(qa_pairs):
            raw = raw_scores[i]
            normalized = normalized_scores[i]
            
            # Apply weighted formula from TRD
            final_score = (
                self.weights['w_q'] * normalized['q_score'] +
                self.weights['w_a'] * normalized['a_score'] +
                self.weights['w_c'] * normalized['completion']
            )
            
            # Scale to 0-10 range
            final_score *= 10
            
            components = QualityScoreComponents(
                raw_question_score=raw['q_score'],
                raw_answer_score=raw['a_score'],
                completion_bonus=raw['completion'],
                normalized_question_score=normalized['q_score'],
                normalized_answer_score=normalized['a_score'],
                final_score=final_score
            )
            
            quality_metrics.append(QualityMetrics(
                overall_score=final_score,
                score_components=components,
                meets_threshold=final_score >= self.threshold,
                quality_tier=self._determine_quality_tier(final_score)
            ))
        
        logger.info(f"Batch scoring complete: {sum(1 for qm in quality_metrics if qm.meets_threshold)} items above threshold")
        return quality_metrics
    
    def _calculate_question_score(self, question: Dict[str, Any]) -> float:
        """
        Calculate question score using TRD formula:
        Q_Score = log10(view_count + 1) + (score * 2) + log10(owner_reputation + 1)
        """
        view_count = question.get('view_count', 0)
        score = question.get('score', 0)
        owner_reputation = question.get('owner', {}).get('reputation', 1)
        
        # Apply TRD formula
        view_component = math.log10(view_count + 1)
        score_component = score * 2
        reputation_component = math.log10(owner_reputation + 1)
        
        q_score = view_component + score_component + reputation_component
        
        logger.debug(f"Question score: views={view_count}, score={score}, rep={owner_reputation} -> {q_score:.2f}")
        return q_score
    
    def _calculate_answer_score(self, answer: Dict[str, Any]) -> float:
        """
        Calculate answer score using TRD formula:
        A_Score = (score * 2) + (is_accepted * 5) + log10(owner_reputation + 1)
        """
        if not answer:
            return 0.0
        
        score = answer.get('score', 0)
        is_accepted = 1 if answer.get('is_accepted', False) else 0
        owner_reputation = answer.get('owner', {}).get('reputation', 1)
        
        # Apply TRD formula
        score_component = score * 2
        acceptance_component = is_accepted * 5  # Big bonus for accepted answers
        reputation_component = math.log10(owner_reputation + 1)
        
        a_score = score_component + acceptance_component + reputation_component
        
        logger.debug(f"Answer score: score={score}, accepted={is_accepted}, rep={owner_reputation} -> {a_score:.2f}")
        return a_score
    
    def _calculate_completion_bonus(self, qa_pair: Dict[str, Any]) -> float:
        """
        Calculate completion bonus using TRD criteria:
        Base(1) + Code blocks(2) + Image context(3)
        """
        bonus = self.completion_config['base']  # Base bonus: 1
        
        # Check for code blocks (from text processing)
        text_result = qa_pair.get('text_processing', {})
        if text_result.get('has_code_blocks', False) or len(text_result.get('code_blocks', [])) > 0:
            bonus += self.completion_config['code_blocks']  # +2
            logger.debug("Code blocks bonus applied")
        
        # Check for image context (from image processing)
        image_result = qa_pair.get('image_processing', {})
        if image_result.get('success', False) and len(image_result.get('extracted_content', '')) > 0:
            bonus += self.completion_config['image_context']  # +3
            logger.debug("Image context bonus applied")
        
        logger.debug(f"Completion bonus: {bonus}")
        return bonus
    
    def _normalize_scores_batch(self, raw_scores: List[Dict[str, float]]) -> List[Dict[str, float]]:
        """
        Apply Min-Max normalization within batch (TRD Section 4)
        """
        if len(raw_scores) <= 1:
            # Can't normalize single item, return as-is
            return [{'q_score': 1.0, 'a_score': 1.0, 'completion': 1.0} for _ in raw_scores]
        
        # Extract score arrays
        q_scores = [s['q_score'] for s in raw_scores]
        a_scores = [s['a_score'] for s in raw_scores]
        completion_scores = [s['completion'] for s in raw_scores]
        
        # Min-Max normalization
        def min_max_normalize(scores):
            min_score = min(scores)
            max_score = max(scores)
            if max_score == min_score:
                return [1.0] * len(scores)  # All equal, normalize to 1.0
            return [(s - min_score) / (max_score - min_score) for s in scores]
        
        norm_q_scores = min_max_normalize(q_scores)
        norm_a_scores = min_max_normalize(a_scores)
        norm_completion_scores = min_max_normalize(completion_scores)
        
        normalized_scores = []
        for i in range(len(raw_scores)):
            normalized_scores.append({
                'q_score': norm_q_scores[i],
                'a_score': norm_a_scores[i],
                'completion': norm_completion_scores[i]
            })
        
        logger.debug(f"Batch normalization: Q({min(q_scores):.1f}-{max(q_scores):.1f}), A({min(a_scores):.1f}-{max(a_scores):.1f})")
        return normalized_scores
    
    def _determine_quality_tier(self, score: float) -> str:
        """Determine quality tier based on final score"""
        if score >= 9.0:
            return 'excellent'
        elif score >= 7.0:
            return 'good'
        elif score >= 5.0:
            return 'fair'
        else:
            return 'poor'
    
    def filter_by_quality(self, qa_pairs: List[Dict[str, Any]], 
                         quality_metrics: List[QualityMetrics]) -> List[Dict[str, Any]]:
        """
        Filter Q&A pairs by quality threshold (TRD Section 4)
        Items below threshold are discarded
        """
        if len(qa_pairs) != len(quality_metrics):
            raise ValueError("Mismatch between Q&A pairs and quality metrics")
        
        filtered_pairs = []
        for qa_pair, quality in zip(qa_pairs, quality_metrics):
            if quality.meets_threshold:
                # Add quality information to the Q&A pair
                qa_pair['quality_metrics'] = {
                    'overall_score': quality.overall_score,
                    'quality_tier': quality.quality_tier,
                    'raw_question_score': quality.score_components.raw_question_score,
                    'raw_answer_score': quality.score_components.raw_answer_score,
                    'completion_bonus': quality.score_components.completion_bonus
                }
                filtered_pairs.append(qa_pair)
        
        logger.info(f"Quality filtering: {len(filtered_pairs)}/{len(qa_pairs)} items passed threshold {self.threshold}")
        return filtered_pairs
    
    def get_batch_statistics(self, quality_metrics: List[QualityMetrics]) -> Dict[str, Any]:
        """Calculate statistics for a batch of quality metrics"""
        if not quality_metrics:
            return {}
        
        scores = [qm.overall_score for qm in quality_metrics]
        
        tier_counts = {}
        for tier in ['excellent', 'good', 'fair', 'poor']:
            tier_counts[tier] = sum(1 for qm in quality_metrics if qm.quality_tier == tier)
        
        return {
            'total_items': len(quality_metrics),
            'average_score': np.mean(scores),
            'median_score': np.median(scores),
            'min_score': np.min(scores),
            'max_score': np.max(scores),
            'std_score': np.std(scores),
            'above_threshold': sum(1 for qm in quality_metrics if qm.meets_threshold),
            'threshold_percentage': (sum(1 for qm in quality_metrics if qm.meets_threshold) / len(quality_metrics)) * 100,
            'tier_distribution': tier_counts
        }

# Import Config here to avoid circular imports
from config import Config