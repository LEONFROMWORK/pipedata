"""
Reddit-specific Quality Scoring Algorithm
Reddit TRD Section 3.4: Reddit 특화 정량적 품질 평가

공식: QualityScore = (w_s * Norm_S_Score) + (w_a * Norm_A_Score) + (w_b * Bonus)
가중치: w_s = 0.3, w_a = 0.4, w_b = 0.3 (보너스 비중이 Stack Overflow보다 높음)
"""
import logging
import math
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import numpy as np

from config import Config
from collectors.reddit_collector import ThreadAnalysisResult

logger = logging.getLogger('pipeline.reddit_scorer')

@dataclass
class RedditQualityComponents:
    """Reddit-specific quality score components"""
    raw_submission_score: float
    raw_answer_score: float
    bonus_score: float
    normalized_submission_score: float
    normalized_answer_score: float
    normalized_bonus_score: float
    final_score: float

@dataclass
class RedditQualityMetrics:
    """Complete Reddit quality assessment"""
    overall_score: float
    score_components: RedditQualityComponents
    meets_threshold: bool
    quality_tier: str
    reddit_specific_features: Dict[str, Any]

class RedditQualityScorer:
    """
    Reddit-specific quality scoring implementation
    
    Key differences from Stack Overflow:
    1. Higher bonus weight (0.3 vs 0.1) - community feedback matters more
    2. OP confirmation gets massive bonus (+10)
    3. Different score calculation: submission.score * upvote_ratio
    4. Higher threshold (6.5 vs 5.0) - Reddit data is noisier
    """
    
    def __init__(self):
        self.config = Config.REDDIT_QUALITY_SCORING
        self.weights = self.config['weights']
        self.bonus_scores = self.config['bonus_scores']
        self.threshold = self.config['threshold']
        
        logger.info(f"RedditQualityScorer initialized with threshold {self.threshold}")
    
    def score_reddit_batch(self, reddit_results: List[ThreadAnalysisResult], 
                          processed_data: List[Dict[str, Any]]) -> List[RedditQualityMetrics]:
        """
        Score a batch of Reddit Q&A pairs with Reddit-specific normalization
        
        Args:
            reddit_results: Raw thread analysis results
            processed_data: Processed content data (text/image processing results)
            
        Returns:
            List of RedditQualityMetrics for each pair
        """
        if not reddit_results:
            return []
        
        logger.info(f"Scoring Reddit batch of {len(reddit_results)} Q&A pairs")
        
        # Step 1: Calculate raw scores
        raw_scores = []
        for i, result in enumerate(reddit_results):
            processed_item = processed_data[i] if i < len(processed_data) else {}
            
            submission_score = self._calculate_submission_score(result.submission)
            answer_score = self._calculate_answer_score(result.solution)
            bonus_score = self._calculate_reddit_bonus(result, processed_item)
            
            raw_scores.append({
                's_score': submission_score,
                'a_score': answer_score,
                'bonus': bonus_score
            })
        
        # Step 2: Normalize scores within batch
        normalized_scores = self._normalize_reddit_scores(raw_scores)
        
        # Step 3: Calculate final scores
        quality_metrics = []
        for i, result in enumerate(reddit_results):
            raw = raw_scores[i]
            normalized = normalized_scores[i]
            
            # Apply Reddit-specific weighted formula
            final_score = (
                self.weights['w_s'] * normalized['s_score'] +
                self.weights['w_a'] * normalized['a_score'] +
                self.weights['w_b'] * normalized['bonus']
            )
            
            # Scale to 0-10 range
            final_score *= 10
            
            components = RedditQualityComponents(
                raw_submission_score=raw['s_score'],
                raw_answer_score=raw['a_score'],
                bonus_score=raw['bonus'],
                normalized_submission_score=normalized['s_score'],
                normalized_answer_score=normalized['a_score'],
                normalized_bonus_score=normalized['bonus'],
                final_score=final_score
            )
            
            # Extract Reddit-specific features
            reddit_features = self._extract_reddit_features(result)
            
            quality_metrics.append(RedditQualityMetrics(
                overall_score=final_score,
                score_components=components,
                meets_threshold=final_score >= self.threshold,
                quality_tier=self._determine_reddit_quality_tier(final_score),
                reddit_specific_features=reddit_features
            ))
        
        passed_count = sum(1 for qm in quality_metrics if qm.meets_threshold)
        logger.info(f"Reddit batch scoring complete: {passed_count} items above threshold {self.threshold}")
        
        return quality_metrics
    
    def _calculate_submission_score(self, submission_data: Dict[str, Any]) -> float:
        """
        Calculate Reddit submission score
        Formula: submission.score * submission.upvote_ratio
        """
        score = submission_data.get('score', 0)
        upvote_ratio = submission_data.get('upvote_ratio', 0.5)
        
        # Reddit TRD formula
        submission_score = score * upvote_ratio
        
        logger.debug(f"Submission score: {score} * {upvote_ratio} = {submission_score}")
        return submission_score
    
    def _calculate_answer_score(self, solution_data: Dict[str, Any]) -> float:
        """
        Calculate Reddit comment/solution score
        Simple comment score (no complex formula like SO)
        """
        score = solution_data.get('score', 0)
        
        logger.debug(f"Answer score: {score}")
        return float(score)
    
    def _calculate_reddit_bonus(self, result: ThreadAnalysisResult, 
                               processed_data: Dict[str, Any]) -> float:
        """
        Calculate Reddit-specific bonus score
        Much higher bonuses than Stack Overflow due to community nature
        """
        bonus = 0.0
        
        # OP confirmation bonus (MASSIVE - this is gold in Reddit)
        if result.metadata.get('op_confirmed', False):
            bonus += self.bonus_scores['op_confirmed']  # +10
            logger.debug("OP confirmation bonus applied (+10)")
        
        # Solved flair bonus
        flair = result.submission.get('link_flair_text', '')
        if flair and 'solved' in flair.lower():
            bonus += self.bonus_scores['solved_flair']  # +5
            logger.debug("Solved flair bonus applied (+5)")
        
        # Code blocks bonus
        text_result = processed_data.get('text_processing', {})
        if text_result.get('has_code_blocks', False):
            bonus += self.bonus_scores['code_blocks']  # +3
            logger.debug("Code blocks bonus applied (+3)")
        
        # Image context bonus
        image_result = processed_data.get('image_processing', {})
        if image_result.get('success', False):
            bonus += self.bonus_scores['image_context']  # +2
            logger.debug("Image context bonus applied (+2)")
        
        logger.debug(f"Total Reddit bonus: {bonus}")
        return bonus
    
    def _normalize_reddit_scores(self, raw_scores: List[Dict[str, float]]) -> List[Dict[str, float]]:
        """
        Apply Min-Max normalization for Reddit scores
        """
        if len(raw_scores) <= 1:
            return [{'s_score': 1.0, 'a_score': 1.0, 'bonus': 1.0} for _ in raw_scores]
        
        # Extract score arrays
        s_scores = [s['s_score'] for s in raw_scores]
        a_scores = [s['a_score'] for s in raw_scores]
        bonus_scores = [s['bonus'] for s in raw_scores]
        
        # Min-Max normalization
        def min_max_normalize(scores):
            min_score = min(scores)
            max_score = max(scores)
            if max_score == min_score:
                return [1.0] * len(scores)
            return [(s - min_score) / (max_score - min_score) for s in scores]
        
        norm_s_scores = min_max_normalize(s_scores)
        norm_a_scores = min_max_normalize(a_scores)
        norm_bonus_scores = min_max_normalize(bonus_scores)
        
        normalized_scores = []
        for i in range(len(raw_scores)):
            normalized_scores.append({
                's_score': norm_s_scores[i],
                'a_score': norm_a_scores[i],
                'bonus': norm_bonus_scores[i]
            })
        
        logger.debug(f"Reddit batch normalization: S({min(s_scores):.1f}-{max(s_scores):.1f}), "
                    f"A({min(a_scores):.1f}-{max(a_scores):.1f}), "
                    f"B({min(bonus_scores):.1f}-{max(bonus_scores):.1f})")
        
        return normalized_scores
    
    def _determine_reddit_quality_tier(self, score: float) -> str:
        """Determine quality tier for Reddit content"""
        if score >= 9.0:
            return 'excellent'
        elif score >= 7.5:  # Slightly higher than SO due to higher threshold
            return 'good'
        elif score >= 6.5:  # Matches threshold
            return 'fair'
        else:
            return 'poor'
    
    def _extract_reddit_features(self, result: ThreadAnalysisResult) -> Dict[str, Any]:
        """Extract Reddit-specific features for analysis"""
        return {
            'solution_type': result.metadata.get('solution_type'),
            'op_confirmed': result.metadata.get('op_confirmed', False),
            'total_comments': result.metadata.get('total_comments', 0),
            'submission_upvote_ratio': result.submission.get('upvote_ratio', 0),
            'submission_flair': result.submission.get('link_flair_text'),
            'solution_score': result.solution.get('score', 0),
            'solution_is_root': result.solution.get('is_root', False),
            'author_same_as_op': result.solution.get('author') == result.submission.get('author')
        }
    
    def filter_reddit_by_quality(self, reddit_results: List[ThreadAnalysisResult],
                                processed_data: List[Dict[str, Any]],
                                quality_metrics: List[RedditQualityMetrics]) -> List[Dict[str, Any]]:
        """
        Filter Reddit Q&A pairs by quality threshold and convert to standard format
        """
        if len(reddit_results) != len(quality_metrics):
            raise ValueError("Mismatch between Reddit results and quality metrics")
        
        filtered_pairs = []
        for i, (result, quality) in enumerate(zip(reddit_results, quality_metrics)):
            if quality.meets_threshold:
                # Convert to standard Q&A format compatible with existing pipeline
                processed_item = processed_data[i] if i < len(processed_data) else {}
                
                qa_pair = {
                    'source': 'reddit',
                    'question': self._format_reddit_question(result.submission),
                    'answer': self._format_reddit_answer(result.solution),
                    'quality_metrics': {
                        'overall_score': quality.overall_score,
                        'quality_tier': quality.quality_tier,
                        'submission_score': quality.score_components.raw_submission_score,
                        'answer_score': quality.score_components.raw_answer_score,
                        'bonus_score': quality.score_components.bonus_score,
                        'reddit_features': quality.reddit_specific_features
                    },
                    'text_processing': processed_item.get('text_processing', {}),
                    'image_processing': processed_item.get('image_processing', {}),
                    'triage_result': processed_item.get('triage_result', {}),
                    'metadata': {
                        'source_url': f"https://reddit.com{result.submission['permalink']}",
                        'submission_id': result.submission['id'],
                        'solution_comment_id': result.solution['id'],
                        'collection_timestamp': datetime.now().isoformat()
                    }
                }
                
                filtered_pairs.append(qa_pair)
        
        logger.info(f"Reddit quality filtering: {len(filtered_pairs)}/{len(reddit_results)} pairs passed")
        return filtered_pairs
    
    def _format_reddit_question(self, submission_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format Reddit submission as question"""
        return {
            'title': submission_data.get('title', ''),
            'body': submission_data.get('selftext', ''),
            'body_markdown': submission_data.get('selftext', ''),
            'score': submission_data.get('score', 0),
            'view_count': submission_data.get('num_comments', 0),  # Use comment count as proxy
            'tags': self._extract_reddit_tags(submission_data),
            'owner': {
                'name': submission_data.get('author', 'unknown'),
                'reputation': submission_data.get('score', 0)  # Use submission score as reputation
            }
        }
    
    def _format_reddit_answer(self, solution_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format Reddit comment as answer"""
        return {
            'body': solution_data.get('body', ''),
            'body_markdown': solution_data.get('body', ''),
            'score': solution_data.get('score', 0),
            'is_accepted': True,  # All selected solutions are considered "accepted"
            'owner': {
                'name': solution_data.get('author', 'unknown'),
                'reputation': solution_data.get('score', 0)
            }
        }
    
    def _extract_reddit_tags(self, submission_data: Dict[str, Any]) -> List[str]:
        """Extract tags from Reddit submission"""
        tags = ['excel']  # Base tag
        
        # Add flair as tag
        flair = submission_data.get('link_flair_text')
        if flair:
            tags.append(flair.lower().replace(' ', '-'))
        
        # Extract tags from title (simple keyword matching)
        title_lower = submission_data.get('title', '').lower()
        
        excel_keywords = {
            'vlookup': 'vlookup',
            'pivot': 'pivot-table',
            'macro': 'vba',
            'formula': 'formula',
            'chart': 'chart',
            'conditional formatting': 'conditional-formatting',
            'sum': 'sum-function',
            'count': 'count-function'
        }
        
        for keyword, tag in excel_keywords.items():
            if keyword in title_lower:
                tags.append(tag)
        
        return list(set(tags))  # Remove duplicates
    
    def get_reddit_batch_statistics(self, quality_metrics: List[RedditQualityMetrics]) -> Dict[str, Any]:
        """Calculate statistics for Reddit batch"""
        if not quality_metrics:
            return {}
        
        scores = [qm.overall_score for qm in quality_metrics]
        
        # Reddit-specific statistics
        op_confirmed_count = sum(1 for qm in quality_metrics 
                                if qm.reddit_specific_features.get('op_confirmed', False))
        
        solution_types = {}
        for qm in quality_metrics:
            solution_type = qm.reddit_specific_features.get('solution_type', 'unknown')
            solution_types[solution_type] = solution_types.get(solution_type, 0) + 1
        
        return {
            'total_items': len(quality_metrics),
            'average_score': np.mean(scores),
            'median_score': np.median(scores),
            'min_score': np.min(scores),
            'max_score': np.max(scores),
            'above_threshold': sum(1 for qm in quality_metrics if qm.meets_threshold),
            'threshold_percentage': (sum(1 for qm in quality_metrics if qm.meets_threshold) / len(quality_metrics)) * 100,
            'reddit_specific': {
                'op_confirmed_count': op_confirmed_count,
                'op_confirmed_percentage': (op_confirmed_count / len(quality_metrics)) * 100,
                'solution_type_distribution': solution_types,
                'average_submission_score': np.mean([qm.score_components.raw_submission_score for qm in quality_metrics]),
                'average_answer_score': np.mean([qm.score_components.raw_answer_score for qm in quality_metrics])
            }
        }

# Import here to avoid circular imports
from datetime import datetime