"""
Layer 2: Behavioral Bot Detection System
Advanced behavioral analysis with CQS patterns, timing analysis, and behavioral fingerprinting

This module implements sophisticated bot detection based on user behavior patterns,
posting frequency, content similarity, and Reddit's Contributor Quality Score (CQS) metrics.
"""

import re
import logging
import time
import statistics
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict, Counter
import hashlib
import json

logger = logging.getLogger('pipeline.behavioral_bot_detector')

class BehavioralBotType(Enum):
    """Behavioral bot classification types"""
    SCHEDULED_BOT = "scheduled_bot"
    SPAM_FARM_BOT = "spam_farm_bot"
    CONTENT_SCRAPER_BOT = "content_scraper_bot"
    ENGAGEMENT_BOT = "engagement_bot"
    SOPHISTICATED_AI_BOT = "sophisticated_ai_bot"
    HUMAN = "human"

@dataclass
class BehavioralMetrics:
    """Comprehensive behavioral metrics for bot detection"""
    posting_frequency_score: float
    content_similarity_score: float
    timing_consistency_score: float
    response_pattern_score: float
    engagement_pattern_score: float
    language_complexity_score: float
    cqs_score: float
    account_age_score: float

@dataclass
class BehavioralBotResult:
    """Behavioral bot detection result with detailed analysis"""
    is_bot: bool
    confidence: float
    bot_type: BehavioralBotType
    behavioral_metrics: BehavioralMetrics
    indicators: List[str]
    risk_factors: List[str]
    detection_timestamp: str

class BehavioralBotDetector:
    """
    Advanced behavioral bot detection system implementing Layer 2 detection
    
    Features:
    - CQS (Contributor Quality Score) analysis
    - Temporal pattern analysis
    - Content similarity detection
    - Behavioral fingerprinting
    - Account maturity assessment
    """
    
    def __init__(self):
        self.user_history_cache = {}  # Cache for user behavioral history
        self.temporal_patterns = {}   # Cache for temporal analysis
        self.content_fingerprints = {}  # Cache for content similarity
        self.setup_behavioral_patterns()
        
    def setup_behavioral_patterns(self):
        """Initialize behavioral analysis patterns"""
        self.suspicious_patterns = {
            'rapid_posting': {
                'threshold': 10,  # posts per hour
                'weight': 0.8
            },
            'content_repetition': {
                'threshold': 0.85,  # similarity threshold
                'weight': 0.9
            },
            'timing_regularity': {
                'threshold': 0.95,  # consistency threshold
                'weight': 0.7
            },
            'low_engagement': {
                'threshold': 0.1,  # engagement ratio
                'weight': 0.6
            }
        }
        
        # CQS mapping based on Reddit's contributor quality system
        self.cqs_risk_mapping = {
            'lowest': 0.95,    # 95% bot probability
            'low': 0.75,       # 75% bot probability
            'medium': 0.4,     # 40% bot probability
            'high': 0.1,       # 10% bot probability
            'highest': 0.02    # 2% bot probability
        }
        
        # Language complexity indicators
        self.complexity_indicators = {
            'avg_word_length': (4.5, 6.5),    # Normal range
            'sentence_variety': (8, 25),      # Normal sentence length variety
            'vocabulary_richness': (0.6, 0.85),  # Unique words ratio
            'punctuation_variety': (3, 8)     # Different punctuation marks
        }
        
    def analyze_user_behavior(self, user_data: Dict[str, Any], 
                            user_comments: List[Dict[str, Any]]) -> BehavioralBotResult:
        """
        Comprehensive behavioral analysis of a user
        
        Args:
            user_data: User account information
            user_comments: List of user's recent comments/posts
            
        Returns:
            BehavioralBotResult with detailed behavioral analysis
        """
        indicators = []
        risk_factors = []
        
        # 1. CQS Analysis
        cqs_score = self._analyze_cqs_patterns(user_data)
        if cqs_score > 0.7:
            indicators.append(f"High CQS risk score: {cqs_score:.2f}")
            risk_factors.append("Low contributor quality score")
        
        # 2. Temporal Pattern Analysis
        timing_score = self._analyze_temporal_patterns(user_comments)
        if timing_score > 0.8:
            indicators.append(f"Suspicious timing patterns: {timing_score:.2f}")
            risk_factors.append("Highly regular posting schedule")
        
        # 3. Content Similarity Analysis
        similarity_score = self._analyze_content_similarity(user_comments)
        if similarity_score > 0.8:
            indicators.append(f"High content similarity: {similarity_score:.2f}")
            risk_factors.append("Repetitive content patterns")
        
        # 4. Posting Frequency Analysis
        frequency_score = self._analyze_posting_frequency(user_comments)
        if frequency_score > 0.7:
            indicators.append(f"Suspicious posting frequency: {frequency_score:.2f}")
            risk_factors.append("Abnormal posting rate")
        
        # 5. Response Pattern Analysis
        response_score = self._analyze_response_patterns(user_comments)
        if response_score > 0.7:
            indicators.append(f"Bot-like response patterns: {response_score:.2f}")
            risk_factors.append("Automated response characteristics")
        
        # 6. Engagement Pattern Analysis
        engagement_score = self._analyze_engagement_patterns(user_comments)
        if engagement_score > 0.7:
            indicators.append(f"Suspicious engagement patterns: {engagement_score:.2f}")
            risk_factors.append("Unusual engagement behavior")
        
        # 7. Language Complexity Analysis
        complexity_score = self._analyze_language_complexity(user_comments)
        if complexity_score > 0.7:
            indicators.append(f"Artificial language patterns: {complexity_score:.2f}")
            risk_factors.append("Non-natural language characteristics")
        
        # 8. Account Age Analysis
        age_score = self._analyze_account_age(user_data)
        if age_score > 0.6:
            indicators.append(f"Suspicious account age patterns: {age_score:.2f}")
            risk_factors.append("Account age inconsistencies")
        
        # Create behavioral metrics
        behavioral_metrics = BehavioralMetrics(
            posting_frequency_score=frequency_score,
            content_similarity_score=similarity_score,
            timing_consistency_score=timing_score,
            response_pattern_score=response_score,
            engagement_pattern_score=engagement_score,
            language_complexity_score=complexity_score,
            cqs_score=cqs_score,
            account_age_score=age_score
        )
        
        # Calculate overall confidence
        metric_scores = [
            cqs_score * 0.20,          # CQS is most important
            timing_score * 0.18,       # Temporal patterns
            similarity_score * 0.15,   # Content similarity
            frequency_score * 0.12,    # Posting frequency
            response_score * 0.12,     # Response patterns
            engagement_score * 0.10,   # Engagement patterns
            complexity_score * 0.08,   # Language complexity
            age_score * 0.05          # Account age
        ]
        
        overall_confidence = sum(metric_scores)
        is_bot = overall_confidence >= 0.7
        
        # Determine bot type
        bot_type = self._classify_behavioral_bot_type(behavioral_metrics, indicators)
        
        result = BehavioralBotResult(
            is_bot=is_bot,
            confidence=overall_confidence,
            bot_type=bot_type,
            behavioral_metrics=behavioral_metrics,
            indicators=indicators,
            risk_factors=risk_factors,
            detection_timestamp=datetime.now().isoformat()
        )
        
        self._log_behavioral_analysis(result, user_data.get('username', 'unknown'))
        return result
    
    def _analyze_cqs_patterns(self, user_data: Dict[str, Any]) -> float:
        """Analyze Reddit's Contributor Quality Score patterns"""
        cqs = user_data.get('contributor_quality_score', 'medium')
        
        # Map CQS to risk score
        risk_score = self.cqs_risk_mapping.get(cqs, 0.5)
        
        # Additional CQS-related analysis
        karma_ratio = user_data.get('comment_karma', 1) / max(user_data.get('link_karma', 1), 1)
        
        # Suspicious patterns
        if karma_ratio > 100:  # Only comments, no posts
            risk_score += 0.2
        elif karma_ratio < 0.01:  # Only posts, no comments
            risk_score += 0.3
        
        # Account age vs karma inconsistencies
        account_age_days = user_data.get('account_age_days', 1)
        total_karma = user_data.get('comment_karma', 0) + user_data.get('link_karma', 0)
        
        if account_age_days > 30 and total_karma < 10:
            risk_score += 0.2
        elif account_age_days < 7 and total_karma > 1000:
            risk_score += 0.3
        
        return min(risk_score, 1.0)
    
    def _analyze_temporal_patterns(self, comments: List[Dict[str, Any]]) -> float:
        """Analyze temporal posting patterns for bot behavior"""
        if len(comments) < 3:
            return 0.0
        
        # Extract timestamps
        timestamps = []
        for comment in comments:
            timestamp = comment.get('created_utc', 0)
            if timestamp:
                timestamps.append(timestamp)
        
        if len(timestamps) < 3:
            return 0.0
        
        timestamps.sort()
        
        # Calculate intervals between posts
        intervals = []
        for i in range(1, len(timestamps)):
            interval = timestamps[i] - timestamps[i-1]
            intervals.append(interval)
        
        # Analyze consistency
        if len(intervals) < 2:
            return 0.0
        
        # Calculate coefficient of variation
        mean_interval = statistics.mean(intervals)
        std_interval = statistics.stdev(intervals) if len(intervals) > 1 else 0
        
        if mean_interval == 0:
            return 1.0  # Suspicious: all posts at same time
        
        coefficient_of_variation = std_interval / mean_interval
        
        # Low variation = high bot probability
        if coefficient_of_variation < 0.1:
            return 0.9
        elif coefficient_of_variation < 0.3:
            return 0.7
        elif coefficient_of_variation < 0.5:
            return 0.4
        else:
            return 0.1
    
    def _analyze_content_similarity(self, comments: List[Dict[str, Any]]) -> float:
        """Analyze content similarity for repetitive patterns"""
        if len(comments) < 2:
            return 0.0
        
        # Extract comment texts
        texts = []
        for comment in comments:
            text = comment.get('body', '') or comment.get('text', '')
            if text and len(text.strip()) > 10:
                texts.append(text.strip().lower())
        
        if len(texts) < 2:
            return 0.0
        
        # Calculate pairwise similarity
        similarities = []
        for i in range(len(texts)):
            for j in range(i+1, len(texts)):
                similarity = self._calculate_text_similarity(texts[i], texts[j])
                similarities.append(similarity)
        
        if not similarities:
            return 0.0
        
        # High average similarity indicates bot behavior
        avg_similarity = statistics.mean(similarities)
        max_similarity = max(similarities)
        
        # Weighted score
        return (avg_similarity * 0.6) + (max_similarity * 0.4)
    
    def _calculate_text_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity between two texts using character n-grams"""
        if not text1 or not text2:
            return 0.0
        
        # Create character trigrams
        def get_trigrams(text):
            text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
            trigrams = set()
            for i in range(len(text) - 2):
                trigrams.add(text[i:i+3])
            return trigrams
        
        trigrams1 = get_trigrams(text1)
        trigrams2 = get_trigrams(text2)
        
        if not trigrams1 or not trigrams2:
            return 0.0
        
        # Jaccard similarity
        intersection = len(trigrams1.intersection(trigrams2))
        union = len(trigrams1.union(trigrams2))
        
        return intersection / union if union > 0 else 0.0
    
    def _analyze_posting_frequency(self, comments: List[Dict[str, Any]]) -> float:
        """Analyze posting frequency patterns"""
        if len(comments) < 2:
            return 0.0
        
        # Group comments by hour
        hourly_counts = defaultdict(int)
        
        for comment in comments:
            timestamp = comment.get('created_utc', 0)
            if timestamp:
                dt = datetime.fromtimestamp(timestamp)
                hour_key = dt.strftime('%Y-%m-%d-%H')
                hourly_counts[hour_key] += 1
        
        if not hourly_counts:
            return 0.0
        
        # Calculate posting rate
        counts = list(hourly_counts.values())
        max_hourly = max(counts)
        avg_hourly = statistics.mean(counts)
        
        # Suspicious patterns
        if max_hourly > 20:  # More than 20 posts per hour
            return 0.9
        elif max_hourly > 10:  # More than 10 posts per hour
            return 0.7
        elif avg_hourly > 5:  # Average more than 5 posts per hour
            return 0.6
        else:
            return 0.1
    
    def _analyze_response_patterns(self, comments: List[Dict[str, Any]]) -> float:
        """Analyze response patterns for bot-like characteristics"""
        if len(comments) < 3:
            return 0.0
        
        # Extract response characteristics
        response_lengths = []
        response_times = []
        
        for comment in comments:
            text = comment.get('body', '') or comment.get('text', '')
            if text:
                response_lengths.append(len(text))
                # Mock response time (in real implementation, would calculate from parent)
                response_times.append(comment.get('response_time', 300))  # 5 minutes default
        
        if not response_lengths:
            return 0.0
        
        # Analyze length consistency
        length_cv = statistics.stdev(response_lengths) / statistics.mean(response_lengths)
        
        # Analyze response time consistency
        if len(response_times) > 1:
            time_cv = statistics.stdev(response_times) / statistics.mean(response_times)
        else:
            time_cv = 1.0
        
        # Low variation in both = high bot probability
        bot_score = 0.0
        
        if length_cv < 0.2:  # Very consistent length
            bot_score += 0.4
        elif length_cv < 0.5:  # Somewhat consistent
            bot_score += 0.2
        
        if time_cv < 0.1:  # Very consistent timing
            bot_score += 0.5
        elif time_cv < 0.3:  # Somewhat consistent
            bot_score += 0.3
        
        return min(bot_score, 1.0)
    
    def _analyze_engagement_patterns(self, comments: List[Dict[str, Any]]) -> float:
        """Analyze engagement patterns for suspicious behavior"""
        if len(comments) < 2:
            return 0.0
        
        # Calculate engagement metrics
        total_score = 0
        total_comments = len(comments)
        upvote_ratios = []
        
        for comment in comments:
            score = comment.get('score', 0)
            total_score += score
            
            # Mock upvote ratio (in real implementation, would be from Reddit API)
            upvote_ratio = comment.get('upvote_ratio', 0.8)
            upvote_ratios.append(upvote_ratio)
        
        if total_comments == 0:
            return 0.0
        
        avg_score = total_score / total_comments
        avg_upvote_ratio = statistics.mean(upvote_ratios) if upvote_ratios else 0.5
        
        # Suspicious engagement patterns
        bot_score = 0.0
        
        # Always low scores
        if avg_score < 1 and total_comments > 5:
            bot_score += 0.3
        
        # Consistently low upvote ratios
        if avg_upvote_ratio < 0.6:
            bot_score += 0.4
        
        # No variation in engagement
        if len(upvote_ratios) > 1:
            ratio_std = statistics.stdev(upvote_ratios)
            if ratio_std < 0.1:
                bot_score += 0.3
        
        return min(bot_score, 1.0)
    
    def _analyze_language_complexity(self, comments: List[Dict[str, Any]]) -> float:
        """Analyze language complexity for artificial patterns"""
        if len(comments) < 2:
            return 0.0
        
        # Extract texts
        texts = []
        for comment in comments:
            text = comment.get('body', '') or comment.get('text', '')
            if text and len(text.strip()) > 10:
                texts.append(text.strip())
        
        if not texts:
            return 0.0
        
        # Calculate complexity metrics
        total_words = 0
        total_chars = 0
        sentences = []
        vocabulary = set()
        punctuation_types = set()
        
        for text in texts:
            words = text.split()
            total_words += len(words)
            total_chars += len(text)
            
            # Add to vocabulary
            for word in words:
                clean_word = re.sub(r'[^\w]', '', word.lower())
                if clean_word:
                    vocabulary.add(clean_word)
            
            # Count sentences
            text_sentences = re.split(r'[.!?]+', text)
            for sentence in text_sentences:
                if sentence.strip():
                    sentences.append(len(sentence.strip().split()))
            
            # Count punctuation types
            for char in text:
                if char in '.,!?;:()[]{}"-':
                    punctuation_types.add(char)
        
        if total_words == 0:
            return 0.0
        
        # Calculate metrics
        avg_word_length = total_chars / total_words
        vocabulary_richness = len(vocabulary) / total_words
        punctuation_variety = len(punctuation_types)
        
        if sentences:
            sentence_variety = statistics.stdev(sentences) if len(sentences) > 1 else 0
        else:
            sentence_variety = 0
        
        # Check against normal ranges
        bot_score = 0.0
        
        # Average word length
        if avg_word_length < 3.5 or avg_word_length > 7.5:
            bot_score += 0.2
        
        # Vocabulary richness
        if vocabulary_richness < 0.4 or vocabulary_richness > 0.9:
            bot_score += 0.3
        
        # Punctuation variety
        if punctuation_variety < 2 or punctuation_variety > 10:
            bot_score += 0.2
        
        # Sentence variety
        if sentence_variety < 2 or sentence_variety > 30:
            bot_score += 0.3
        
        return min(bot_score, 1.0)
    
    def _analyze_account_age(self, user_data: Dict[str, Any]) -> float:
        """Analyze account age patterns"""
        account_age_days = user_data.get('account_age_days', 0)
        
        if account_age_days < 1:
            return 0.8  # Very new account
        elif account_age_days < 7:
            return 0.6  # Less than a week
        elif account_age_days < 30:
            return 0.3  # Less than a month
        else:
            return 0.1  # Established account
    
    def _classify_behavioral_bot_type(self, metrics: BehavioralMetrics, 
                                    indicators: List[str]) -> BehavioralBotType:
        """Classify the type of behavioral bot"""
        # Analyze indicators to determine bot type
        indicator_text = ' '.join(indicators).lower()
        
        if metrics.timing_consistency_score > 0.8:
            return BehavioralBotType.SCHEDULED_BOT
        elif metrics.content_similarity_score > 0.8:
            return BehavioralBotType.CONTENT_SCRAPER_BOT
        elif metrics.posting_frequency_score > 0.8:
            return BehavioralBotType.SPAM_FARM_BOT
        elif metrics.engagement_pattern_score > 0.7:
            return BehavioralBotType.ENGAGEMENT_BOT
        elif metrics.language_complexity_score > 0.7:
            return BehavioralBotType.SOPHISTICATED_AI_BOT
        else:
            return BehavioralBotType.HUMAN
    
    def _log_behavioral_analysis(self, result: BehavioralBotResult, username: str):
        """Log behavioral analysis results"""
        if result.is_bot:
            logger.warning(
                f"🤖 Behavioral bot detected: {username} "
                f"(type: {result.bot_type.value}, confidence: {result.confidence:.2f})"
            )
            for indicator in result.indicators:
                logger.info(f"   📊 {indicator}")
        else:
            logger.debug(
                f"✅ Human behavior detected: {username} "
                f"(confidence: {1-result.confidence:.2f})"
            )
    
    def get_behavioral_stats(self) -> Dict[str, Any]:
        """Get behavioral detection statistics"""
        return {
            'cached_users': len(self.user_history_cache),
            'temporal_patterns': len(self.temporal_patterns),
            'content_fingerprints': len(self.content_fingerprints),
            'cqs_mappings': len(self.cqs_risk_mapping),
            'complexity_indicators': len(self.complexity_indicators),
            'version': '2.0-behavioral'
        }

# Global instance for integration
behavioral_detector = BehavioralBotDetector()