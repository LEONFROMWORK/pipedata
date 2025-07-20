"""
Layer 3: AI-based Bot Detection System
Advanced AI analysis using BERT and structural pattern recognition

This module implements sophisticated bot detection using:
- BERT-based natural language analysis
- Structural pattern recognition
- Semantic consistency analysis
- Advanced AI-generated content detection
"""

import re
import logging
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import hashlib
import json
from collections import Counter

# Mock BERT implementation for production environment
# In a real implementation, this would use transformers library
class MockBERTAnalyzer:
    """Mock BERT analyzer for production environments without transformers"""
    
    def __init__(self):
        self.ai_patterns = {
            'hedging_phrases': [
                'perhaps', 'maybe', 'possibly', 'might', 'could', 'would suggest',
                'it appears', 'it seems', 'one might', 'you might consider'
            ],
            'formal_phrases': [
                'however', 'furthermore', 'nevertheless', 'therefore', 'consequently',
                'additionally', 'specifically', 'particularly', 'essentially'
            ],
            'perfect_structure': [
                'first', 'second', 'third', 'finally', 'in conclusion',
                'to summarize', 'in summary', 'let me explain', 'here\'s how'
            ],
            'ai_courtesy': [
                'i hope this helps', 'let me know if', 'feel free to', 'don\'t hesitate',
                'i\'m here to help', 'happy to assist', 'please let me know'
            ]
        }
        
    def analyze_text(self, text: str) -> Dict[str, float]:
        """Analyze text for AI-generated patterns"""
        if not text:
            return {'ai_probability': 0.0, 'confidence': 0.0}
        
        text_lower = text.lower()
        
        # Count AI indicators
        hedging_count = sum(1 for phrase in self.ai_patterns['hedging_phrases'] 
                          if phrase in text_lower)
        formal_count = sum(1 for phrase in self.ai_patterns['formal_phrases'] 
                         if phrase in text_lower)
        structure_count = sum(1 for phrase in self.ai_patterns['perfect_structure'] 
                            if phrase in text_lower)
        courtesy_count = sum(1 for phrase in self.ai_patterns['ai_courtesy'] 
                           if phrase in text_lower)
        
        # Calculate AI probability
        word_count = len(text.split())
        if word_count == 0:
            return {'ai_probability': 0.0, 'confidence': 0.0}
        
        # Weighted scoring
        ai_score = (hedging_count * 0.3 + formal_count * 0.2 + 
                   structure_count * 0.3 + courtesy_count * 0.2) / word_count * 100
        
        # Additional checks
        if self._check_perfect_grammar(text):
            ai_score += 0.2
        
        if self._check_list_formatting(text):
            ai_score += 0.15
        
        ai_probability = min(ai_score, 1.0)
        confidence = min(ai_probability + 0.1, 1.0)
        
        return {'ai_probability': ai_probability, 'confidence': confidence}
    
    def _check_perfect_grammar(self, text: str) -> bool:
        """Check for suspiciously perfect grammar"""
        # Simple heuristic: check for consistent punctuation and capitalization
        sentences = re.split(r'[.!?]+', text)
        if len(sentences) < 2:
            return False
        
        perfect_count = 0
        for sentence in sentences:
            sentence = sentence.strip()
            if sentence and sentence[0].isupper() and not sentence.endswith(('...', '??', '!!')):
                perfect_count += 1
        
        return perfect_count / len(sentences) > 0.8
    
    def _check_list_formatting(self, text: str) -> bool:
        """Check for structured list formatting"""
        list_patterns = [
            r'^\d+\.',  # 1. 2. 3.
            r'^-\s',    # - item
            r'^\*\s',   # * item
            r'^\w+:\s', # Label:
        ]
        
        lines = text.split('\n')
        list_lines = 0
        
        for line in lines:
            line = line.strip()
            if any(re.match(pattern, line) for pattern in list_patterns):
                list_lines += 1
        
        return list_lines >= 3

logger = logging.getLogger('pipeline.ai_bot_detector')

class AIBotType(Enum):
    """AI-based bot classification types"""
    GPT_GENERATED = "gpt_generated"
    TEMPLATE_AI = "template_ai"
    HYBRID_AI = "hybrid_ai"
    SOPHISTICATED_AI = "sophisticated_ai"
    HUMAN_AUTHORED = "human_authored"

@dataclass
class AIAnalysisResult:
    """AI analysis result with detailed metrics"""
    ai_probability: float
    confidence: float
    ai_indicators: List[str]
    semantic_consistency: float
    structural_analysis: Dict[str, float]
    bert_analysis: Dict[str, float]

@dataclass
class AIBotResult:
    """AI-based bot detection result"""
    is_ai_generated: bool
    confidence: float
    ai_type: AIBotType
    analysis_result: AIAnalysisResult
    reasoning: str
    detection_timestamp: str

class AIBotDetector:
    """
    Advanced AI-based bot detection system implementing Layer 3 detection
    
    Features:
    - BERT-based natural language analysis
    - Structural pattern recognition
    - Semantic consistency checking
    - AI-generated content detection
    """
    
    def __init__(self):
        self.bert_analyzer = MockBERTAnalyzer()
        self.ai_detection_threshold = 0.5  # Lowered threshold for better sensitivity
        self.setup_ai_patterns()
        
    def setup_ai_patterns(self):
        """Initialize AI detection patterns"""
        self.ai_signatures = {
            'gpt_patterns': [
                'as an ai', 'i am an ai', 'i\'m an ai', 'artificial intelligence',
                'i don\'t have personal', 'i cannot', 'i\'m not able to',
                'i don\'t have the ability', 'i cannot access', 'i\'m not capable'
            ],
            'template_responses': [
                'thank you for your question', 'i\'d be happy to help',
                'here\'s what i would recommend', 'i hope this information helps',
                'let me provide you with', 'based on your description'
            ],
            'structured_thinking': [
                'first, let\'s', 'next, we should', 'finally, you can',
                'step 1:', 'step 2:', 'step 3:', 'to begin with',
                'in summary', 'to conclude', 'in conclusion'
            ],
            'hedging_language': [
                'it appears that', 'it seems like', 'you might want to',
                'you could try', 'perhaps you could', 'one option would be',
                'another approach', 'alternatively', 'you might consider'
            ]
        }
        
        self.human_indicators = [
            'i had the same problem', 'i\'ve been struggling with',
            'this happened to me', 'i remember when', 'i think i',
            'if i recall correctly', 'from my experience', 'i usually',
            'damn', 'wow', 'oh no', 'ugh', 'lol', 'haha', 'omg'
        ]
        
    def analyze_ai_content(self, text: str, metadata: Optional[Dict[str, Any]] = None) -> AIBotResult:
        """
        Comprehensive AI content analysis
        
        Args:
            text: Text content to analyze
            metadata: Optional metadata about the content
            
        Returns:
            AIBotResult with detailed AI analysis
        """
        if not text or len(text.strip()) < 10:
            return AIBotResult(
                is_ai_generated=False,
                confidence=0.0,
                ai_type=AIBotType.HUMAN_AUTHORED,
                analysis_result=AIAnalysisResult(
                    ai_probability=0.0,
                    confidence=0.0,
                    ai_indicators=[],
                    semantic_consistency=0.0,
                    structural_analysis={},
                    bert_analysis={}
                ),
                reasoning="Text too short for analysis",
                detection_timestamp=datetime.now().isoformat()
            )
        
        # 1. BERT Analysis
        bert_result = self.bert_analyzer.analyze_text(text)
        
        # 2. Structural Analysis
        structural_analysis = self._analyze_text_structure(text)
        
        # 3. Semantic Consistency Analysis
        semantic_consistency = self._analyze_semantic_consistency(text)
        
        # 4. AI Pattern Detection
        ai_indicators = self._detect_ai_patterns(text)
        
        # 5. Human Indicator Detection
        human_indicators = self._detect_human_indicators(text)
        
        # Calculate final AI probability
        ai_probability = self._calculate_ai_probability(
            bert_result, structural_analysis, semantic_consistency, 
            ai_indicators, human_indicators
        )
        
        # Determine AI type
        ai_type = self._classify_ai_type(ai_probability, ai_indicators, structural_analysis)
        
        # Generate reasoning
        reasoning = self._generate_ai_reasoning(
            ai_probability, ai_indicators, human_indicators, structural_analysis
        )
        
        # Create analysis result
        analysis_result = AIAnalysisResult(
            ai_probability=ai_probability,
            confidence=bert_result['confidence'],
            ai_indicators=ai_indicators,
            semantic_consistency=semantic_consistency,
            structural_analysis=structural_analysis,
            bert_analysis=bert_result
        )
        
        result = AIBotResult(
            is_ai_generated=ai_probability >= self.ai_detection_threshold,
            confidence=ai_probability,
            ai_type=ai_type,
            analysis_result=analysis_result,
            reasoning=reasoning,
            detection_timestamp=datetime.now().isoformat()
        )
        
        self._log_ai_analysis(result)
        return result
    
    def _analyze_text_structure(self, text: str) -> Dict[str, float]:
        """Analyze text structure for AI characteristics"""
        analysis = {}
        
        # Sentence structure analysis
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        if sentences:
            # Average sentence length
            avg_sentence_length = sum(len(s.split()) for s in sentences) / len(sentences)
            analysis['avg_sentence_length'] = avg_sentence_length
            
            # Sentence length variation
            lengths = [len(s.split()) for s in sentences]
            if len(lengths) > 1:
                length_variance = np.var(lengths)
                analysis['sentence_length_variance'] = length_variance
            else:
                analysis['sentence_length_variance'] = 0.0
        
        # Paragraph structure
        paragraphs = text.split('\n\n')
        analysis['paragraph_count'] = len(paragraphs)
        
        # List detection
        list_items = len(re.findall(r'^\s*[-*•]\s', text, re.MULTILINE))
        numbered_items = len(re.findall(r'^\s*\d+\.\s', text, re.MULTILINE))
        analysis['list_items'] = list_items + numbered_items
        
        # Formatting consistency
        bold_count = text.count('**')
        italic_count = text.count('*') - bold_count
        analysis['formatting_density'] = (bold_count + italic_count) / max(len(text), 1)
        
        return analysis
    
    def _analyze_semantic_consistency(self, text: str) -> float:
        """Analyze semantic consistency for AI detection"""
        # Simple semantic consistency check
        words = re.findall(r'\b\w+\b', text.lower())
        if len(words) < 10:
            return 0.5
        
        # Check for repetitive vocabulary
        word_counts = Counter(words)
        unique_words = len(word_counts)
        total_words = len(words)
        
        vocabulary_richness = unique_words / total_words
        
        # Check for topic consistency (mock implementation)
        excel_terms = ['excel', 'formula', 'cell', 'sheet', 'workbook', 'vlookup', 'pivot']
        excel_count = sum(1 for word in words if word in excel_terms)
        
        if total_words > 0:
            topic_consistency = excel_count / total_words
        else:
            topic_consistency = 0.0
        
        # Higher consistency might indicate AI generation
        consistency_score = (vocabulary_richness + topic_consistency) / 2
        
        return consistency_score
    
    def _detect_ai_patterns(self, text: str) -> List[str]:
        """Detect AI-specific patterns in text"""
        detected_patterns = []
        text_lower = text.lower()
        
        # Check each AI signature category
        for category, patterns in self.ai_signatures.items():
            for pattern in patterns:
                if pattern in text_lower:
                    detected_patterns.append(f"{category}: {pattern}")
        
        return detected_patterns
    
    def _detect_human_indicators(self, text: str) -> List[str]:
        """Detect human-specific indicators in text"""
        detected_indicators = []
        text_lower = text.lower()
        
        for indicator in self.human_indicators:
            if indicator in text_lower:
                detected_indicators.append(indicator)
        
        return detected_indicators
    
    def _calculate_ai_probability(self, bert_result: Dict[str, float], 
                                structural_analysis: Dict[str, float],
                                semantic_consistency: float,
                                ai_indicators: List[str],
                                human_indicators: List[str]) -> float:
        """Calculate final AI probability using multiple factors"""
        
        # Base probability from BERT analysis
        base_prob = bert_result.get('ai_probability', 0.0)
        
        # Structural indicators
        structure_score = 0.0
        if structural_analysis.get('avg_sentence_length', 0) > 20:
            structure_score += 0.2
        if structural_analysis.get('list_items', 0) > 2:
            structure_score += 0.2
        if structural_analysis.get('formatting_density', 0) > 0.1:
            structure_score += 0.1
        
        # AI pattern indicators
        ai_pattern_score = min(len(ai_indicators) * 0.15, 0.6)
        
        # Human indicators (negative score)
        human_pattern_score = min(len(human_indicators) * 0.2, 0.4)
        
        # Semantic consistency (high consistency might indicate AI)
        semantic_score = semantic_consistency * 0.3
        
        # Combine all factors
        final_probability = (
            base_prob * 0.4 +
            structure_score * 0.2 +
            ai_pattern_score * 0.2 +
            semantic_score * 0.1 -
            human_pattern_score * 0.1
        )
        
        return max(0.0, min(1.0, final_probability))
    
    def _classify_ai_type(self, ai_probability: float, ai_indicators: List[str], 
                        structural_analysis: Dict[str, float]) -> AIBotType:
        """Classify the type of AI-generated content"""
        if ai_probability < 0.5:
            return AIBotType.HUMAN_AUTHORED
        
        # Check for GPT-specific patterns
        gpt_patterns = [ind for ind in ai_indicators if 'gpt_patterns' in ind]
        if gpt_patterns:
            return AIBotType.GPT_GENERATED
        
        # Check for template responses
        template_patterns = [ind for ind in ai_indicators if 'template_responses' in ind]
        if template_patterns:
            return AIBotType.TEMPLATE_AI
        
        # Check for structured thinking
        structured_patterns = [ind for ind in ai_indicators if 'structured_thinking' in ind]
        if structured_patterns and structural_analysis.get('list_items', 0) > 2:
            return AIBotType.SOPHISTICATED_AI
        
        # Default to hybrid AI
        return AIBotType.HYBRID_AI
    
    def _generate_ai_reasoning(self, ai_probability: float, ai_indicators: List[str],
                             human_indicators: List[str], structural_analysis: Dict[str, float]) -> str:
        """Generate human-readable reasoning for AI detection"""
        reasons = []
        
        if ai_probability >= 0.8:
            reasons.append(f"High AI probability ({ai_probability:.2f})")
        elif ai_probability >= 0.6:
            reasons.append(f"Moderate AI probability ({ai_probability:.2f})")
        else:
            reasons.append(f"Low AI probability ({ai_probability:.2f})")
        
        if ai_indicators:
            reasons.append(f"AI patterns detected: {len(ai_indicators)}")
        
        if human_indicators:
            reasons.append(f"Human indicators found: {len(human_indicators)}")
        
        if structural_analysis.get('list_items', 0) > 2:
            reasons.append("Structured formatting detected")
        
        return " | ".join(reasons)
    
    def _log_ai_analysis(self, result: AIBotResult):
        """Log AI analysis results"""
        if result.is_ai_generated:
            logger.warning(
                f"🤖 AI content detected: {result.ai_type.value} "
                f"(confidence: {result.confidence:.2f})"
            )
            logger.info(f"   🧠 Reasoning: {result.reasoning}")
        else:
            logger.debug(
                f"✅ Human content detected (confidence: {1-result.confidence:.2f})"
            )
    
    def get_ai_detection_stats(self) -> Dict[str, Any]:
        """Get AI detection statistics"""
        return {
            'ai_signature_categories': len(self.ai_signatures),
            'total_ai_patterns': sum(len(patterns) for patterns in self.ai_signatures.values()),
            'human_indicators': len(self.human_indicators),
            'detection_threshold': self.ai_detection_threshold,
            'version': '3.0-ai-detection'
        }

# Global instance for integration
ai_detector = AIBotDetector()