"""
Integrated Bot Detection System - Layers 1 & 2 Combined
Advanced multi-layer bot detection combining immediate blocking with behavioral analysis

This module provides the integrated bot detection system that combines:
- Layer 1: Immediate blocking using PRAW metadata and text patterns
- Layer 2: Behavioral analysis with CQS patterns and timing analysis
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from .advanced_bot_detector import AdvancedBotDetector, BotDetectionResult, BotType
from .behavioral_bot_detector import BehavioralBotDetector, BehavioralBotResult, BehavioralBotType

logger = logging.getLogger('pipeline.integrated_bot_detector')

class IntegratedBotType(Enum):
    """Integrated bot classification combining both layers"""
    IMMEDIATE_BLOCK = "immediate_block"      # Layer 1 detection
    BEHAVIORAL_BLOCK = "behavioral_block"    # Layer 2 detection
    SOPHISTICATED_BLOCK = "sophisticated_block"  # Both layers detect
    HUMAN_VERIFIED = "human_verified"       # Both layers confirm human
    HUMAN_LIKELY = "human_likely"           # One layer confirms human

@dataclass
class IntegratedBotResult:
    """Integrated bot detection result combining both layers"""
    is_bot: bool
    confidence: float
    integrated_type: IntegratedBotType
    layer1_result: BotDetectionResult
    layer2_result: Optional[BehavioralBotResult]
    final_reasoning: str
    detection_timestamp: str

class IntegratedBotDetector:
    """
    Integrated bot detection system combining multiple detection layers
    
    Features:
    - Layer 1: Immediate pattern-based detection
    - Layer 2: Behavioral analysis (when user history available)
    - Confidence weighting and fusion
    - Multi-stage escalation system
    """
    
    def __init__(self):
        self.layer1_detector = AdvancedBotDetector()
        self.layer2_detector = BehavioralBotDetector()
        self.detection_stats = {
            'layer1_detections': 0,
            'layer2_detections': 0,
            'combined_detections': 0,
            'human_confirmations': 0
        }
        
    def detect_bot_integrated(self, comment_data: Dict[str, Any], 
                            user_data: Optional[Dict[str, Any]] = None,
                            user_history: Optional[List[Dict[str, Any]]] = None) -> IntegratedBotResult:
        """
        Integrated bot detection using multiple layers
        
        Args:
            comment_data: Comment content and metadata
            user_data: User account information (optional)
            user_history: User's comment history (optional, for Layer 2)
            
        Returns:
            IntegratedBotResult with combined analysis
        """
        # Layer 1: Immediate blocking
        layer1_result = self.layer1_detector.detect_bot_comprehensive(comment_data, user_data)
        
        # Layer 2: Behavioral analysis (if user history available)
        layer2_result = None
        if user_data and user_history and len(user_history) >= 2:
            try:
                layer2_result = self.layer2_detector.analyze_user_behavior(user_data, user_history)
            except Exception as e:
                logger.warning(f"Layer 2 analysis failed: {e}")
                layer2_result = None
        
        # Combine results
        integrated_result = self._combine_detection_results(layer1_result, layer2_result)
        
        # Update statistics
        self._update_detection_stats(integrated_result)
        
        # Log integrated result
        self._log_integrated_result(integrated_result)
        
        return integrated_result
    
    def _combine_detection_results(self, layer1_result: BotDetectionResult, 
                                 layer2_result: Optional[BehavioralBotResult]) -> IntegratedBotResult:
        """Combine Layer 1 and Layer 2 results using intelligent fusion"""
        
        # Base case: Only Layer 1 available
        if layer2_result is None:
            return IntegratedBotResult(
                is_bot=layer1_result.is_bot,
                confidence=layer1_result.confidence,
                integrated_type=IntegratedBotType.IMMEDIATE_BLOCK if layer1_result.is_bot else IntegratedBotType.HUMAN_LIKELY,
                layer1_result=layer1_result,
                layer2_result=None,
                final_reasoning=f"Layer 1 only: {layer1_result.confidence:.2f} confidence",
                detection_timestamp=datetime.now().isoformat()
            )
        
        # Both layers available - intelligent fusion
        layer1_confidence = layer1_result.confidence
        layer2_confidence = layer2_result.confidence
        
        # Weighted combination based on detection strengths
        layer1_weight = 0.6  # Layer 1 is more reliable for immediate threats
        layer2_weight = 0.4  # Layer 2 provides behavioral context
        
        # Special weighting adjustments
        if layer1_confidence > 0.9:  # High confidence Layer 1 detection
            layer1_weight = 0.8
            layer2_weight = 0.2
        elif layer2_confidence > 0.9:  # High confidence Layer 2 detection
            layer1_weight = 0.4
            layer2_weight = 0.6
        
        # Calculate combined confidence
        combined_confidence = (layer1_confidence * layer1_weight) + (layer2_confidence * layer2_weight)
        
        # Determine final decision
        is_bot = combined_confidence >= 0.7
        
        # Determine integrated type
        integrated_type = self._determine_integrated_type(
            layer1_result.is_bot, layer2_result.is_bot, 
            layer1_confidence, layer2_confidence
        )
        
        # Generate reasoning
        reasoning = self._generate_reasoning(
            layer1_result, layer2_result, combined_confidence
        )
        
        return IntegratedBotResult(
            is_bot=is_bot,
            confidence=combined_confidence,
            integrated_type=integrated_type,
            layer1_result=layer1_result,
            layer2_result=layer2_result,
            final_reasoning=reasoning,
            detection_timestamp=datetime.now().isoformat()
        )
    
    def _determine_integrated_type(self, layer1_is_bot: bool, layer2_is_bot: bool,
                                 layer1_conf: float, layer2_conf: float) -> IntegratedBotType:
        """Determine the integrated bot type based on layer results"""
        
        if layer1_is_bot and layer2_is_bot:
            return IntegratedBotType.SOPHISTICATED_BLOCK
        elif layer1_is_bot and not layer2_is_bot:
            if layer1_conf > 0.8:
                return IntegratedBotType.IMMEDIATE_BLOCK
            else:
                return IntegratedBotType.HUMAN_LIKELY
        elif not layer1_is_bot and layer2_is_bot:
            if layer2_conf > 0.8:
                return IntegratedBotType.BEHAVIORAL_BLOCK
            else:
                return IntegratedBotType.HUMAN_LIKELY
        else:
            return IntegratedBotType.HUMAN_VERIFIED
    
    def _generate_reasoning(self, layer1_result: BotDetectionResult, 
                          layer2_result: BehavioralBotResult, 
                          combined_confidence: float) -> str:
        """Generate human-readable reasoning for the detection"""
        
        reasons = []
        
        # Layer 1 reasoning
        if layer1_result.is_bot:
            reasons.append(f"Layer 1 detected {layer1_result.bot_type.value} "
                         f"({layer1_result.confidence:.2f} confidence)")
            if layer1_result.indicators:
                reasons.append(f"Key indicators: {', '.join(layer1_result.indicators[:3])}")
        else:
            reasons.append(f"Layer 1 suggests human ({1-layer1_result.confidence:.2f} confidence)")
        
        # Layer 2 reasoning
        if layer2_result:
            if layer2_result.is_bot:
                reasons.append(f"Layer 2 detected {layer2_result.bot_type.value} "
                             f"({layer2_result.confidence:.2f} confidence)")
                if layer2_result.indicators:
                    reasons.append(f"Behavioral indicators: {', '.join(layer2_result.indicators[:2])}")
            else:
                reasons.append(f"Layer 2 suggests human ({1-layer2_result.confidence:.2f} confidence)")
        
        # Final decision
        reasons.append(f"Final decision: {combined_confidence:.2f} confidence")
        
        return " | ".join(reasons)
    
    def _update_detection_stats(self, result: IntegratedBotResult):
        """Update detection statistics"""
        if result.layer1_result.is_bot:
            self.detection_stats['layer1_detections'] += 1
        
        if result.layer2_result and result.layer2_result.is_bot:
            self.detection_stats['layer2_detections'] += 1
        
        if result.is_bot:
            self.detection_stats['combined_detections'] += 1
        else:
            self.detection_stats['human_confirmations'] += 1
    
    def _log_integrated_result(self, result: IntegratedBotResult):
        """Log integrated detection result"""
        if result.is_bot:
            logger.warning(
                f"🤖 INTEGRATED BOT DETECTED: {result.integrated_type.value} "
                f"(confidence: {result.confidence:.2f})"
            )
            logger.info(f"   💡 Reasoning: {result.final_reasoning}")
        else:
            logger.debug(
                f"✅ INTEGRATED HUMAN CONFIRMED: {result.integrated_type.value} "
                f"(confidence: {1-result.confidence:.2f})"
            )
    
    def is_bot_simple(self, text: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Simple interface for backward compatibility
        Uses only Layer 1 detection for immediate response
        """
        comment_data = {
            'body': text,
            'author': metadata.get('author', '') if metadata else '',
            'score': metadata.get('score', 0) if metadata else 0,
            'created_utc': metadata.get('created_utc', 0) if metadata else 0
        }
        
        result = self.detect_bot_integrated(comment_data)
        return result.is_bot
    
    def get_detection_performance(self) -> Dict[str, Any]:
        """Get detection performance metrics"""
        total_detections = (self.detection_stats['combined_detections'] + 
                          self.detection_stats['human_confirmations'])
        
        if total_detections == 0:
            return {
                'total_processed': 0,
                'bot_detection_rate': 0.0,
                'human_confirmation_rate': 0.0,
                'layer1_effectiveness': 0.0,
                'layer2_effectiveness': 0.0
            }
        
        return {
            'total_processed': total_detections,
            'bot_detection_rate': self.detection_stats['combined_detections'] / total_detections,
            'human_confirmation_rate': self.detection_stats['human_confirmations'] / total_detections,
            'layer1_detections': self.detection_stats['layer1_detections'],
            'layer2_detections': self.detection_stats['layer2_detections'],
            'combined_detections': self.detection_stats['combined_detections'],
            'version': '1.0-integrated'
        }
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get overall system status"""
        layer1_stats = self.layer1_detector.get_detection_stats()
        layer2_stats = self.layer2_detector.get_behavioral_stats()
        
        return {
            'layer1_status': 'active',
            'layer2_status': 'active',
            'layer1_patterns': layer1_stats['total_patterns'],
            'layer2_metrics': len(layer2_stats),
            'integration_version': '2.0-behavioral-integrated',
            'detection_stats': self.detection_stats
        }

# Global instance for easy integration
integrated_detector = IntegratedBotDetector()

def is_bot_response(text: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
    """
    Global function for backward compatibility
    Uses the integrated detection system
    """
    return integrated_detector.is_bot_simple(text, metadata)