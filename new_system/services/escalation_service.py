"""
Escalation Service for Excel Q&A System
Intelligent escalation logic with learning capabilities
"""
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import asyncio
import json

logger = logging.getLogger('escalation_service')

class EscalationReason(Enum):
    """Reasons for escalation"""
    QUALITY_THRESHOLD = "quality_threshold"
    FORMULA_VALIDATION_FAILED = "formula_validation_failed"
    COMPLEXITY_MISMATCH = "complexity_mismatch"
    USER_FEEDBACK = "user_feedback"
    COST_OPTIMIZATION = "cost_optimization"
    TIMEOUT = "timeout"

@dataclass
class EscalationEvent:
    """Record of an escalation event"""
    timestamp: str
    from_tier: str
    to_tier: str
    reason: EscalationReason
    question: str
    original_score: float
    final_score: float
    success: bool
    cost_impact: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "from_tier": self.from_tier,
            "to_tier": self.to_tier,
            "reason": self.reason.value,
            "question": self.question[:100] + "..." if len(self.question) > 100 else self.question,
            "original_score": self.original_score,
            "final_score": self.final_score,
            "success": self.success,
            "cost_impact": self.cost_impact
        }

class EscalationService:
    """Service for managing intelligent escalation logic"""
    
    def __init__(self):
        # Escalation statistics
        self.stats = {
            "total_escalations": 0,
            "successful_escalations": 0,
            "failed_escalations": 0,
            "average_improvement": 0.0,
            "total_cost_impact": 0.0,
            "escalation_by_reason": {reason.value: 0 for reason in EscalationReason},
            "escalation_by_tier": {
                "tier_1_to_2": 0,
                "tier_2_to_3": 0,
                "tier_1_to_3": 0
            }
        }
        
        # Learning data
        self.escalation_history: List[EscalationEvent] = []
        self.max_history_size = 1000
        
        # Dynamic thresholds (learned from experience)
        self.adaptive_thresholds = {
            "tier_1": 0.70,
            "tier_2": 0.80,
            "tier_3": 0.90
        }
        
        # Escalation rules
        self.escalation_rules = {
            "max_escalations_per_question": 2,
            "min_improvement_threshold": 0.05,
            "cost_escalation_threshold": 0.001,  # $0.001
            "timeout_threshold": 30.0,  # 30 seconds
            "formula_validation_weight": 0.3
        }
    
    async def should_escalate(self, current_tier: str, quality_score: float, 
                            formula_validation_score: float, response_time: float,
                            cost_so_far: float, question: str) -> Tuple[bool, EscalationReason, str]:
        """Determine if escalation is needed and to which tier"""
        
        # Check quality threshold
        threshold = self.adaptive_thresholds.get(current_tier, 0.70)
        
        # Combine quality and formula validation scores
        combined_score = (quality_score * 0.7) + (formula_validation_score * 0.3)
        
        if combined_score < threshold:
            next_tier = self._get_next_tier(current_tier)
            if next_tier:
                return True, EscalationReason.QUALITY_THRESHOLD, next_tier
        
        # Check formula validation specifically
        if formula_validation_score < 0.5:
            next_tier = self._get_next_tier(current_tier)
            if next_tier:
                return True, EscalationReason.FORMULA_VALIDATION_FAILED, next_tier
        
        # Check complexity mismatch
        if await self._detect_complexity_mismatch(question, current_tier):
            next_tier = self._get_next_tier(current_tier)
            if next_tier:
                return True, EscalationReason.COMPLEXITY_MISMATCH, next_tier
        
        # Check timeout
        if response_time > self.escalation_rules["timeout_threshold"]:
            next_tier = self._get_next_tier(current_tier)
            if next_tier:
                return True, EscalationReason.TIMEOUT, next_tier
        
        # Check cost optimization (skip tier if cost is getting too high)
        if cost_so_far > self.escalation_rules["cost_escalation_threshold"]:
            if current_tier == "tier_1":
                # Skip tier 2, go directly to tier 3 for cost efficiency
                return True, EscalationReason.COST_OPTIMIZATION, "tier_3"
        
        return False, None, None
    
    def _get_next_tier(self, current_tier: str) -> Optional[str]:
        """Get the next tier for escalation"""
        tier_progression = {
            "tier_1": "tier_2",
            "tier_2": "tier_3",
            "tier_3": None  # No further escalation
        }
        return tier_progression.get(current_tier)
    
    async def _detect_complexity_mismatch(self, question: str, current_tier: str) -> bool:
        """Detect if question complexity doesn't match current tier"""
        
        # Keywords indicating high complexity
        high_complexity_keywords = [
            "vba", "macro", "array formula", "pivot table", "power query",
            "complex", "advanced", "multiple conditions", "nested",
            "dynamic array", "lambda", "let function"
        ]
        
        # Keywords indicating low complexity
        low_complexity_keywords = [
            "sum", "average", "count", "basic", "simple", "easy",
            "beginner", "how to", "what is"
        ]
        
        question_lower = question.lower()
        
        # Check for high complexity keywords in lower tier
        if current_tier == "tier_1":
            high_complexity_count = sum(1 for keyword in high_complexity_keywords if keyword in question_lower)
            if high_complexity_count >= 2:
                return True
        
        # Check for low complexity keywords in higher tier
        if current_tier == "tier_3":
            low_complexity_count = sum(1 for keyword in low_complexity_keywords if keyword in question_lower)
            if low_complexity_count >= 2:
                return False  # Don't escalate further
        
        return False
    
    async def record_escalation(self, from_tier: str, to_tier: str, reason: EscalationReason,
                               question: str, original_score: float, final_score: float,
                               success: bool, cost_impact: float):
        """Record an escalation event for learning"""
        
        event = EscalationEvent(
            timestamp=datetime.now().isoformat(),
            from_tier=from_tier,
            to_tier=to_tier,
            reason=reason,
            question=question,
            original_score=original_score,
            final_score=final_score,
            success=success,
            cost_impact=cost_impact
        )
        
        # Add to history
        self.escalation_history.append(event)
        
        # Maintain history size
        if len(self.escalation_history) > self.max_history_size:
            self.escalation_history.pop(0)
        
        # Update statistics
        self.stats["total_escalations"] += 1
        self.stats["escalation_by_reason"][reason.value] += 1
        
        tier_key = f"{from_tier}_to_{to_tier.split('_')[1]}"
        if tier_key in self.stats["escalation_by_tier"]:
            self.stats["escalation_by_tier"][tier_key] += 1
        
        if success:
            self.stats["successful_escalations"] += 1
            improvement = final_score - original_score
            self._update_average_improvement(improvement)
        else:
            self.stats["failed_escalations"] += 1
        
        self.stats["total_cost_impact"] += cost_impact
        
        # Learn from this escalation
        await self._learn_from_escalation(event)
        
        logger.info(f"Recorded escalation: {from_tier} → {to_tier} (reason: {reason.value})")
    
    async def _learn_from_escalation(self, event: EscalationEvent):
        """Learn from escalation events to improve future decisions"""
        
        # Adjust thresholds based on success rate
        if len(self.escalation_history) >= 10:  # Need minimum data
            recent_events = self.escalation_history[-10:]
            
            # Calculate success rate for each tier
            for tier in ["tier_1", "tier_2", "tier_3"]:
                tier_events = [e for e in recent_events if e.from_tier == tier]
                
                if len(tier_events) >= 3:
                    success_rate = sum(1 for e in tier_events if e.success) / len(tier_events)
                    
                    # Adjust threshold based on success rate
                    if success_rate < 0.5:
                        # Too many failures, lower threshold (escalate more)
                        self.adaptive_thresholds[tier] = max(0.5, self.adaptive_thresholds[tier] - 0.05)
                    elif success_rate > 0.8:
                        # Too many successes, raise threshold (escalate less)
                        self.adaptive_thresholds[tier] = min(0.95, self.adaptive_thresholds[tier] + 0.02)
        
        # Log threshold adjustments
        logger.info(f"Adaptive thresholds: {self.adaptive_thresholds}")
    
    def _update_average_improvement(self, improvement: float):
        """Update average improvement statistic"""
        current_avg = self.stats["average_improvement"]
        successful_count = self.stats["successful_escalations"]
        
        if successful_count == 1:
            self.stats["average_improvement"] = improvement
        else:
            self.stats["average_improvement"] = (
                (current_avg * (successful_count - 1) + improvement) / successful_count
            )
    
    async def get_escalation_recommendation(self, question: str, 
                                         current_results: Dict[str, Any]) -> Dict[str, Any]:
        """Get recommendation for escalation based on current results"""
        
        current_tier = current_results.get("tier", "tier_1")
        quality_score = current_results.get("quality_score", 0.0)
        formula_validation_score = current_results.get("formula_validation_score", 1.0)
        response_time = current_results.get("response_time", 0.0)
        cost_so_far = current_results.get("cost", 0.0)
        
        should_escalate, reason, next_tier = await self.should_escalate(
            current_tier, quality_score, formula_validation_score,
            response_time, cost_so_far, question
        )
        
        if should_escalate:
            # Predict expected improvement
            expected_improvement = await self._predict_improvement(current_tier, next_tier, reason)
            
            # Calculate cost impact
            cost_impact = await self._calculate_cost_impact(current_tier, next_tier)
            
            return {
                "should_escalate": True,
                "reason": reason.value,
                "next_tier": next_tier,
                "expected_improvement": expected_improvement,
                "cost_impact": cost_impact,
                "confidence": self._calculate_confidence(reason, current_tier)
            }
        
        return {
            "should_escalate": False,
            "reason": "quality_threshold_met",
            "current_score": quality_score,
            "threshold": self.adaptive_thresholds.get(current_tier, 0.70)
        }
    
    async def _predict_improvement(self, from_tier: str, to_tier: str, reason: EscalationReason) -> float:
        """Predict expected improvement from escalation"""
        
        # Historical data for this type of escalation
        similar_escalations = [
            e for e in self.escalation_history
            if e.from_tier == from_tier and e.to_tier == to_tier and e.reason == reason
        ]
        
        if similar_escalations:
            improvements = [e.final_score - e.original_score for e in similar_escalations if e.success]
            return sum(improvements) / len(improvements) if improvements else 0.15
        
        # Default expectations based on tier jump
        default_improvements = {
            ("tier_1", "tier_2"): 0.15,
            ("tier_2", "tier_3"): 0.10,
            ("tier_1", "tier_3"): 0.25
        }
        
        return default_improvements.get((from_tier, to_tier), 0.10)
    
    async def _calculate_cost_impact(self, from_tier: str, to_tier: str) -> float:
        """Calculate expected cost impact of escalation"""
        
        # Model costs (per 1000 tokens, approximate)
        model_costs = {
            "tier_1": 0.0002,  # Mistral Small 3.1
            "tier_2": 0.0005,  # Llama 4 Maverick
            "tier_3": 0.0015   # GPT-4.1 Mini
        }
        
        from_cost = model_costs.get(from_tier, 0.0005)
        to_cost = model_costs.get(to_tier, 0.0005)
        
        # Assume average of 1000 tokens per response
        return (to_cost - from_cost) * 1000
    
    def _calculate_confidence(self, reason: EscalationReason, current_tier: str) -> float:
        """Calculate confidence in escalation recommendation"""
        
        # Base confidence by reason
        reason_confidence = {
            EscalationReason.QUALITY_THRESHOLD: 0.8,
            EscalationReason.FORMULA_VALIDATION_FAILED: 0.9,
            EscalationReason.COMPLEXITY_MISMATCH: 0.7,
            EscalationReason.TIMEOUT: 0.6,
            EscalationReason.COST_OPTIMIZATION: 0.5,
            EscalationReason.USER_FEEDBACK: 0.95
        }
        
        base_confidence = reason_confidence.get(reason, 0.6)
        
        # Adjust based on historical success rate
        similar_escalations = [
            e for e in self.escalation_history
            if e.from_tier == current_tier and e.reason == reason
        ]
        
        if len(similar_escalations) >= 5:
            success_rate = sum(1 for e in similar_escalations if e.success) / len(similar_escalations)
            base_confidence = (base_confidence + success_rate) / 2
        
        return base_confidence
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get escalation service statistics"""
        return {
            "stats": self.stats.copy(),
            "adaptive_thresholds": self.adaptive_thresholds.copy(),
            "escalation_rules": self.escalation_rules.copy(),
            "history_size": len(self.escalation_history),
            "recent_events": [e.to_dict() for e in self.escalation_history[-5:]],
            "timestamp": datetime.now().isoformat()
        }
    
    def reset_learning(self):
        """Reset learning data (for testing or retraining)"""
        self.escalation_history.clear()
        self.adaptive_thresholds = {
            "tier_1": 0.70,
            "tier_2": 0.80,
            "tier_3": 0.90
        }
        logger.info("Escalation learning data reset")

# Singleton instance
_escalation_service = None

async def get_escalation_service() -> EscalationService:
    """Get singleton escalation service instance"""
    global _escalation_service
    if _escalation_service is None:
        _escalation_service = EscalationService()
    return _escalation_service