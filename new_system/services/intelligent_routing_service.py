"""
Intelligent Routing Service for Excel Q&A System
Advanced routing logic with cost optimization and performance monitoring
"""
import logging
import asyncio
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import json
import re
from statistics import mean, median

from services.escalation_service import EscalationService, EscalationReason

logger = logging.getLogger('intelligent_routing_service')

class RoutingStrategy(Enum):
    """Routing strategies"""
    COST_OPTIMIZED = "cost_optimized"
    PERFORMANCE_OPTIMIZED = "performance_optimized"
    QUALITY_OPTIMIZED = "quality_optimized"
    BALANCED = "balanced"

class ComplexityLevel(Enum):
    """Question complexity levels"""
    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"
    EXPERT = "expert"

@dataclass
class RoutingDecision:
    """Routing decision with reasoning"""
    chosen_tier: str
    strategy: RoutingStrategy
    complexity_level: ComplexityLevel
    confidence: float
    expected_cost: float
    expected_quality: float
    expected_time: float
    reasoning: str
    alternative_tiers: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class PerformanceMetrics:
    """Performance metrics for a model/tier"""
    tier: str
    total_requests: int
    successful_requests: int
    average_quality: float
    average_cost: float
    average_response_time: float
    success_rate: float
    last_updated: str

class IntelligentRoutingService:
    """Intelligent routing service with adaptive learning"""
    
    def __init__(self):
        # Performance tracking
        self.performance_metrics: Dict[str, PerformanceMetrics] = {}
        
        # Complexity patterns (learned from experience)
        self.complexity_patterns = {
            ComplexityLevel.SIMPLE: {
                "keywords": ["sum", "average", "count", "basic", "simple", "how to"],
                "max_length": 100,
                "function_count": 1
            },
            ComplexityLevel.MODERATE: {
                "keywords": ["vlookup", "if", "conditional", "lookup", "multiple"],
                "max_length": 300,
                "function_count": 3
            },
            ComplexityLevel.COMPLEX: {
                "keywords": ["nested", "array", "index match", "pivot", "advanced"],
                "max_length": 500,
                "function_count": 5
            },
            ComplexityLevel.EXPERT: {
                "keywords": ["vba", "macro", "power query", "dynamic array", "lambda"],
                "max_length": 1000,
                "function_count": 10
            }
        }
        
        # Model cost estimations (per 1000 tokens)
        self.model_costs = {
            "tier_1": 0.00015,  # Mistral Small 3.1
            "tier_2": 0.00039,  # Llama 4 Maverick
            "tier_3": 0.00100   # GPT-4.1 Mini
        }
        
        # Expected performance baselines
        self.baseline_performance = {
            "tier_1": {"quality": 0.75, "time": 3.0, "success_rate": 0.80},
            "tier_2": {"quality": 0.85, "time": 5.0, "success_rate": 0.90},
            "tier_3": {"quality": 0.95, "time": 8.0, "success_rate": 0.95}
        }
        
        # Routing statistics
        self.routing_stats = {
            "total_routings": 0,
            "strategy_usage": {strategy.value: 0 for strategy in RoutingStrategy},
            "tier_assignments": {"tier_1": 0, "tier_2": 0, "tier_3": 0},
            "complexity_distribution": {level.value: 0 for level in ComplexityLevel},
            "routing_accuracy": 0.0,
            "cost_savings": 0.0,
            "performance_improvements": 0.0
        }
        
        # Configuration
        self.config = {
            "default_strategy": RoutingStrategy.BALANCED,
            "cost_threshold": 0.01,  # $0.01 per request
            "quality_threshold": 0.85,
            "time_threshold": 10.0,  # 10 seconds
            "learning_rate": 0.1,
            "confidence_threshold": 0.7
        }
    
    async def route_question(self, question: str, context: str = "", 
                           images: List[str] = None, strategy: RoutingStrategy = None) -> RoutingDecision:
        """Make intelligent routing decision for a question"""
        try:
            self.routing_stats["total_routings"] += 1
            
            # Use default strategy if none provided
            if strategy is None:
                strategy = self.config["default_strategy"]
            
            # Analyze question complexity
            complexity_level = await self._analyze_complexity(question, context, images)
            
            # Get routing decision based on strategy
            decision = await self._make_routing_decision(question, context, complexity_level, strategy)
            
            # Update statistics
            self.routing_stats["strategy_usage"][strategy.value] += 1
            self.routing_stats["tier_assignments"][decision.chosen_tier] += 1
            self.routing_stats["complexity_distribution"][complexity_level.value] += 1
            
            logger.info(f"Routed question to {decision.chosen_tier} (strategy: {strategy.value}, complexity: {complexity_level.value})")
            return decision
            
        except Exception as e:
            logger.error(f"Error in intelligent routing: {e}")
            # Fallback to simple routing
            return self._fallback_routing(question, context)
    
    async def _analyze_complexity(self, question: str, context: str, images: List[str] = None) -> ComplexityLevel:
        """Analyze question complexity using multiple factors"""
        try:
            # Text analysis
            full_text = f"{question} {context}".lower()
            text_length = len(full_text)
            
            # Count Excel functions mentioned
            excel_functions = self._extract_excel_functions(full_text)
            function_count = len(excel_functions)
            
            # Image complexity (more images = potentially more complex)
            image_complexity = len(images) if images else 0
            
            # Keyword analysis
            complexity_scores = {}
            for level, patterns in self.complexity_patterns.items():
                score = 0
                
                # Keyword matching
                keyword_matches = sum(1 for keyword in patterns["keywords"] if keyword in full_text)
                score += keyword_matches * 0.3
                
                # Length factor
                if text_length <= patterns["max_length"]:
                    score += 0.2
                
                # Function count factor
                if function_count <= patterns["function_count"]:
                    score += 0.3
                
                # Image factor
                if image_complexity > 0:
                    score += min(image_complexity * 0.1, 0.2)
                
                complexity_scores[level] = score
            
            # Find the best matching complexity level
            best_match = max(complexity_scores, key=complexity_scores.get)
            confidence = complexity_scores[best_match]
            
            # If confidence is low, use moderate as default
            if confidence < 0.3:
                best_match = ComplexityLevel.MODERATE
            
            return best_match
            
        except Exception as e:
            logger.error(f"Error analyzing complexity: {e}")
            return ComplexityLevel.MODERATE
    
    def _extract_excel_functions(self, text: str) -> List[str]:
        """Extract Excel function names from text"""
        excel_functions = [
            "sum", "average", "count", "max", "min", "if", "vlookup", "hlookup",
            "index", "match", "sumif", "sumifs", "countif", "countifs", "round",
            "abs", "and", "or", "not", "iferror", "xlookup", "filter", "sort",
            "unique", "textjoin", "concatenate", "left", "right", "mid", "len",
            "date", "today", "now", "year", "month", "day", "pivot", "macro", "vba"
        ]
        
        found_functions = []
        for func in excel_functions:
            if func in text:
                found_functions.append(func)
        
        return found_functions
    
    async def _make_routing_decision(self, question: str, context: str, 
                                   complexity_level: ComplexityLevel, 
                                   strategy: RoutingStrategy) -> RoutingDecision:
        """Make routing decision based on strategy and complexity"""
        
        # Get candidate tiers based on complexity
        candidate_tiers = self._get_candidate_tiers(complexity_level)
        
        # Evaluate each candidate
        evaluations = {}
        for tier in candidate_tiers:
            evaluations[tier] = await self._evaluate_tier(tier, question, context, strategy)
        
        # Select best tier based on strategy
        chosen_tier = self._select_best_tier(evaluations, strategy)
        
        # Build decision object
        decision = RoutingDecision(
            chosen_tier=chosen_tier,
            strategy=strategy,
            complexity_level=complexity_level,
            confidence=evaluations[chosen_tier]["confidence"],
            expected_cost=evaluations[chosen_tier]["cost"],
            expected_quality=evaluations[chosen_tier]["quality"],
            expected_time=evaluations[chosen_tier]["time"],
            reasoning=evaluations[chosen_tier]["reasoning"],
            alternative_tiers=[tier for tier in candidate_tiers if tier != chosen_tier]
        )
        
        return decision
    
    def _get_candidate_tiers(self, complexity_level: ComplexityLevel) -> List[str]:
        """Get candidate tiers based on complexity level"""
        tier_mapping = {
            ComplexityLevel.SIMPLE: ["tier_1", "tier_2"],
            ComplexityLevel.MODERATE: ["tier_1", "tier_2", "tier_3"],
            ComplexityLevel.COMPLEX: ["tier_2", "tier_3"],
            ComplexityLevel.EXPERT: ["tier_3"]
        }
        
        return tier_mapping.get(complexity_level, ["tier_1", "tier_2", "tier_3"])
    
    async def _evaluate_tier(self, tier: str, question: str, context: str, 
                           strategy: RoutingStrategy) -> Dict[str, Any]:
        """Evaluate a tier for routing decision"""
        
        # Get performance metrics
        metrics = self.performance_metrics.get(tier)
        
        if metrics:
            # Use real performance data
            expected_quality = metrics.average_quality
            expected_cost = metrics.average_cost
            expected_time = metrics.average_response_time
            confidence = min(metrics.success_rate, 1.0)
        else:
            # Use baseline estimates
            baseline = self.baseline_performance[tier]
            expected_quality = baseline["quality"]
            expected_cost = self.model_costs[tier]
            expected_time = baseline["time"]
            confidence = baseline["success_rate"]
        
        # Adjust based on question characteristics
        question_length = len(question + context)
        if question_length > 500:
            expected_time *= 1.2
            expected_cost *= 1.1
        
        # Strategy-specific adjustments
        if strategy == RoutingStrategy.COST_OPTIMIZED:
            # Favor lower cost tiers
            if tier == "tier_1":
                confidence += 0.1
            elif tier == "tier_3":
                confidence -= 0.1
        
        elif strategy == RoutingStrategy.QUALITY_OPTIMIZED:
            # Favor higher quality tiers
            if tier == "tier_3":
                confidence += 0.1
            elif tier == "tier_1":
                confidence -= 0.1
        
        elif strategy == RoutingStrategy.PERFORMANCE_OPTIMIZED:
            # Favor faster tiers
            if expected_time < 5.0:
                confidence += 0.1
            elif expected_time > 10.0:
                confidence -= 0.1
        
        # Generate reasoning
        reasoning = self._generate_reasoning(tier, expected_quality, expected_cost, expected_time, strategy)
        
        return {
            "quality": expected_quality,
            "cost": expected_cost,
            "time": expected_time,
            "confidence": min(max(confidence, 0.0), 1.0),
            "reasoning": reasoning
        }
    
    def _select_best_tier(self, evaluations: Dict[str, Dict[str, Any]], 
                         strategy: RoutingStrategy) -> str:
        """Select the best tier based on strategy"""
        
        if strategy == RoutingStrategy.COST_OPTIMIZED:
            # Select tier with lowest cost and acceptable quality
            valid_tiers = {tier: eval for tier, eval in evaluations.items() 
                          if eval["quality"] >= self.config["quality_threshold"]}
            if valid_tiers:
                return min(valid_tiers, key=lambda t: valid_tiers[t]["cost"])
        
        elif strategy == RoutingStrategy.QUALITY_OPTIMIZED:
            # Select tier with highest quality
            return max(evaluations, key=lambda t: evaluations[t]["quality"])
        
        elif strategy == RoutingStrategy.PERFORMANCE_OPTIMIZED:
            # Select tier with fastest response time and acceptable quality
            valid_tiers = {tier: eval for tier, eval in evaluations.items() 
                          if eval["quality"] >= self.config["quality_threshold"]}
            if valid_tiers:
                return min(valid_tiers, key=lambda t: valid_tiers[t]["time"])
        
        else:  # BALANCED
            # Balance cost, quality, and time
            scores = {}
            for tier, eval in evaluations.items():
                # Normalize scores (0-1 scale)
                quality_score = eval["quality"]
                cost_score = 1.0 - (eval["cost"] / max(e["cost"] for e in evaluations.values()))
                time_score = 1.0 - (eval["time"] / max(e["time"] for e in evaluations.values()))
                
                # Weighted combination
                balanced_score = (quality_score * 0.4 + cost_score * 0.3 + time_score * 0.3)
                scores[tier] = balanced_score
            
            return max(scores, key=scores.get)
        
        # Fallback to tier with highest confidence
        return max(evaluations, key=lambda t: evaluations[t]["confidence"])
    
    def _generate_reasoning(self, tier: str, quality: float, cost: float, 
                          time: float, strategy: RoutingStrategy) -> str:
        """Generate human-readable reasoning for routing decision"""
        
        tier_names = {
            "tier_1": "Mistral Small 3.1 (Fast & Economical)",
            "tier_2": "Llama 4 Maverick (Balanced Performance)",
            "tier_3": "GPT-4.1 Mini (Highest Quality)"
        }
        
        tier_name = tier_names.get(tier, tier)
        
        reason_parts = [f"Selected {tier_name}"]
        
        if strategy == RoutingStrategy.COST_OPTIMIZED:
            reason_parts.append(f"for cost optimization (${cost:.4f} estimated)")
        elif strategy == RoutingStrategy.QUALITY_OPTIMIZED:
            reason_parts.append(f"for maximum quality ({quality:.1%} expected)")
        elif strategy == RoutingStrategy.PERFORMANCE_OPTIMIZED:
            reason_parts.append(f"for fast response ({time:.1f}s estimated)")
        else:
            reason_parts.append(f"for balanced performance (quality: {quality:.1%}, cost: ${cost:.4f}, time: {time:.1f}s)")
        
        return " ".join(reason_parts)
    
    def _fallback_routing(self, question: str, context: str) -> RoutingDecision:
        """Fallback routing when intelligent routing fails"""
        
        # Simple heuristic: use tier 2 for most questions
        return RoutingDecision(
            chosen_tier="tier_2",
            strategy=RoutingStrategy.BALANCED,
            complexity_level=ComplexityLevel.MODERATE,
            confidence=0.5,
            expected_cost=self.model_costs["tier_2"],
            expected_quality=0.85,
            expected_time=5.0,
            reasoning="Fallback routing to balanced tier",
            alternative_tiers=["tier_1", "tier_3"]
        )
    
    async def record_performance(self, tier: str, quality_score: float, 
                               cost: float, response_time: float, success: bool):
        """Record performance metrics for learning"""
        
        if tier not in self.performance_metrics:
            self.performance_metrics[tier] = PerformanceMetrics(
                tier=tier,
                total_requests=0,
                successful_requests=0,
                average_quality=0.0,
                average_cost=0.0,
                average_response_time=0.0,
                success_rate=0.0,
                last_updated=datetime.now().isoformat()
            )
        
        metrics = self.performance_metrics[tier]
        
        # Update metrics
        metrics.total_requests += 1
        if success:
            metrics.successful_requests += 1
        
        # Update running averages
        n = metrics.total_requests
        metrics.average_quality = ((metrics.average_quality * (n - 1)) + quality_score) / n
        metrics.average_cost = ((metrics.average_cost * (n - 1)) + cost) / n
        metrics.average_response_time = ((metrics.average_response_time * (n - 1)) + response_time) / n
        metrics.success_rate = metrics.successful_requests / metrics.total_requests
        metrics.last_updated = datetime.now().isoformat()
        
        logger.info(f"Updated performance metrics for {tier}: quality={metrics.average_quality:.2f}, cost=${metrics.average_cost:.4f}")
    
    async def optimize_routing_strategy(self) -> Dict[str, Any]:
        """Optimize routing strategy based on performance data"""
        
        if not self.performance_metrics:
            return {"message": "No performance data available for optimization"}
        
        # Analyze performance patterns
        analysis = {
            "tier_performance": {},
            "recommendations": [],
            "cost_savings_potential": 0.0,
            "quality_improvement_potential": 0.0
        }
        
        for tier, metrics in self.performance_metrics.items():
            analysis["tier_performance"][tier] = {
                "success_rate": metrics.success_rate,
                "average_quality": metrics.average_quality,
                "average_cost": metrics.average_cost,
                "average_time": metrics.average_response_time,
                "total_requests": metrics.total_requests
            }
        
        # Generate recommendations
        best_quality_tier = max(self.performance_metrics, 
                               key=lambda t: self.performance_metrics[t].average_quality)
        best_cost_tier = min(self.performance_metrics, 
                            key=lambda t: self.performance_metrics[t].average_cost)
        best_time_tier = min(self.performance_metrics, 
                            key=lambda t: self.performance_metrics[t].average_response_time)
        
        analysis["recommendations"] = [
            f"For maximum quality: use {best_quality_tier}",
            f"For cost optimization: use {best_cost_tier}",
            f"For fastest response: use {best_time_tier}"
        ]
        
        return analysis
    
    async def get_routing_statistics(self) -> Dict[str, Any]:
        """Get comprehensive routing statistics"""
        
        return {
            "routing_stats": self.routing_stats.copy(),
            "performance_metrics": {
                tier: asdict(metrics) for tier, metrics in self.performance_metrics.items()
            },
            "configuration": self.config.copy(),
            "complexity_patterns": {
                level.value: patterns for level, patterns in self.complexity_patterns.items()
            },
            "timestamp": datetime.now().isoformat()
        }
    
    def reset_performance_data(self):
        """Reset performance data for relearning"""
        self.performance_metrics.clear()
        self.routing_stats = {
            "total_routings": 0,
            "strategy_usage": {strategy.value: 0 for strategy in RoutingStrategy},
            "tier_assignments": {"tier_1": 0, "tier_2": 0, "tier_3": 0},
            "complexity_distribution": {level.value: 0 for level in ComplexityLevel},
            "routing_accuracy": 0.0,
            "cost_savings": 0.0,
            "performance_improvements": 0.0
        }
        logger.info("Routing performance data reset")

# Singleton instance
_intelligent_routing_service = None

async def get_intelligent_routing_service() -> IntelligentRoutingService:
    """Get singleton intelligent routing service instance"""
    global _intelligent_routing_service
    if _intelligent_routing_service is None:
        _intelligent_routing_service = IntelligentRoutingService()
    return _intelligent_routing_service