"""
LLM-as-a-Judge Service for Excel Q&A Quality Assessment
Uses OpenRouter models to evaluate response quality
"""
import logging
import asyncio
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from dataclasses import dataclass, asdict
from enum import Enum
import httpx

from config import Config

logger = logging.getLogger('llm_judge_service')

class QualityDimension(Enum):
    """Quality assessment dimensions"""
    FACTUAL_ACCURACY = "factual_accuracy"  # Is the answer factually correct?
    RELEVANCE = "relevance"  # Does the answer address the question?
    COMPLETENESS = "completeness"  # Is the answer complete and comprehensive?
    CLARITY = "clarity"  # Is the answer clear and well-explained?
    PRACTICALITY = "practicality"  # Is the answer practical and usable?

@dataclass
class QualityScore:
    """Quality score for a specific dimension"""
    dimension: QualityDimension
    score: float  # 0.0 to 1.0
    confidence: float  # 0.0 to 1.0
    reasoning: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "dimension": self.dimension.value,
            "score": self.score,
            "confidence": self.confidence,
            "reasoning": self.reasoning
        }

@dataclass
class QualityAssessment:
    """Overall quality assessment"""
    overall_score: float
    dimension_scores: List[QualityScore]
    recommendation: str
    validation_passed: bool
    timestamp: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "overall_score": self.overall_score,
            "dimension_scores": [score.to_dict() for score in self.dimension_scores],
            "recommendation": self.recommendation,
            "validation_passed": self.validation_passed,
            "timestamp": self.timestamp
        }

class LLMJudgeService:
    """LLM-as-a-Judge service for quality assessment"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or Config.OPENROUTER_API_KEY
        self.base_url = "https://openrouter.ai/api/v1"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://excel-qa-system.com",
            "X-Title": "Excel QA Quality Judge"
        }
        self.client = httpx.AsyncClient(headers=self.headers, timeout=45.0)
        
        # Use GPT-4.1 Mini for judging (high accuracy, reasonable cost)
        self.judge_model = "openai/gpt-4.1-mini"
        
        # Quality thresholds for different tiers
        self.quality_thresholds = {
            "tier_1": 0.70,  # 70% threshold for Tier 1
            "tier_2": 0.80,  # 80% threshold for Tier 2
            "tier_3": 0.90   # 90% threshold for Tier 3
        }
        
        # Statistics
        self.stats = {
            "total_assessments": 0,
            "passed_assessments": 0,
            "failed_assessments": 0,
            "average_score": 0.0,
            "last_assessment": None
        }
    
    async def assess_quality(self, question: str, context: str, answer: str,
                           tier: str = "tier_1") -> QualityAssessment:
        """Assess the quality of an Excel Q&A response"""
        try:
            self.stats["total_assessments"] += 1
            
            # Assess each quality dimension
            dimension_scores = []
            
            for dimension in QualityDimension:
                score = await self._assess_dimension(question, context, answer, dimension)
                dimension_scores.append(score)
            
            # Calculate overall score (weighted average)
            overall_score = self._calculate_overall_score(dimension_scores)
            
            # Determine if validation passed
            threshold = self.quality_thresholds.get(tier, 0.70)
            validation_passed = overall_score >= threshold
            
            # Generate recommendation
            recommendation = self._generate_recommendation(dimension_scores, overall_score, validation_passed)
            
            # Update statistics
            if validation_passed:
                self.stats["passed_assessments"] += 1
            else:
                self.stats["failed_assessments"] += 1
            
            self._update_average_score(overall_score)
            self.stats["last_assessment"] = datetime.now().isoformat()
            
            assessment = QualityAssessment(
                overall_score=overall_score,
                dimension_scores=dimension_scores,
                recommendation=recommendation,
                validation_passed=validation_passed,
                timestamp=datetime.now().isoformat()
            )
            
            logger.info(f"Quality assessment: {overall_score:.2f} ({'PASS' if validation_passed else 'FAIL'})")
            return assessment
            
        except Exception as e:
            logger.error(f"Error assessing quality: {e}")
            return QualityAssessment(
                overall_score=0.0,
                dimension_scores=[],
                recommendation="Error during assessment",
                validation_passed=False,
                timestamp=datetime.now().isoformat()
            )
    
    async def _assess_dimension(self, question: str, context: str, answer: str,
                               dimension: QualityDimension) -> QualityScore:
        """Assess a specific quality dimension"""
        try:
            prompt = self._build_assessment_prompt(question, context, answer, dimension)
            
            # Call judge model
            response = await self._call_judge_model(prompt)
            
            if response["success"]:
                # Parse the response
                assessment = self._parse_assessment_response(response["content"], dimension)
                return assessment
            else:
                logger.error(f"Judge model failed for {dimension.value}: {response['error']}")
                return QualityScore(
                    dimension=dimension,
                    score=0.5,
                    confidence=0.0,
                    reasoning="Assessment failed"
                )
                
        except Exception as e:
            logger.error(f"Error assessing dimension {dimension.value}: {e}")
            return QualityScore(
                dimension=dimension,
                score=0.5,
                confidence=0.0,
                reasoning=f"Error: {str(e)}"
            )
    
    def _build_assessment_prompt(self, question: str, context: str, answer: str,
                                dimension: QualityDimension) -> str:
        """Build prompt for quality assessment"""
        base_prompt = f"""You are an expert Excel consultant evaluating the quality of Q&A responses.

Question: {question}
Context: {context}
Answer: {answer}

Evaluate the answer specifically for: {dimension.value.replace('_', ' ').title()}

"""
        
        dimension_instructions = {
            QualityDimension.FACTUAL_ACCURACY: """
Assess whether the answer is factually correct and accurate:
- Are the Excel formulas syntactically correct?
- Are the function names and parameters accurate?
- Is the logic sound and mathematically correct?
- Are there any factual errors or misconceptions?
""",
            QualityDimension.RELEVANCE: """
Assess whether the answer directly addresses the question:
- Does the answer solve the specific problem asked?
- Is the response on-topic and focused?
- Does it address the user's actual needs?
- Are there any irrelevant tangents?
""",
            QualityDimension.COMPLETENESS: """
Assess whether the answer is complete and comprehensive:
- Does it provide a complete solution?
- Are all parts of the question addressed?
- Is sufficient detail provided?
- Are important steps or considerations missing?
""",
            QualityDimension.CLARITY: """
Assess whether the answer is clear and well-explained:
- Is the explanation easy to understand?
- Are the steps clearly outlined?
- Is the language appropriate for the user level?
- Are examples or illustrations provided when helpful?
""",
            QualityDimension.PRACTICALITY: """
Assess whether the answer is practical and usable:
- Can the user actually implement the solution?
- Is the solution efficient and appropriate?
- Are there any practical limitations mentioned?
- Is the solution suitable for the user's context?
"""
        }
        
        prompt = base_prompt + dimension_instructions[dimension]
        
        prompt += """
Provide your assessment in JSON format:
{
    "score": 0.85,          // Float between 0.0 and 1.0
    "confidence": 0.90,     // Your confidence in this assessment (0.0 to 1.0)
    "reasoning": "The answer correctly uses the VLOOKUP function with proper syntax..."
}

Only return the JSON, no other text."""
        
        return prompt
    
    async def _call_judge_model(self, prompt: str) -> Dict[str, Any]:
        """Call the judge model to assess quality"""
        try:
            request_data = {
                "model": self.judge_model,
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an expert Excel consultant and quality assessor. Provide accurate, objective assessments in the requested JSON format."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": 500,
                "temperature": 0.1,
                "top_p": 0.9
            }
            
            response = await self.client.post(
                f"{self.base_url}/chat/completions",
                json=request_data
            )
            response.raise_for_status()
            
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            
            return {
                "success": True,
                "content": content,
                "usage": result.get("usage", {})
            }
            
        except Exception as e:
            logger.error(f"Error calling judge model: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _parse_assessment_response(self, response: str, dimension: QualityDimension) -> QualityScore:
        """Parse the assessment response from the judge model"""
        try:
            # Try to extract JSON from the response
            import re
            
            # Look for JSON in the response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(0)
                assessment_data = json.loads(json_str)
                
                return QualityScore(
                    dimension=dimension,
                    score=float(assessment_data.get("score", 0.5)),
                    confidence=float(assessment_data.get("confidence", 0.5)),
                    reasoning=assessment_data.get("reasoning", "No reasoning provided")
                )
            else:
                # Fallback parsing
                return self._fallback_parse(response, dimension)
                
        except Exception as e:
            logger.error(f"Error parsing assessment response: {e}")
            return QualityScore(
                dimension=dimension,
                score=0.5,
                confidence=0.0,
                reasoning="Failed to parse assessment"
            )
    
    def _fallback_parse(self, response: str, dimension: QualityDimension) -> QualityScore:
        """Fallback parsing when JSON parsing fails"""
        # Simple heuristic-based scoring
        score = 0.5
        confidence = 0.3
        
        positive_words = ["correct", "accurate", "good", "clear", "complete", "excellent"]
        negative_words = ["incorrect", "wrong", "unclear", "incomplete", "poor", "missing"]
        
        response_lower = response.lower()
        
        positive_count = sum(1 for word in positive_words if word in response_lower)
        negative_count = sum(1 for word in negative_words if word in response_lower)
        
        if positive_count > negative_count:
            score = 0.7
        elif negative_count > positive_count:
            score = 0.3
        
        return QualityScore(
            dimension=dimension,
            score=score,
            confidence=confidence,
            reasoning="Fallback assessment based on response content"
        )
    
    def _calculate_overall_score(self, dimension_scores: List[QualityScore]) -> float:
        """Calculate overall quality score with weighted dimensions"""
        if not dimension_scores:
            return 0.0
        
        # Weights for different dimensions
        weights = {
            QualityDimension.FACTUAL_ACCURACY: 0.35,  # Most important
            QualityDimension.RELEVANCE: 0.25,
            QualityDimension.COMPLETENESS: 0.20,
            QualityDimension.CLARITY: 0.15,
            QualityDimension.PRACTICALITY: 0.05
        }
        
        weighted_sum = 0.0
        total_weight = 0.0
        
        for score in dimension_scores:
            weight = weights.get(score.dimension, 0.2)
            weighted_sum += score.score * weight
            total_weight += weight
        
        return weighted_sum / total_weight if total_weight > 0 else 0.0
    
    def _generate_recommendation(self, dimension_scores: List[QualityScore],
                               overall_score: float, validation_passed: bool) -> str:
        """Generate recommendation based on assessment"""
        if validation_passed:
            return "Response meets quality standards and can be delivered to user."
        
        # Find the weakest dimensions
        weak_dimensions = [
            score for score in dimension_scores
            if score.score < 0.6
        ]
        
        if not weak_dimensions:
            return "Response is borderline. Consider minor improvements."
        
        weak_areas = [dim.dimension.value.replace('_', ' ') for dim in weak_dimensions]
        
        return f"Response needs improvement in: {', '.join(weak_areas)}. Consider escalating to higher tier model."
    
    def _update_average_score(self, new_score: float):
        """Update running average score"""
        current_avg = self.stats["average_score"]
        total_assessments = self.stats["total_assessments"]
        
        if total_assessments == 1:
            self.stats["average_score"] = new_score
        else:
            self.stats["average_score"] = (
                (current_avg * (total_assessments - 1) + new_score) / total_assessments
            )
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get judge service statistics"""
        return {
            "stats": self.stats.copy(),
            "quality_thresholds": self.quality_thresholds.copy(),
            "judge_model": self.judge_model,
            "supported_dimensions": [dim.value for dim in QualityDimension],
            "timestamp": datetime.now().isoformat()
        }
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

# Singleton instance
_llm_judge_service = None

async def get_llm_judge_service() -> LLMJudgeService:
    """Get singleton LLM judge service instance"""
    global _llm_judge_service
    if _llm_judge_service is None:
        _llm_judge_service = LLMJudgeService()
    return _llm_judge_service

async def cleanup_llm_judge_service():
    """Clean up LLM judge service"""
    global _llm_judge_service
    if _llm_judge_service:
        await _llm_judge_service.close()
        _llm_judge_service = None