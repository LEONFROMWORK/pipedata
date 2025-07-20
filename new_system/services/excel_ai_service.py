"""
Excel AI Service with Multi-Tier LLM System
Integrates Mistral Small 3.1, Llama 4 Maverick, and GPT-4.1 Mini
"""
import httpx
import logging
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import asyncio
from config import Config

logger = logging.getLogger('excel_ai_service')

class ModelTier(Enum):
    """Model tier levels for escalation"""
    TIER_1 = "tier_1"  # Fast, basic problems
    TIER_2 = "tier_2"  # Complex problems
    TIER_3 = "tier_3"  # Highest difficulty

@dataclass
class ModelConfig:
    """Configuration for each model"""
    model_id: str
    name: str
    tier: ModelTier
    input_price: float  # $ per 1M tokens
    output_price: float  # $ per 1M tokens
    context_window: int
    specialties: List[str]
    quality_threshold: float  # Minimum quality score to accept

class ExcelAIService:
    """Multi-tier Excel AI service with OpenRouter integration"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or Config.OPENROUTER_API_KEY
        self.base_url = "https://openrouter.ai/api/v1"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://excel-qa-system.com",
            "X-Title": "Excel QA System"
        }
        self.client = httpx.AsyncClient(headers=self.headers, timeout=60.0)
        
        # Configure the 3 selected models
        self.models = {
            ModelTier.TIER_1: ModelConfig(
                model_id="mistralai/mistral-small-3.1",
                name="Mistral Small 3.1",
                tier=ModelTier.TIER_1,
                input_price=0.10,
                output_price=0.30,
                context_window=128000,
                specialties=["fast_response", "chart_analysis", "basic_formulas"],
                quality_threshold=0.70
            ),
            ModelTier.TIER_2: ModelConfig(
                model_id="meta-llama/llama-4-maverick",
                name="Llama 4 Maverick",
                tier=ModelTier.TIER_2,
                input_price=0.23,
                output_price=0.85,
                context_window=1000000,
                specialties=["multimodal", "complex_reasoning", "cost_efficiency"],
                quality_threshold=0.80
            ),
            ModelTier.TIER_3: ModelConfig(
                model_id="openai/gpt-4.1-mini",
                name="GPT-4.1 Mini",
                tier=ModelTier.TIER_3,
                input_price=0.40,
                output_price=1.60,
                context_window=200000,
                specialties=["highest_accuracy", "complex_problems", "final_validation"],
                quality_threshold=0.90
            )
        }
        
        # Usage tracking
        self.usage_stats = {
            "total_requests": 0,
            "tier_usage": {tier.value: 0 for tier in ModelTier},
            "total_cost": 0.0,
            "escalations": 0
        }
    
    def _assess_question_complexity(self, question: str, context: str = "") -> ModelTier:
        """Assess question complexity to determine initial tier"""
        question_lower = question.lower()
        context_lower = context.lower()
        full_text = f"{question_lower} {context_lower}"
        
        # Keywords indicating complexity levels
        tier_3_keywords = [
            "vba", "macro", "complex formula", "array formula", "multiple conditions",
            "pivot table", "advanced function", "nested if", "index match",
            "power query", "data model", "dynamic array"
        ]
        
        tier_2_keywords = [
            "vlookup", "hlookup", "xlookup", "sumifs", "countifs", "conditional",
            "chart", "graph", "visualization", "multiple sheets", "reference",
            "function combination", "lookup", "filter"
        ]
        
        # Check for tier 3 indicators
        if any(keyword in full_text for keyword in tier_3_keywords):
            return ModelTier.TIER_3
        
        # Check for tier 2 indicators
        if any(keyword in full_text for keyword in tier_2_keywords):
            return ModelTier.TIER_2
        
        # Length-based assessment
        if len(full_text) > 500:
            return ModelTier.TIER_2
        elif len(full_text) > 200:
            return ModelTier.TIER_1
        
        # Default to tier 1 for simple questions
        return ModelTier.TIER_1
    
    async def _call_model(self, tier: ModelTier, prompt: str, images: List[str] = None) -> Dict[str, Any]:
        """Call a specific model tier"""
        model_config = self.models[tier]
        
        try:
            # Prepare messages
            messages = [
                {
                    "role": "system",
                    "content": self._get_system_prompt(tier)
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
            
            # Add images if provided (for multimodal models)
            if images and tier in [ModelTier.TIER_1, ModelTier.TIER_2, ModelTier.TIER_3]:
                # All selected models support multimodal input
                content_parts = [{"type": "text", "text": prompt}]
                
                # Add each image
                for image in images:
                    if image.startswith('data:'):
                        # Base64 encoded image
                        content_parts.append({
                            "type": "image_url", 
                            "image_url": {"url": image}
                        })
                    else:
                        # URL or file path
                        content_parts.append({
                            "type": "image_url", 
                            "image_url": {"url": image}
                        })
                
                messages[-1]["content"] = content_parts
            
            # API request
            request_data = {
                "model": model_config.model_id,
                "messages": messages,
                "max_tokens": 2000,
                "temperature": 0.1,
                "top_p": 0.9
            }
            
            logger.info(f"Calling {model_config.name} ({tier.value})")
            
            response = await self.client.post(
                f"{self.base_url}/chat/completions",
                json=request_data
            )
            response.raise_for_status()
            
            result = response.json()
            
            # Extract response
            message = result["choices"][0]["message"]["content"]
            usage = result.get("usage", {})
            
            # Calculate cost
            input_tokens = usage.get("prompt_tokens", 0)
            output_tokens = usage.get("completion_tokens", 0)
            cost = self._calculate_cost(tier, input_tokens, output_tokens)
            
            # Update usage stats
            self.usage_stats["total_requests"] += 1
            self.usage_stats["tier_usage"][tier.value] += 1
            self.usage_stats["total_cost"] += cost
            
            return {
                "success": True,
                "response": message,
                "model": model_config.name,
                "tier": tier.value,
                "cost": cost,
                "tokens": {
                    "input": input_tokens,
                    "output": output_tokens,
                    "total": input_tokens + output_tokens
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error calling {model_config.name}: {e}")
            return {
                "success": False,
                "error": f"HTTP {e.response.status_code}: {e.response.text}",
                "model": model_config.name,
                "tier": tier.value
            }
        except Exception as e:
            logger.error(f"Error calling {model_config.name}: {e}")
            return {
                "success": False,
                "error": str(e),
                "model": model_config.name,
                "tier": tier.value
            }
    
    def _get_system_prompt(self, tier: ModelTier) -> str:
        """Get system prompt for each tier"""
        base_prompt = """You are an Excel expert AI assistant. Your role is to help users solve Excel problems with accurate, practical solutions.

Guidelines:
1. Provide clear, step-by-step solutions
2. Include exact Excel formulas when applicable
3. Explain the logic behind your solutions
4. Consider different Excel versions when relevant
5. Prioritize accuracy over speed
6. If unsure, indicate limitations clearly"""
        
        if tier == ModelTier.TIER_1:
            return base_prompt + """

As a Tier 1 assistant, focus on:
- Quick, accurate responses for common Excel problems
- Basic formulas and functions
- Chart and graph analysis
- Simple data manipulation tasks
- Clear, concise explanations"""
        
        elif tier == ModelTier.TIER_2:
            return base_prompt + """

As a Tier 2 assistant, handle:
- Complex multi-step problems
- Advanced formula combinations
- Multimodal analysis (images + text)
- Multiple sheet operations
- Advanced data analysis
- Detailed explanations with examples"""
        
        else:  # TIER_3
            return base_prompt + """

As a Tier 3 assistant, provide:
- Highest accuracy solutions for complex problems
- VBA and macro solutions when needed
- Advanced Excel features (Power Query, etc.)
- Complex business logic implementation
- Comprehensive error handling
- Production-ready solutions"""
    
    def _calculate_cost(self, tier: ModelTier, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost for a model call"""
        model_config = self.models[tier]
        input_cost = (input_tokens / 1000000) * model_config.input_price
        output_cost = (output_tokens / 1000000) * model_config.output_price
        return input_cost + output_cost
    
    async def _validate_response_quality(self, response: str, question: str, context: str = "", 
                                         tier: str = "tier_1") -> float:
        """Validate response quality using LLM-as-a-Judge"""
        try:
            # Import here to avoid circular imports
            from services.llm_judge_service import get_llm_judge_service
            
            # Get LLM judge service
            judge_service = await get_llm_judge_service()
            
            # Assess quality
            assessment = await judge_service.assess_quality(
                question=question,
                context=context,
                answer=response,
                tier=tier
            )
            
            return assessment.overall_score
            
        except Exception as e:
            logger.error(f"Error in LLM-as-a-Judge validation: {e}")
            # Fallback to basic validation
            return await self._basic_quality_validation(response, question)
    
    async def _basic_quality_validation(self, response: str, question: str) -> float:
        """Basic quality validation as fallback"""
        quality_score = 0.5  # Base score
        
        # Check for Excel-specific content
        excel_keywords = ["=", "formula", "function", "cell", "sheet", "workbook"]
        if any(keyword in response.lower() for keyword in excel_keywords):
            quality_score += 0.2
        
        # Check for step-by-step explanation
        if any(indicator in response.lower() for indicator in ["step", "first", "then", "next"]):
            quality_score += 0.15
        
        # Check for completeness
        if len(response) > 100:
            quality_score += 0.1
        
        # Check for code examples
        if "=" in response and any(func in response.upper() for func in ["IF", "SUM", "COUNT", "LOOKUP"]):
            quality_score += 0.15
        
        return min(quality_score, 1.0)
    
    def _build_rag_prompt(self, question: str, context: str, vector_context: str, tier: ModelTier) -> str:
        """Build RAG-enhanced prompt tailored for each tier"""
        base_prompt = f"""Question: {question}

Context: {context}"""
        
        if vector_context:
            base_prompt += f"""

Related Knowledge from Database:
{vector_context}"""
        
        # Tier-specific instructions
        if tier == ModelTier.TIER_1:
            base_prompt += """

Instructions: Provide a quick, accurate solution focusing on:
- Direct answer with exact Excel formula
- Clear step-by-step explanation
- Use the related knowledge to ensure accuracy
- Keep response concise but complete"""
        
        elif tier == ModelTier.TIER_2:
            base_prompt += """

Instructions: Provide a comprehensive solution focusing on:
- Detailed analysis of the problem
- Multiple solution approaches if applicable
- Consider edge cases and error handling
- Use related knowledge to enhance your solution
- Include practical examples"""
        
        else:  # TIER_3
            base_prompt += """

Instructions: Provide the most accurate, production-ready solution:
- Complete analysis with multiple approaches
- Error handling and edge case considerations
- Performance implications
- Alternative solutions comparison
- Best practices and recommendations
- Use all available knowledge to ensure perfection"""
        
        return base_prompt
    
    async def solve_excel_problem(self, question: str, context: str = "", images: List[str] = None, 
                                 vector_context: str = "") -> Dict[str, Any]:
        """Main method to solve Excel problems with multi-tier approach"""
        try:
            # Assess initial complexity
            initial_tier = self._assess_question_complexity(question, context)
            
            # Prepare enhanced prompt with vector context (RAG integration)
            enhanced_prompt = self._build_rag_prompt(question, context, vector_context, initial_tier)
            
            # Try each tier starting from assessed level
            tiers_to_try = [initial_tier]
            
            # Add escalation tiers
            if initial_tier == ModelTier.TIER_1:
                tiers_to_try.extend([ModelTier.TIER_2, ModelTier.TIER_3])
            elif initial_tier == ModelTier.TIER_2:
                tiers_to_try.append(ModelTier.TIER_3)
            
            best_response = None
            
            for tier in tiers_to_try:
                logger.info(f"Trying {tier.value} for question: {question[:50]}...")
                
                response = await self._call_model(tier, enhanced_prompt, images)
                
                if not response["success"]:
                    logger.warning(f"{tier.value} failed: {response.get('error', 'Unknown error')}")
                    continue
                
                # Validate response quality using LLM-as-a-Judge
                quality_score = await self._validate_response_quality(
                    response["response"], 
                    question, 
                    context, 
                    tier.value
                )
                response["quality_score"] = quality_score
                
                logger.info(f"{tier.value} quality score: {quality_score:.2f}")
                
                # Check if quality meets threshold
                if quality_score >= self.models[tier].quality_threshold:
                    logger.info(f"{tier.value} response accepted (quality: {quality_score:.2f})")
                    best_response = response
                    break
                else:
                    logger.info(f"{tier.value} response below threshold, escalating...")
                    if tier != ModelTier.TIER_3:
                        self.usage_stats["escalations"] += 1
                    best_response = response  # Keep as fallback
            
            if best_response:
                return {
                    "success": True,
                    "solution": best_response["response"],
                    "metadata": {
                        "model_used": best_response["model"],
                        "tier": best_response["tier"],
                        "cost": best_response["cost"],
                        "quality_score": best_response["quality_score"],
                        "tokens": best_response["tokens"],
                        "timestamp": best_response["timestamp"],
                        "escalated": initial_tier.value != best_response["tier"]
                    }
                }
            else:
                return {
                    "success": False,
                    "error": "All models failed to provide acceptable response",
                    "metadata": {
                        "initial_tier": initial_tier.value,
                        "tiers_tried": [t.value for t in tiers_to_try]
                    }
                }
                
        except Exception as e:
            logger.error(f"Error in solve_excel_problem: {e}")
            return {
                "success": False,
                "error": str(e),
                "metadata": {
                    "timestamp": datetime.now().isoformat()
                }
            }
    
    async def get_usage_statistics(self) -> Dict[str, Any]:
        """Get usage statistics"""
        return {
            "usage_stats": self.usage_stats.copy(),
            "model_configurations": {
                tier.value: {
                    "model_id": config.model_id,
                    "name": config.name,
                    "input_price": config.input_price,
                    "output_price": config.output_price,
                    "quality_threshold": config.quality_threshold
                }
                for tier, config in self.models.items()
            },
            "timestamp": datetime.now().isoformat()
        }
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

# Singleton instance
_excel_ai_service = None

async def get_excel_ai_service() -> ExcelAIService:
    """Get singleton Excel AI service instance"""
    global _excel_ai_service
    if _excel_ai_service is None:
        _excel_ai_service = ExcelAIService()
    return _excel_ai_service

async def close_excel_ai_service():
    """Close Excel AI service"""
    global _excel_ai_service
    if _excel_ai_service:
        await _excel_ai_service.close()
        _excel_ai_service = None