"""
OpenRouter API Service for AI Cost Monitoring and Model Management
"""
import httpx
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import asyncio
from config import Config

logger = logging.getLogger('openrouter_service')

class OpenRouterService:
    """OpenRouter API service for cost monitoring and model management"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or Config.OPENROUTER_API_KEY
        self.base_url = "https://openrouter.ai/api/v1"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://excel-qa-dashboard.com",
            "X-Title": "Excel QA Dashboard"
        }
        self.client = httpx.AsyncClient(headers=self.headers, timeout=30.0)
        self.current_model = Config.get_current_model()
        
    async def get_account_balance(self) -> Dict[str, Any]:
        """Get current account balance and limits"""
        try:
            response = await self.client.get(f"{self.base_url}/auth/key")
            response.raise_for_status()
            
            data = response.json()
            
            # Extract balance information
            balance_info = {
                "balance": data.get("data", {}).get("balance", 0),
                "usage": data.get("data", {}).get("usage", 0),
                "limit": data.get("data", {}).get("limit", 0),
                "is_free_tier": data.get("data", {}).get("is_free_tier", False),
                "rate_limit": data.get("data", {}).get("rate_limit", {}),
                "last_updated": datetime.now().isoformat()
            }
            
            logger.info(f"Account balance retrieved: ${balance_info['balance']:.4f}")
            return balance_info
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error getting balance: {e}")
            return {"error": f"HTTP {e.response.status_code}: {e.response.text}"}
        except Exception as e:
            logger.error(f"Error getting balance: {e}")
            return {"error": str(e)}
    
    async def get_usage_stats(self, days: int = 7) -> Dict[str, Any]:
        """Get usage statistics for the last N days"""
        try:
            # OpenRouter doesn't have a direct usage stats endpoint
            # We'll simulate this by tracking our own usage
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # For now, return mock data structure
            # In production, you would track this in your database
            usage_stats = {
                "period": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat(),
                    "days": days
                },
                "total_cost": 0.0,
                "total_requests": 0,
                "models_used": {},
                "daily_usage": [],
                "top_models": []
            }
            
            return usage_stats
            
        except Exception as e:
            logger.error(f"Error getting usage stats: {e}")
            return {"error": str(e)}
    
    async def get_available_models(self) -> List[Dict[str, Any]]:
        """Get list of available models with pricing"""
        try:
            response = await self.client.get(f"{self.base_url}/models")
            response.raise_for_status()
            
            data = response.json()
            models = data.get("data", [])
            
            # Filter for commonly used models and add categorization
            popular_models = [
                "anthropic/claude-3.5-sonnet",
                "anthropic/claude-3-haiku",
                "openai/gpt-4o",
                "openai/gpt-4o-mini",
                "openai/gpt-3.5-turbo",
                "meta-llama/llama-3.1-70b-instruct",
                "google/gemini-pro",
                "cohere/command-r-plus"
            ]
            
            # Format models for dashboard
            formatted_models = []
            for model in models:
                model_id = model.get("id", "")
                
                # Add category
                category = "Other"
                if "anthropic" in model_id:
                    category = "Anthropic"
                elif "openai" in model_id:
                    category = "OpenAI"
                elif "google" in model_id:
                    category = "Google"
                elif "meta" in model_id:
                    category = "Meta"
                elif "cohere" in model_id:
                    category = "Cohere"
                
                formatted_model = {
                    "id": model_id,
                    "name": model.get("name", model_id),
                    "category": category,
                    "pricing": model.get("pricing", {}),
                    "context_length": model.get("context_length", 0),
                    "is_popular": model_id in popular_models,
                    "description": model.get("description", "")
                }
                
                formatted_models.append(formatted_model)
            
            # Sort by popularity and category
            formatted_models.sort(key=lambda x: (not x["is_popular"], x["category"], x["name"]))
            
            logger.info(f"Retrieved {len(formatted_models)} available models")
            return formatted_models
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error getting models: {e}")
            return []
        except Exception as e:
            logger.error(f"Error getting models: {e}")
            return []
    
    async def estimate_cost(self, model_id: str, prompt_tokens: int, completion_tokens: int) -> Dict[str, Any]:
        """Estimate cost for a request"""
        try:
            models = await self.get_available_models()
            
            # Find the model
            model = next((m for m in models if m["id"] == model_id), None)
            
            if not model:
                return {"error": "Model not found"}
            
            pricing = model.get("pricing", {})
            prompt_cost = float(pricing.get("prompt", "0")) * prompt_tokens / 1000000
            completion_cost = float(pricing.get("completion", "0")) * completion_tokens / 1000000
            
            total_cost = prompt_cost + completion_cost
            
            return {
                "model_id": model_id,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "prompt_cost": prompt_cost,
                "completion_cost": completion_cost,
                "total_cost": total_cost,
                "pricing": pricing
            }
            
        except Exception as e:
            logger.error(f"Error estimating cost: {e}")
            return {"error": str(e)}
    
    async def log_usage(self, model_id: str, prompt_tokens: int, completion_tokens: int, 
                       cost: float, task_type: str = "unknown") -> None:
        """Log usage for tracking (would be stored in database)"""
        try:
            usage_entry = {
                "timestamp": datetime.now().isoformat(),
                "model_id": model_id,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
                "cost": cost,
                "task_type": task_type
            }
            
            # In production, this would be stored in a database
            # For now, just log it
            logger.info(f"AI Usage: {model_id} - {task_type} - ${cost:.6f}")
            
        except Exception as e:
            logger.error(f"Error logging usage: {e}")
    
    async def get_model_info(self, model_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific model"""
        try:
            models = await self.get_available_models()
            model = next((m for m in models if m["id"] == model_id), None)
            
            if not model:
                return {"error": "Model not found"}
            
            return model
            
        except Exception as e:
            logger.error(f"Error getting model info: {e}")
            return {"error": str(e)}
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test OpenRouter API connection"""
        try:
            response = await self.client.get(f"{self.base_url}/auth/key")
            response.raise_for_status()
            
            return {
                "status": "success",
                "message": "OpenRouter API connection successful",
                "timestamp": datetime.now().isoformat()
            }
            
        except httpx.HTTPStatusError as e:
            return {
                "status": "error",
                "message": f"HTTP {e.response.status_code}: {e.response.text}",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

# Singleton instance
_openrouter_service = None

async def get_openrouter_service() -> OpenRouterService:
    """Get singleton OpenRouter service instance"""
    global _openrouter_service
    if _openrouter_service is None:
        _openrouter_service = OpenRouterService()
    return _openrouter_service

async def close_openrouter_service():
    """Close OpenRouter service"""
    global _openrouter_service
    if _openrouter_service:
        await _openrouter_service.close()
        _openrouter_service = None