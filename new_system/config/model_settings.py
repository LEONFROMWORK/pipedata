"""
Model Settings Management for BigData System
Synchronizes with Dashboard UI model selection
"""
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
import logging
from datetime import datetime

logger = logging.getLogger('model_settings')

class ModelSettings:
    """Manage AI model settings for BigData system"""
    
    def __init__(self, settings_file: str = None):
        if settings_file:
            self.settings_file = Path(settings_file)
        else:
            # Default path to dashboard settings
            dashboard_dir = Path(__file__).parent.parent.parent / "dashboard-ui"
            self.settings_file = dashboard_dir / "data" / "settings.json"
        
        self.settings_file.parent.mkdir(parents=True, exist_ok=True)
        
        self.default_settings = {
            "model": "anthropic/claude-3.5-sonnet",
            "updated_at": None,
            "model_config": {
                "temperature": 0.1,
                "max_tokens": 1000,
                "top_p": 0.9
            }
        }
    
    def get_current_model(self) -> str:
        """Get currently selected model"""
        try:
            settings = self.load_settings()
            return settings.get("model", self.default_settings["model"])
        except Exception as e:
            logger.warning(f"Failed to get current model, using default: {e}")
            return self.default_settings["model"]
    
    def get_model_config(self) -> Dict[str, Any]:
        """Get model configuration parameters"""
        try:
            settings = self.load_settings()
            return settings.get("model_config", self.default_settings["model_config"])
        except Exception as e:
            logger.warning(f"Failed to get model config, using default: {e}")
            return self.default_settings["model_config"]
    
    def load_settings(self) -> Dict[str, Any]:
        """Load settings from file"""
        if not self.settings_file.exists():
            return self.default_settings.copy()
        
        try:
            with open(self.settings_file, 'r', encoding='utf-8') as f:
                settings = json.load(f)
                
            # Merge with defaults to ensure all keys exist
            merged_settings = self.default_settings.copy()
            merged_settings.update(settings)
            
            return merged_settings
            
        except Exception as e:
            logger.error(f"Error loading settings: {e}")
            return self.default_settings.copy()
    
    def save_settings(self, settings: Dict[str, Any]) -> None:
        """Save settings to file"""
        try:
            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(settings, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"Error saving settings: {e}")
            raise
    
    def update_model(self, model_id: str) -> None:
        """Update the current model"""
        try:
            settings = self.load_settings()
            settings["model"] = model_id
            settings["updated_at"] = datetime.now().isoformat()
            
            self.save_settings(settings)
            logger.info(f"Model updated to: {model_id}")
            
        except Exception as e:
            logger.error(f"Error updating model: {e}")
            raise
    
    def get_openrouter_config(self) -> Dict[str, Any]:
        """Get OpenRouter API configuration for current model"""
        current_model = self.get_current_model()
        model_config = self.get_model_config()
        
        return {
            "model": current_model,
            "temperature": model_config.get("temperature", 0.1),
            "max_tokens": model_config.get("max_tokens", 1000),
            "top_p": model_config.get("top_p", 0.9),
            "stream": False
        }
    
    def is_model_changed(self, last_check: Optional[str] = None) -> bool:
        """Check if model has been changed since last check"""
        try:
            settings = self.load_settings()
            current_updated = settings.get("updated_at")
            
            if not current_updated or not last_check:
                return True
                
            return current_updated != last_check
            
        except Exception as e:
            logger.warning(f"Error checking model changes: {e}")
            return True
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about current model"""
        settings = self.load_settings()
        
        return {
            "model": settings.get("model", self.default_settings["model"]),
            "updated_at": settings.get("updated_at"),
            "config": settings.get("model_config", self.default_settings["model_config"]),
            "settings_file": str(self.settings_file)
        }

# Singleton instance
_model_settings = None

def get_model_settings() -> ModelSettings:
    """Get singleton model settings instance"""
    global _model_settings
    if _model_settings is None:
        _model_settings = ModelSettings()
    return _model_settings

def get_current_model() -> str:
    """Quick access to current model"""
    return get_model_settings().get_current_model()

def get_openrouter_config() -> Dict[str, Any]:
    """Quick access to OpenRouter configuration"""
    return get_model_settings().get_openrouter_config()

if __name__ == "__main__":
    # Test the model settings
    from datetime import datetime
    
    settings = get_model_settings()
    print(f"Current model: {settings.get_current_model()}")
    print(f"Model config: {settings.get_model_config()}")
    print(f"OpenRouter config: {settings.get_openrouter_config()}")
    print(f"Model info: {settings.get_model_info()}")