"""
Bot Detection Package - Advanced Reddit Bot Detection System

This package provides comprehensive bot detection capabilities for Reddit data collection,
implementing multiple layers of detection for maximum accuracy.
"""

from .advanced_bot_detector import AdvancedBotDetector, BotDetectionResult, BotType, is_bot_response
from .behavioral_bot_detector import BehavioralBotDetector, BehavioralBotResult, BehavioralBotType, behavioral_detector

__all__ = ['AdvancedBotDetector', 'BotDetectionResult', 'BotType', 'is_bot_response', 
           'BehavioralBotDetector', 'BehavioralBotResult', 'BehavioralBotType', 'behavioral_detector']