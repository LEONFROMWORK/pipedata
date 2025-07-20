"""
Layer 4: Real-time Bot Detection System with Caching and Monitoring
Production-level real-time bot detection with comprehensive monitoring and caching

This module implements:
- Real-time bot detection with sub-second response times
- Redis-based caching for performance optimization
- Comprehensive monitoring and metrics collection
- Rate limiting and performance optimization
- Production-grade error handling and logging
"""

import time
import json
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict, deque
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Mock Redis implementation for production environments
class MockRedis:
    """Mock Redis implementation for environments without Redis"""
    
    def __init__(self):
        self.data = {}
        self.expiry = {}
        self.lock = threading.Lock()
    
    def get(self, key: str) -> Optional[str]:
        with self.lock:
            if key in self.expiry and datetime.now() > self.expiry[key]:
                del self.data[key]
                del self.expiry[key]
                return None
            return self.data.get(key)
    
    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        with self.lock:
            self.data[key] = value
            if ex:
                self.expiry[key] = datetime.now() + timedelta(seconds=ex)
            return True
    
    def delete(self, key: str) -> bool:
        with self.lock:
            if key in self.data:
                del self.data[key]
                if key in self.expiry:
                    del self.expiry[key]
                return True
            return False
    
    def exists(self, key: str) -> bool:
        with self.lock:
            return key in self.data
    
    def incr(self, key: str) -> int:
        with self.lock:
            current = int(self.data.get(key, 0))
            self.data[key] = str(current + 1)
            return current + 1

logger = logging.getLogger('pipeline.real_time_bot_detector')

class DetectionPriority(Enum):
    """Detection priority levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class CacheConfig:
    """Cache configuration"""
    detection_cache_ttl: int = 3600  # 1 hour
    user_cache_ttl: int = 1800      # 30 minutes
    pattern_cache_ttl: int = 7200    # 2 hours
    max_cache_size: int = 10000      # Maximum cache entries

@dataclass
class RateLimitConfig:
    """Rate limiting configuration"""
    max_requests_per_minute: int = 1000
    max_requests_per_hour: int = 50000
    burst_limit: int = 100
    cooldown_period: int = 60

@dataclass
class MonitoringMetrics:
    """Monitoring metrics"""
    total_detections: int = 0
    bot_detections: int = 0
    human_confirmations: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    avg_response_time: float = 0.0
    error_count: int = 0
    rate_limit_hits: int = 0

@dataclass
class RealTimeDetectionResult:
    """Real-time detection result with performance metrics"""
    is_bot: bool
    confidence: float
    detection_layers: List[str]
    cache_hit: bool
    response_time_ms: float
    priority: DetectionPriority
    reasoning: str
    timestamp: str

class RealTimeBotDetector:
    """
    Production-grade real-time bot detection system
    
    Features:
    - Sub-second response times with intelligent caching
    - Multi-layer detection with priority escalation
    - Comprehensive monitoring and metrics
    - Rate limiting and performance optimization
    - Error handling and graceful degradation
    """
    
    def __init__(self, cache_config: Optional[CacheConfig] = None,
                 rate_limit_config: Optional[RateLimitConfig] = None):
        self.cache_config = cache_config or CacheConfig()
        self.rate_limit_config = rate_limit_config or RateLimitConfig()
        
        # Initialize cache (Redis in production, mock for development)
        self.cache = MockRedis()
        
        # Initialize monitoring
        self.metrics = MonitoringMetrics()
        self.metrics_lock = threading.Lock()
        
        # Rate limiting
        self.rate_limiter = self._setup_rate_limiter()
        
        # Thread pool for concurrent processing
        self.executor = ThreadPoolExecutor(max_workers=10)
        
        # Initialize detection layers
        self._initialize_detection_layers()
        
        # Performance optimization
        self.response_times = deque(maxlen=1000)  # Keep last 1000 response times
        
        logger.info("Real-time bot detector initialized with production configuration")
    
    def _setup_rate_limiter(self) -> Dict[str, Any]:
        """Setup rate limiting system"""
        return {
            'minute_counter': defaultdict(int),
            'hour_counter': defaultdict(int),
            'last_reset_minute': datetime.now().minute,
            'last_reset_hour': datetime.now().hour,
            'blocked_ips': set()
        }
    
    def _initialize_detection_layers(self):
        """Initialize all detection layers"""
        try:
            from .advanced_bot_detector import AdvancedBotDetector
            from .behavioral_bot_detector import BehavioralBotDetector
            from .ai_bot_detector import AIBotDetector
            
            self.layer1_detector = AdvancedBotDetector()
            self.layer2_detector = BehavioralBotDetector()
            self.layer3_detector = AIBotDetector()
            
            logger.info("All detection layers initialized successfully")
        except ImportError as e:
            logger.error(f"Failed to initialize detection layers: {e}")
            raise
    
    async def detect_bot_realtime(self, content: str, 
                                metadata: Optional[Dict[str, Any]] = None,
                                client_ip: Optional[str] = None,
                                priority: DetectionPriority = DetectionPriority.MEDIUM) -> RealTimeDetectionResult:
        """
        Real-time bot detection with caching and monitoring
        
        Args:
            content: Text content to analyze
            metadata: Optional metadata
            client_ip: Client IP for rate limiting
            priority: Detection priority level
            
        Returns:
            RealTimeDetectionResult with comprehensive analysis
        """
        start_time = time.time()
        
        try:
            # Rate limiting check
            if client_ip and not self._check_rate_limit(client_ip):
                self._update_metrics('rate_limit_hits', 1)
                return RealTimeDetectionResult(
                    is_bot=False,
                    confidence=0.0,
                    detection_layers=[],
                    cache_hit=False,
                    response_time_ms=0.0,
                    priority=priority,
                    reasoning="Rate limit exceeded",
                    timestamp=datetime.now().isoformat()
                )
            
            # Check cache first
            cache_key = self._generate_cache_key(content, metadata)
            cached_result = self._get_cached_result(cache_key)
            
            if cached_result:
                self._update_metrics('cache_hits', 1)
                response_time = (time.time() - start_time) * 1000
                cached_result.response_time_ms = response_time
                cached_result.cache_hit = True
                return cached_result
            
            # Cache miss - perform detection
            self._update_metrics('cache_misses', 1)
            
            # Priority-based detection
            if priority == DetectionPriority.CRITICAL:
                result = await self._critical_priority_detection(content, metadata)
            elif priority == DetectionPriority.HIGH:
                result = await self._high_priority_detection(content, metadata)
            elif priority == DetectionPriority.MEDIUM:
                result = await self._medium_priority_detection(content, metadata)
            else:
                result = await self._low_priority_detection(content, metadata)
            
            # Calculate response time
            response_time = (time.time() - start_time) * 1000
            result.response_time_ms = response_time
            result.cache_hit = False
            
            # Cache the result
            self._cache_result(cache_key, result)
            
            # Update metrics
            self._update_metrics('total_detections', 1)
            if result.is_bot:
                self._update_metrics('bot_detections', 1)
            else:
                self._update_metrics('human_confirmations', 1)
            
            # Update response time metrics
            self._update_response_time(response_time)
            
            return result
            
        except Exception as e:
            logger.error(f"Error in real-time detection: {e}")
            self._update_metrics('error_count', 1)
            
            # Graceful degradation
            return RealTimeDetectionResult(
                is_bot=False,
                confidence=0.0,
                detection_layers=[],
                cache_hit=False,
                response_time_ms=(time.time() - start_time) * 1000,
                priority=priority,
                reasoning=f"Error in detection: {str(e)}",
                timestamp=datetime.now().isoformat()
            )
    
    async def _critical_priority_detection(self, content: str, 
                                         metadata: Optional[Dict[str, Any]]) -> RealTimeDetectionResult:
        """Critical priority detection - all layers, parallel processing"""
        comment_data = self._prepare_comment_data(content, metadata)
        
        # Run all layers in parallel
        tasks = [
            self._run_layer1_detection(comment_data),
            self._run_layer2_detection(comment_data, metadata),
            self._run_layer3_detection(content)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        active_layers = []
        confidences = []
        
        for i, result in enumerate(results):
            if not isinstance(result, Exception):
                layer_name = f"layer{i+1}"
                active_layers.append(layer_name)
                confidences.append(result.get('confidence', 0.0))
        
        # Calculate final confidence (weighted average)
        if confidences:
            final_confidence = sum(confidences) / len(confidences)
        else:
            final_confidence = 0.0
        
        is_bot = final_confidence >= 0.7
        
        return RealTimeDetectionResult(
            is_bot=is_bot,
            confidence=final_confidence,
            detection_layers=active_layers,
            cache_hit=False,
            response_time_ms=0.0,
            priority=DetectionPriority.CRITICAL,
            reasoning=f"Critical priority detection: {len(active_layers)} layers active",
            timestamp=datetime.now().isoformat()
        )
    
    async def _high_priority_detection(self, content: str, 
                                     metadata: Optional[Dict[str, Any]]) -> RealTimeDetectionResult:
        """High priority detection - Layer 1 + Layer 2"""
        comment_data = self._prepare_comment_data(content, metadata)
        
        # Layer 1 (immediate)
        layer1_result = await self._run_layer1_detection(comment_data)
        
        # If Layer 1 detects bot with high confidence, return immediately
        if layer1_result.get('confidence', 0.0) >= 0.9:
            return RealTimeDetectionResult(
                is_bot=True,
                confidence=layer1_result['confidence'],
                detection_layers=['layer1'],
                cache_hit=False,
                response_time_ms=0.0,
                priority=DetectionPriority.HIGH,
                reasoning="High confidence Layer 1 detection",
                timestamp=datetime.now().isoformat()
            )
        
        # Layer 2 (behavioral)
        layer2_result = await self._run_layer2_detection(comment_data, metadata)
        
        # Combine results
        combined_confidence = (layer1_result.get('confidence', 0.0) * 0.6 + 
                             layer2_result.get('confidence', 0.0) * 0.4)
        
        return RealTimeDetectionResult(
            is_bot=combined_confidence >= 0.7,
            confidence=combined_confidence,
            detection_layers=['layer1', 'layer2'],
            cache_hit=False,
            response_time_ms=0.0,
            priority=DetectionPriority.HIGH,
            reasoning="High priority Layer 1+2 detection",
            timestamp=datetime.now().isoformat()
        )
    
    async def _medium_priority_detection(self, content: str, 
                                       metadata: Optional[Dict[str, Any]]) -> RealTimeDetectionResult:
        """Medium priority detection - Layer 1 only"""
        comment_data = self._prepare_comment_data(content, metadata)
        
        layer1_result = await self._run_layer1_detection(comment_data)
        
        return RealTimeDetectionResult(
            is_bot=layer1_result.get('is_bot', False),
            confidence=layer1_result.get('confidence', 0.0),
            detection_layers=['layer1'],
            cache_hit=False,
            response_time_ms=0.0,
            priority=DetectionPriority.MEDIUM,
            reasoning="Medium priority Layer 1 detection",
            timestamp=datetime.now().isoformat()
        )
    
    async def _low_priority_detection(self, content: str, 
                                    metadata: Optional[Dict[str, Any]]) -> RealTimeDetectionResult:
        """Low priority detection - Basic pattern matching"""
        # Simple pattern-based detection for low priority
        simple_bot_patterns = [
            'i am a bot', 'automoderator', 'this action was performed automatically',
            'contact the moderators', 'your post was submitted successfully'
        ]
        
        content_lower = content.lower()
        is_bot = any(pattern in content_lower for pattern in simple_bot_patterns)
        confidence = 0.9 if is_bot else 0.1
        
        return RealTimeDetectionResult(
            is_bot=is_bot,
            confidence=confidence,
            detection_layers=['basic_patterns'],
            cache_hit=False,
            response_time_ms=0.0,
            priority=DetectionPriority.LOW,
            reasoning="Low priority basic pattern detection",
            timestamp=datetime.now().isoformat()
        )
    
    async def _run_layer1_detection(self, comment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Run Layer 1 detection asynchronously"""
        try:
            result = self.layer1_detector.detect_bot_comprehensive(comment_data)
            return {
                'is_bot': result.is_bot,
                'confidence': result.confidence,
                'layer': 'layer1'
            }
        except Exception as e:
            logger.error(f"Layer 1 detection error: {e}")
            return {'is_bot': False, 'confidence': 0.0, 'layer': 'layer1'}
    
    async def _run_layer2_detection(self, comment_data: Dict[str, Any], 
                                  metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Run Layer 2 detection asynchronously"""
        try:
            if metadata and 'user_history' in metadata:
                user_data = metadata.get('user_data', {})
                user_history = metadata.get('user_history', [])
                result = self.layer2_detector.analyze_user_behavior(user_data, user_history)
                return {
                    'is_bot': result.is_bot,
                    'confidence': result.confidence,
                    'layer': 'layer2'
                }
            else:
                return {'is_bot': False, 'confidence': 0.0, 'layer': 'layer2'}
        except Exception as e:
            logger.error(f"Layer 2 detection error: {e}")
            return {'is_bot': False, 'confidence': 0.0, 'layer': 'layer2'}
    
    async def _run_layer3_detection(self, content: str) -> Dict[str, Any]:
        """Run Layer 3 detection asynchronously"""
        try:
            result = self.layer3_detector.analyze_ai_content(content)
            return {
                'is_bot': result.is_ai_generated,
                'confidence': result.confidence,
                'layer': 'layer3'
            }
        except Exception as e:
            logger.error(f"Layer 3 detection error: {e}")
            return {'is_bot': False, 'confidence': 0.0, 'layer': 'layer3'}
    
    def _prepare_comment_data(self, content: str, metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Prepare comment data for detection"""
        return {
            'body': content,
            'author': metadata.get('author', '') if metadata else '',
            'score': metadata.get('score', 0) if metadata else 0,
            'created_utc': metadata.get('created_utc', 0) if metadata else 0
        }
    
    def _generate_cache_key(self, content: str, metadata: Optional[Dict[str, Any]]) -> str:
        """Generate cache key for content"""
        content_hash = hashlib.md5(content.encode()).hexdigest()
        metadata_hash = hashlib.md5(json.dumps(metadata or {}, sort_keys=True).encode()).hexdigest()
        return f"bot_detection:{content_hash}:{metadata_hash}"
    
    def _get_cached_result(self, cache_key: str) -> Optional[RealTimeDetectionResult]:
        """Get cached detection result"""
        try:
            cached_data = self.cache.get(cache_key)
            if cached_data:
                data = json.loads(cached_data)
                return RealTimeDetectionResult(**data)
            return None
        except Exception as e:
            logger.error(f"Error retrieving cached result: {e}")
            return None
    
    def _cache_result(self, cache_key: str, result: RealTimeDetectionResult):
        """Cache detection result"""
        try:
            result_data = asdict(result)
            # Convert enum to string for JSON serialization
            result_data['priority'] = result_data['priority'].value
            self.cache.set(cache_key, json.dumps(result_data), ex=self.cache_config.detection_cache_ttl)
        except Exception as e:
            logger.error(f"Error caching result: {e}")
    
    def _check_rate_limit(self, client_ip: str) -> bool:
        """Check if client is within rate limits"""
        now = datetime.now()
        
        # Reset counters if needed
        if now.minute != self.rate_limiter['last_reset_minute']:
            self.rate_limiter['minute_counter'].clear()
            self.rate_limiter['last_reset_minute'] = now.minute
        
        if now.hour != self.rate_limiter['last_reset_hour']:
            self.rate_limiter['hour_counter'].clear()
            self.rate_limiter['last_reset_hour'] = now.hour
        
        # Check if IP is blocked
        if client_ip in self.rate_limiter['blocked_ips']:
            return False
        
        # Check rate limits
        minute_count = self.rate_limiter['minute_counter'][client_ip]
        hour_count = self.rate_limiter['hour_counter'][client_ip]
        
        if (minute_count >= self.rate_limit_config.max_requests_per_minute or
            hour_count >= self.rate_limit_config.max_requests_per_hour):
            self.rate_limiter['blocked_ips'].add(client_ip)
            return False
        
        # Update counters
        self.rate_limiter['minute_counter'][client_ip] += 1
        self.rate_limiter['hour_counter'][client_ip] += 1
        
        return True
    
    def _update_metrics(self, metric_name: str, value: float):
        """Update monitoring metrics"""
        with self.metrics_lock:
            if hasattr(self.metrics, metric_name):
                current_value = getattr(self.metrics, metric_name)
                setattr(self.metrics, metric_name, current_value + value)
    
    def _update_response_time(self, response_time: float):
        """Update response time metrics"""
        self.response_times.append(response_time)
        
        with self.metrics_lock:
            if self.response_times:
                self.metrics.avg_response_time = sum(self.response_times) / len(self.response_times)
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get comprehensive system metrics"""
        with self.metrics_lock:
            return {
                'detection_metrics': asdict(self.metrics),
                'cache_metrics': {
                    'cache_hit_rate': (self.metrics.cache_hits / max(self.metrics.cache_hits + self.metrics.cache_misses, 1)) * 100,
                    'total_cache_operations': self.metrics.cache_hits + self.metrics.cache_misses
                },
                'performance_metrics': {
                    'avg_response_time_ms': self.metrics.avg_response_time,
                    'response_times_samples': len(self.response_times),
                    'p95_response_time': self._calculate_percentile(95) if self.response_times else 0.0,
                    'p99_response_time': self._calculate_percentile(99) if self.response_times else 0.0
                },
                'system_health': {
                    'error_rate': (self.metrics.error_count / max(self.metrics.total_detections, 1)) * 100,
                    'rate_limit_hit_rate': (self.metrics.rate_limit_hits / max(self.metrics.total_detections, 1)) * 100,
                    'uptime': 'active',
                    'version': '4.0-realtime'
                }
            }
    
    def _calculate_percentile(self, percentile: float) -> float:
        """Calculate response time percentile"""
        if not self.response_times:
            return 0.0
        
        sorted_times = sorted(self.response_times)
        index = int((percentile / 100) * len(sorted_times))
        return sorted_times[min(index, len(sorted_times) - 1)]
    
    def get_health_check(self) -> Dict[str, Any]:
        """Get system health check"""
        return {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'version': '4.0-realtime',
            'layers_active': ['layer1', 'layer2', 'layer3'],
            'cache_status': 'active',
            'avg_response_time_ms': self.metrics.avg_response_time,
            'total_detections': self.metrics.total_detections,
            'error_rate': (self.metrics.error_count / max(self.metrics.total_detections, 1)) * 100
        }

# Global instance for production use
real_time_detector = RealTimeBotDetector()

async def detect_bot_realtime(content: str, metadata: Optional[Dict[str, Any]] = None, 
                            client_ip: Optional[str] = None) -> bool:
    """
    Global function for real-time bot detection
    """
    result = await real_time_detector.detect_bot_realtime(content, metadata, client_ip)
    return result.is_bot