"""
Monitoring and Observability Service for Excel Q&A System
Production-ready monitoring with metrics, logging, and alerting
"""
import logging
import asyncio
import json
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import time
import statistics
from pathlib import Path
import aiofiles

logger = logging.getLogger('monitoring_service')

class AlertLevel(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class MetricType(Enum):
    """Types of metrics"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"

@dataclass
class Alert:
    """Alert definition"""
    id: str
    level: AlertLevel
    message: str
    timestamp: str
    source: str
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class Metric:
    """Metric definition"""
    name: str
    type: MetricType
    value: float
    timestamp: str
    labels: Dict[str, str]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class PerformanceMonitor:
    """Performance monitoring for individual components"""
    
    def __init__(self, component_name: str):
        self.component_name = component_name
        self.metrics: Dict[str, List[float]] = {}
        self.start_time = None
        
    def start_timing(self):
        """Start timing an operation"""
        self.start_time = time.time()
    
    def end_timing(self, metric_name: str):
        """End timing and record metric"""
        if self.start_time is None:
            return
        
        duration = time.time() - self.start_time
        self.record_metric(metric_name, duration)
        self.start_time = None
        return duration
    
    def record_metric(self, metric_name: str, value: float):
        """Record a metric value"""
        if metric_name not in self.metrics:
            self.metrics[metric_name] = []
        
        self.metrics[metric_name].append(value)
        
        # Keep only last 1000 values
        if len(self.metrics[metric_name]) > 1000:
            self.metrics[metric_name].pop(0)
    
    def get_stats(self, metric_name: str) -> Dict[str, float]:
        """Get statistics for a metric"""
        if metric_name not in self.metrics or not self.metrics[metric_name]:
            return {}
        
        values = self.metrics[metric_name]
        return {
            "count": len(values),
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "min": min(values),
            "max": max(values),
            "std": statistics.stdev(values) if len(values) > 1 else 0.0,
            "p95": statistics.quantiles(values, n=20)[18] if len(values) > 1 else values[0],
            "p99": statistics.quantiles(values, n=100)[98] if len(values) > 1 else values[0]
        }

class MonitoringService:
    """Comprehensive monitoring service"""
    
    def __init__(self, log_dir: str = "/Users/kevin/bigdata/new_system/logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Component monitors
        self.monitors: Dict[str, PerformanceMonitor] = {}
        
        # System metrics
        self.system_metrics: Dict[str, Any] = {
            "requests_per_minute": [],
            "error_rate": [],
            "average_response_time": [],
            "cost_per_request": [],
            "quality_scores": [],
            "uptime_start": datetime.now().isoformat()
        }
        
        # Alerts
        self.alerts: List[Alert] = []
        self.alert_handlers: List[Callable] = []
        
        # Thresholds
        self.thresholds = {
            "response_time_warning": 10.0,  # seconds
            "response_time_critical": 30.0,  # seconds
            "error_rate_warning": 0.10,  # 10%
            "error_rate_critical": 0.25,  # 25%
            "cost_per_request_warning": 0.01,  # $0.01
            "cost_per_request_critical": 0.05,  # $0.05
            "quality_score_warning": 0.70,  # 70%
            "quality_score_critical": 0.50   # 50%
        }
        
        # Health check
        self.health_status = {
            "overall": "healthy",
            "components": {},
            "last_check": datetime.now().isoformat()
        }
        
        # Request tracking
        self.request_buffer = []
        self.buffer_size = 1000
        
        # Initialize logging
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup structured logging"""
        log_file = self.log_dir / f"excel_qa_system_{datetime.now().strftime('%Y%m%d')}.log"
        
        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(file_handler)
        logger.info("Monitoring service initialized")
    
    def get_monitor(self, component_name: str) -> PerformanceMonitor:
        """Get or create a performance monitor for a component"""
        if component_name not in self.monitors:
            self.monitors[component_name] = PerformanceMonitor(component_name)
        return self.monitors[component_name]
    
    async def record_request(self, request_data: Dict[str, Any]):
        """Record a request for monitoring"""
        try:
            # Add timestamp
            request_data["timestamp"] = datetime.now().isoformat()
            
            # Add to buffer
            self.request_buffer.append(request_data)
            
            # Maintain buffer size
            if len(self.request_buffer) > self.buffer_size:
                self.request_buffer.pop(0)
            
            # Update real-time metrics
            await self._update_metrics(request_data)
            
            # Check for alerts
            await self._check_alerts(request_data)
            
        except Exception as e:
            logger.error(f"Error recording request: {e}")
    
    async def _update_metrics(self, request_data: Dict[str, Any]):
        """Update system metrics"""
        try:
            # Response time
            if "response_time" in request_data:
                self.system_metrics["average_response_time"].append(request_data["response_time"])
                self._trim_metric_list("average_response_time")
            
            # Cost
            if "cost" in request_data:
                self.system_metrics["cost_per_request"].append(request_data["cost"])
                self._trim_metric_list("cost_per_request")
            
            # Quality score
            if "quality_score" in request_data:
                self.system_metrics["quality_scores"].append(request_data["quality_score"])
                self._trim_metric_list("quality_scores")
            
            # Error tracking
            if "success" in request_data:
                error_value = 0 if request_data["success"] else 1
                self.system_metrics["error_rate"].append(error_value)
                self._trim_metric_list("error_rate")
            
            # Requests per minute (simplified)
            current_minute = datetime.now().minute
            if not self.system_metrics["requests_per_minute"]:
                self.system_metrics["requests_per_minute"] = [{"minute": current_minute, "count": 1}]
            else:
                last_entry = self.system_metrics["requests_per_minute"][-1]
                if last_entry["minute"] == current_minute:
                    last_entry["count"] += 1
                else:
                    self.system_metrics["requests_per_minute"].append({"minute": current_minute, "count": 1})
                    
                    # Keep only last 60 minutes
                    if len(self.system_metrics["requests_per_minute"]) > 60:
                        self.system_metrics["requests_per_minute"].pop(0)
            
        except Exception as e:
            logger.error(f"Error updating metrics: {e}")
    
    def _trim_metric_list(self, metric_name: str, max_size: int = 1000):
        """Trim metric list to maximum size"""
        if len(self.system_metrics[metric_name]) > max_size:
            self.system_metrics[metric_name] = self.system_metrics[metric_name][-max_size:]
    
    async def _check_alerts(self, request_data: Dict[str, Any]):
        """Check for alert conditions"""
        try:
            # Response time alerts
            if "response_time" in request_data:
                response_time = request_data["response_time"]
                if response_time > self.thresholds["response_time_critical"]:
                    await self._trigger_alert(
                        AlertLevel.CRITICAL,
                        f"Response time critical: {response_time:.2f}s",
                        "response_time",
                        {"response_time": response_time}
                    )
                elif response_time > self.thresholds["response_time_warning"]:
                    await self._trigger_alert(
                        AlertLevel.WARNING,
                        f"Response time warning: {response_time:.2f}s",
                        "response_time",
                        {"response_time": response_time}
                    )
            
            # Cost alerts
            if "cost" in request_data:
                cost = request_data["cost"]
                if cost > self.thresholds["cost_per_request_critical"]:
                    await self._trigger_alert(
                        AlertLevel.CRITICAL,
                        f"Cost per request critical: ${cost:.4f}",
                        "cost",
                        {"cost": cost}
                    )
                elif cost > self.thresholds["cost_per_request_warning"]:
                    await self._trigger_alert(
                        AlertLevel.WARNING,
                        f"Cost per request warning: ${cost:.4f}",
                        "cost",
                        {"cost": cost}
                    )
            
            # Quality score alerts
            if "quality_score" in request_data:
                quality = request_data["quality_score"]
                if quality < self.thresholds["quality_score_critical"]:
                    await self._trigger_alert(
                        AlertLevel.CRITICAL,
                        f"Quality score critical: {quality:.2f}",
                        "quality",
                        {"quality_score": quality}
                    )
                elif quality < self.thresholds["quality_score_warning"]:
                    await self._trigger_alert(
                        AlertLevel.WARNING,
                        f"Quality score warning: {quality:.2f}",
                        "quality",
                        {"quality_score": quality}
                    )
            
            # Error rate alerts (check last 10 requests)
            if len(self.system_metrics["error_rate"]) >= 10:
                recent_errors = self.system_metrics["error_rate"][-10:]
                error_rate = sum(recent_errors) / len(recent_errors)
                
                if error_rate > self.thresholds["error_rate_critical"]:
                    await self._trigger_alert(
                        AlertLevel.CRITICAL,
                        f"Error rate critical: {error_rate:.1%}",
                        "error_rate",
                        {"error_rate": error_rate}
                    )
                elif error_rate > self.thresholds["error_rate_warning"]:
                    await self._trigger_alert(
                        AlertLevel.WARNING,
                        f"Error rate warning: {error_rate:.1%}",
                        "error_rate",
                        {"error_rate": error_rate}
                    )
            
        except Exception as e:
            logger.error(f"Error checking alerts: {e}")
    
    async def _trigger_alert(self, level: AlertLevel, message: str, source: str, metadata: Dict[str, Any]):
        """Trigger an alert"""
        alert = Alert(
            id=f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{source}",
            level=level,
            message=message,
            timestamp=datetime.now().isoformat(),
            source=source,
            metadata=metadata
        )
        
        self.alerts.append(alert)
        
        # Keep only last 100 alerts
        if len(self.alerts) > 100:
            self.alerts.pop(0)
        
        # Log alert
        log_level = logging.ERROR if level in [AlertLevel.ERROR, AlertLevel.CRITICAL] else logging.WARNING
        logger.log(log_level, f"ALERT [{level.value.upper()}]: {message}")
        
        # Notify alert handlers
        for handler in self.alert_handlers:
            try:
                await handler(alert)
            except Exception as e:
                logger.error(f"Error in alert handler: {e}")
    
    def add_alert_handler(self, handler: Callable):
        """Add an alert handler"""
        self.alert_handlers.append(handler)
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check"""
        try:
            health_status = {
                "overall": "healthy",
                "timestamp": datetime.now().isoformat(),
                "components": {},
                "metrics": {}
            }
            
            # Check component health
            for component_name, monitor in self.monitors.items():
                component_health = "healthy"
                
                # Check if component has recent activity
                if monitor.metrics:
                    latest_metric = max(monitor.metrics.values(), key=len)
                    if not latest_metric:
                        component_health = "inactive"
                
                health_status["components"][component_name] = component_health
            
            # Check system metrics
            if self.system_metrics["average_response_time"]:
                avg_response_time = statistics.mean(self.system_metrics["average_response_time"][-10:])
                health_status["metrics"]["average_response_time"] = avg_response_time
                
                if avg_response_time > self.thresholds["response_time_critical"]:
                    health_status["overall"] = "critical"
                elif avg_response_time > self.thresholds["response_time_warning"]:
                    health_status["overall"] = "warning"
            
            if self.system_metrics["error_rate"]:
                error_rate = statistics.mean(self.system_metrics["error_rate"][-10:])
                health_status["metrics"]["error_rate"] = error_rate
                
                if error_rate > self.thresholds["error_rate_critical"]:
                    health_status["overall"] = "critical"
                elif error_rate > self.thresholds["error_rate_warning"] and health_status["overall"] != "critical":
                    health_status["overall"] = "warning"
            
            # Update health status
            self.health_status = health_status
            return health_status
            
        except Exception as e:
            logger.error(f"Error in health check: {e}")
            return {
                "overall": "error",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
    
    async def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        try:
            summary = {
                "timestamp": datetime.now().isoformat(),
                "system_metrics": {},
                "component_metrics": {},
                "alerts": {
                    "total": len(self.alerts),
                    "by_level": {},
                    "recent": [alert.to_dict() for alert in self.alerts[-5:]]
                }
            }
            
            # System metrics
            for metric_name, values in self.system_metrics.items():
                if metric_name == "requests_per_minute":
                    summary["system_metrics"][metric_name] = values
                elif metric_name == "uptime_start":
                    uptime = datetime.now() - datetime.fromisoformat(values)
                    summary["system_metrics"]["uptime_seconds"] = uptime.total_seconds()
                elif values:
                    if all(isinstance(v, (int, float)) for v in values):
                        summary["system_metrics"][metric_name] = {
                            "current": values[-1] if values else 0,
                            "mean": statistics.mean(values),
                            "min": min(values),
                            "max": max(values),
                            "count": len(values)
                        }
            
            # Component metrics
            for component_name, monitor in self.monitors.items():
                summary["component_metrics"][component_name] = {}
                for metric_name in monitor.metrics:
                    stats = monitor.get_stats(metric_name)
                    if stats:
                        summary["component_metrics"][component_name][metric_name] = stats
            
            # Alert statistics
            for alert in self.alerts:
                level = alert.level.value
                summary["alerts"]["by_level"][level] = summary["alerts"]["by_level"].get(level, 0) + 1
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting metrics summary: {e}")
            return {"error": str(e)}
    
    async def export_metrics(self, format: str = "json") -> str:
        """Export metrics in specified format"""
        try:
            metrics = await self.get_metrics_summary()
            
            if format.lower() == "json":
                return json.dumps(metrics, indent=2)
            elif format.lower() == "csv":
                # Basic CSV export (simplified)
                lines = ["timestamp,metric,value"]
                timestamp = datetime.now().isoformat()
                
                for metric_name, metric_data in metrics.get("system_metrics", {}).items():
                    if isinstance(metric_data, dict) and "current" in metric_data:
                        lines.append(f"{timestamp},{metric_name},{metric_data['current']}")
                
                return "\n".join(lines)
            else:
                raise ValueError(f"Unsupported format: {format}")
                
        except Exception as e:
            logger.error(f"Error exporting metrics: {e}")
            return f"Error: {str(e)}"
    
    async def save_metrics_to_file(self, filename: str = None):
        """Save metrics to file"""
        try:
            if filename is None:
                filename = f"metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            filepath = self.log_dir / filename
            metrics = await self.get_metrics_summary()
            
            async with aiofiles.open(filepath, 'w') as f:
                await f.write(json.dumps(metrics, indent=2))
            
            logger.info(f"Metrics saved to {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Error saving metrics: {e}")
            return None
    
    def reset_metrics(self):
        """Reset all metrics (for testing)"""
        self.system_metrics = {
            "requests_per_minute": [],
            "error_rate": [],
            "average_response_time": [],
            "cost_per_request": [],
            "quality_scores": [],
            "uptime_start": datetime.now().isoformat()
        }
        self.alerts.clear()
        self.request_buffer.clear()
        self.monitors.clear()
        logger.info("Metrics reset")

# Singleton instance
_monitoring_service = None

async def get_monitoring_service() -> MonitoringService:
    """Get singleton monitoring service instance"""
    global _monitoring_service
    if _monitoring_service is None:
        _monitoring_service = MonitoringService()
    return _monitoring_service

# Context manager for performance monitoring
class monitor_performance:
    """Context manager for monitoring performance"""
    
    def __init__(self, component_name: str, operation_name: str):
        self.component_name = component_name
        self.operation_name = operation_name
        self.monitor = None
    
    async def __aenter__(self):
        monitoring_service = await get_monitoring_service()
        self.monitor = monitoring_service.get_monitor(self.component_name)
        self.monitor.start_timing()
        return self.monitor
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.monitor:
            duration = self.monitor.end_timing(self.operation_name)
            if exc_type:
                self.monitor.record_metric(f"{self.operation_name}_errors", 1)
            return False