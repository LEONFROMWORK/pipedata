"""
공통 데이터 모델
모든 수집기가 사용하는 공통 데이터 구조
"""
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid

@dataclass
class QAEntry:
    """Q&A 데이터 공통 모델"""
    id: str
    user_question: str
    user_context: str
    assistant_response: str
    code_blocks: List[str]
    metadata: Dict[str, Any]
    
    def __post_init__(self):
        if not self.id:
            self.id = f"qa_{uuid.uuid4().hex[:8]}"
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            'id': self.id,
            'user_question': self.user_question,
            'user_context': self.user_context,
            'assistant_response': self.assistant_response,
            'code_blocks': self.code_blocks,
            'metadata': self.metadata
        }

@dataclass
class CollectionStats:
    """수집 통계 공통 모델"""
    source: str
    total_collected: int
    total_skipped: int
    collection_time_seconds: float
    quality_score_avg: float
    errors_count: int
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            'source': self.source,
            'total_collected': self.total_collected,
            'total_skipped': self.total_skipped,
            'collection_time_seconds': self.collection_time_seconds,
            'quality_score_avg': self.quality_score_avg,
            'errors_count': self.errors_count
        }

@dataclass
class BotDetectionResult:
    """봇 탐지 결과 공통 모델"""
    is_bot: bool
    confidence: float
    detection_method: str
    details: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            'is_bot': self.is_bot,
            'confidence': self.confidence,
            'detection_method': self.detection_method,
            'details': self.details
        }