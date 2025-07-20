"""
AI 사용량 추적 시스템
- OpenRouter.ai 모델별 사용량 추적
- 비용 계산 및 분석
- 일일/월별 사용량 통계
"""
import json
import sqlite3
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from decimal import Decimal
import threading

logger = logging.getLogger('pipeline.usage_tracker')

@dataclass
class UsageRecord:
    """사용량 기록 데이터 클래스"""
    timestamp: str
    provider: str  # 'openrouter'
    model: str     # 'mistralai/mistral-7b-instruct'
    usage_type: str  # 'input', 'output'
    tokens: int
    cost_usd: float
    source: str    # 'stackoverflow', 'reddit', etc.
    operation: str # 'collection', 'processing', 'quality_check'

class UsageTracker:
    """AI 사용량 추적기"""
    
    def __init__(self, db_path: str = "data/usage_tracking.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_database()
        
        # OpenRouter 모델별 비용 정보 (per 1K tokens)
        self.model_costs = {
            # Tier 1 - Fast & Cheap
            'mistralai/mistral-7b-instruct': {'input': 0.00013, 'output': 0.00013},
            'meta-llama/llama-3.2-3b-instruct:free': {'input': 0.0, 'output': 0.0},
            
            # Tier 2 - Balanced
            'mistralai/mistral-small': {'input': 0.0002, 'output': 0.0006},
            'meta-llama/llama-3.1-8b-instruct': {'input': 0.00018, 'output': 0.00018},
            
            # Tier 3 - Premium
            'openai/gpt-4o-mini': {'input': 0.00015, 'output': 0.0006},
            'anthropic/claude-3-haiku': {'input': 0.00025, 'output': 0.00125},
            
            # Fallback for unknown models
            'default': {'input': 0.0002, 'output': 0.0006}
        }
    
    def _init_database(self):
        """데이터베이스 초기화"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS usage_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    model TEXT NOT NULL,
                    usage_type TEXT NOT NULL,
                    tokens INTEGER NOT NULL,
                    cost_usd REAL NOT NULL,
                    source TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 인덱스 생성
            conn.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON usage_records(timestamp)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_model ON usage_records(model)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_source ON usage_records(source)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON usage_records(date(timestamp))')
            
            conn.commit()
    
    def track_usage(self, model: str, input_tokens: int, output_tokens: int, 
                   source: str = 'unknown', operation: str = 'general'):
        """사용량 추적"""
        with self._lock:
            try:
                timestamp = datetime.now().isoformat()
                
                # 비용 계산
                costs = self.model_costs.get(model, self.model_costs['default'])
                input_cost = (input_tokens / 1000) * costs['input']
                output_cost = (output_tokens / 1000) * costs['output']
                
                records = []
                
                # Input tokens 기록
                if input_tokens > 0:
                    records.append(UsageRecord(
                        timestamp=timestamp,
                        provider='openrouter',
                        model=model,
                        usage_type='input',
                        tokens=input_tokens,
                        cost_usd=input_cost,
                        source=source,
                        operation=operation
                    ))
                
                # Output tokens 기록
                if output_tokens > 0:
                    records.append(UsageRecord(
                        timestamp=timestamp,
                        provider='openrouter',
                        model=model,
                        usage_type='output',
                        tokens=output_tokens,
                        cost_usd=output_cost,
                        source=source,
                        operation=operation
                    ))
                
                # 데이터베이스에 저장
                self._save_records(records)
                
                logger.info(f"Usage tracked: {model} - {input_tokens}in/{output_tokens}out tokens, ${input_cost + output_cost:.6f}")
                
                return {
                    'total_cost': input_cost + output_cost,
                    'input_cost': input_cost,
                    'output_cost': output_cost,
                    'total_tokens': input_tokens + output_tokens
                }
                
            except Exception as e:
                logger.error(f"Failed to track usage: {e}")
                return None
    
    def _save_records(self, records: List[UsageRecord]):
        """기록을 데이터베이스에 저장"""
        with sqlite3.connect(self.db_path) as conn:
            for record in records:
                conn.execute('''
                    INSERT INTO usage_records 
                    (timestamp, provider, model, usage_type, tokens, cost_usd, source, operation)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    record.timestamp, record.provider, record.model, record.usage_type,
                    record.tokens, record.cost_usd, record.source, record.operation
                ))
            conn.commit()
    
    def get_daily_stats(self, target_date: Optional[date] = None) -> Dict[str, Any]:
        """일일 사용량 통계"""
        if target_date is None:
            target_date = date.today()
        
        date_str = target_date.isoformat()
        
        with sqlite3.connect(self.db_path) as conn:
            # 총 사용량
            cursor = conn.execute('''
                SELECT 
                    COUNT(*) as total_requests,
                    SUM(tokens) as total_tokens,
                    SUM(cost_usd) as total_cost
                FROM usage_records 
                WHERE date(timestamp) = ?
            ''', (date_str,))
            
            total_stats = cursor.fetchone()
            
            # 모델별 사용량
            cursor = conn.execute('''
                SELECT 
                    model,
                    COUNT(*) as requests,
                    SUM(tokens) as tokens,
                    SUM(cost_usd) as cost
                FROM usage_records 
                WHERE date(timestamp) = ?
                GROUP BY model
                ORDER BY cost DESC
            ''', (date_str,))
            
            model_stats = [
                {
                    'model': row[0],
                    'requests': row[1],
                    'tokens': row[2],
                    'cost': row[3]
                }
                for row in cursor.fetchall()
            ]
            
            # 소스별 사용량
            cursor = conn.execute('''
                SELECT 
                    source,
                    COUNT(*) as requests,
                    SUM(tokens) as tokens,
                    SUM(cost_usd) as cost
                FROM usage_records 
                WHERE date(timestamp) = ?
                GROUP BY source
                ORDER BY cost DESC
            ''', (date_str,))
            
            source_stats = [
                {
                    'source': row[0],
                    'requests': row[1],
                    'tokens': row[2],
                    'cost': row[3]
                }
                for row in cursor.fetchall()
            ]
            
            # 시간별 사용량 (최근 24시간)
            cursor = conn.execute('''
                SELECT 
                    strftime('%H', timestamp) as hour,
                    COUNT(*) as requests,
                    SUM(tokens) as tokens,
                    SUM(cost_usd) as cost
                FROM usage_records 
                WHERE date(timestamp) = ?
                GROUP BY hour
                ORDER BY hour
            ''', (date_str,))
            
            hourly_stats = [
                {
                    'hour': int(row[0]),
                    'requests': row[1],
                    'tokens': row[2],
                    'cost': row[3]
                }
                for row in cursor.fetchall()
            ]
            
            return {
                'date': date_str,
                'total_requests': total_stats[0] or 0,
                'total_tokens': total_stats[1] or 0,
                'total_cost': total_stats[2] or 0.0,
                'model_breakdown': model_stats,
                'source_breakdown': source_stats,
                'hourly_breakdown': hourly_stats
            }
    
    def get_monthly_stats(self, year: int, month: int) -> Dict[str, Any]:
        """월별 사용량 통계"""
        with sqlite3.connect(self.db_path) as conn:
            # 해당 월의 일별 사용량
            cursor = conn.execute('''
                SELECT 
                    date(timestamp) as day,
                    COUNT(*) as requests,
                    SUM(tokens) as tokens,
                    SUM(cost_usd) as cost
                FROM usage_records 
                WHERE strftime('%Y', timestamp) = ? AND strftime('%m', timestamp) = ?
                GROUP BY day
                ORDER BY day
            ''', (str(year), f"{month:02d}"))
            
            daily_stats = [
                {
                    'date': row[0],
                    'requests': row[1],
                    'tokens': row[2],
                    'cost': row[3]
                }
                for row in cursor.fetchall()
            ]
            
            # 월 총합
            cursor = conn.execute('''
                SELECT 
                    COUNT(*) as total_requests,
                    SUM(tokens) as total_tokens,
                    SUM(cost_usd) as total_cost
                FROM usage_records 
                WHERE strftime('%Y', timestamp) = ? AND strftime('%m', timestamp) = ?
            ''', (str(year), f"{month:02d}"))
            
            total_stats = cursor.fetchone()
            
            return {
                'year': year,
                'month': month,
                'total_requests': total_stats[0] or 0,
                'total_tokens': total_stats[1] or 0,
                'total_cost': total_stats[2] or 0.0,
                'daily_breakdown': daily_stats
            }
    
    def get_cost_projection(self, days_ahead: int = 30) -> Dict[str, Any]:
        """비용 예측"""
        # 최근 7일 평균 사용량 기반으로 예측
        recent_date = date.today() - timedelta(days=7)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                SELECT 
                    AVG(daily_cost) as avg_daily_cost,
                    MIN(daily_cost) as min_daily_cost,
                    MAX(daily_cost) as max_daily_cost
                FROM (
                    SELECT 
                        date(timestamp) as day,
                        SUM(cost_usd) as daily_cost
                    FROM usage_records 
                    WHERE date(timestamp) >= ?
                    GROUP BY day
                )
            ''', (recent_date.isoformat(),))
            
            stats = cursor.fetchone()
            
            if stats[0]:  # 데이터가 있는 경우
                avg_daily = stats[0]
                min_daily = stats[1]
                max_daily = stats[2]
                
                return {
                    'avg_daily_cost': avg_daily,
                    'min_daily_cost': min_daily,
                    'max_daily_cost': max_daily,
                    'projected_cost_conservative': min_daily * days_ahead,
                    'projected_cost_average': avg_daily * days_ahead,
                    'projected_cost_high': max_daily * days_ahead,
                    'days_projected': days_ahead
                }
            else:
                return {
                    'avg_daily_cost': 0.0,
                    'min_daily_cost': 0.0,
                    'max_daily_cost': 0.0,
                    'projected_cost_conservative': 0.0,
                    'projected_cost_average': 0.0,
                    'projected_cost_high': 0.0,
                    'days_projected': days_ahead
                }
    
    def export_usage_data(self, start_date: date, end_date: date, output_file: str):
        """사용량 데이터 내보내기"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                SELECT * FROM usage_records 
                WHERE date(timestamp) BETWEEN ? AND ?
                ORDER BY timestamp
            ''', (start_date.isoformat(), end_date.isoformat()))
            
            records = cursor.fetchall()
            
        # CSV 형태로 저장
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            # 헤더
            f.write('timestamp,provider,model,usage_type,tokens,cost_usd,source,operation\n')
            
            # 데이터
            for record in records:
                f.write(f"{record[1]},{record[2]},{record[3]},{record[4]},{record[5]},{record[6]},{record[7]},{record[8]}\n")
        
        logger.info(f"Usage data exported to {output_path} ({len(records)} records)")
        return len(records)

# 전역 인스턴스
usage_tracker = UsageTracker()