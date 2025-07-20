"""
Continuous Data Collection System
사용자가 정지할 때까지 지속적으로 데이터를 수집하는 시스템
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path
import json

from pipeline.main_pipeline import ExcelQAPipeline

logger = logging.getLogger('pipeline.continuous_collector')

class ContinuousCollector:
    """
    지속적 데이터 수집기
    
    기능:
    1. 정지 신호가 올 때까지 계속 수집
    2. 배치 단위로 데이터 수집 및 저장
    3. 실시간 상태 업데이트
    4. 수집된 데이터의 누적 통계
    """
    
    def __init__(self):
        self.pipeline = ExcelQAPipeline()
        self.is_running = False
        self.total_collected = 0
        self.total_processed = 0
        self.total_final = 0
        self.batch_count = 0
        self.start_time = None
        self.datasets_generated = []
        
        # 배치 설정
        self.batch_size = 5  # 한 번에 수집할 항목 수
        self.batch_interval = 10  # 배치 간 대기 시간 (초)
        
    async def start_continuous_collection(self, sources: List[str] = None, 
                                        max_per_batch: int = 5) -> Dict[str, Any]:
        """지속적 수집 시작"""
        if sources is None:
            sources = ['reddit']  # 기본적으로 Reddit만 수집
            
        self.is_running = True
        self.start_time = datetime.now()
        self.batch_size = max_per_batch
        
        logger.info(f"Starting continuous collection: sources={sources}, batch_size={max_per_batch}")
        
        total_collected = 0
        total_processed = 0  
        total_final = 0
        
        try:
            batch_num = 1
            while self.is_running:
                logger.info(f"Starting batch {batch_num} collection...")
                
                # 🔧 Pipeline State 리셋 (무한 누적 문제 수정)
                from pipeline.main_pipeline import PipelineState
                self.pipeline.state = PipelineState()
                logger.debug(f"Pipeline state reset for batch {batch_num}")
                
                # 배치 수집 실행
                batch_result = await self.pipeline.run_full_pipeline(
                    from_date=None,  # 최신 데이터
                    max_pages=2,     # 적은 페이지로 빠른 수집
                    target_count=max_per_batch,
                    sources=sources
                )
                
                # 결과 누적
                if batch_result and 'data_flow' in batch_result:
                    data_flow = batch_result['data_flow']
                    total_collected += data_flow.get('collected', 0)
                    total_processed += data_flow.get('processed', 0)
                    total_final += data_flow.get('final_output', 0)
                    
                    # 생성된 데이터셋 추가
                    if 'dataset_path' in batch_result:
                        self.datasets_generated.append({
                            'batch': batch_num,
                            'path': batch_result['dataset_path'],
                            'timestamp': datetime.now().isoformat(),
                            'items': data_flow.get('final_output', 0)
                        })
                
                self.batch_count = batch_num
                self.total_collected = total_collected
                self.total_processed = total_processed
                self.total_final = total_final
                
                logger.info(f"Batch {batch_num} complete: "
                           f"collected={data_flow.get('collected', 0)}, "
                           f"final={data_flow.get('final_output', 0)}")
                logger.info(f"Cumulative totals: "
                           f"collected={total_collected}, "
                           f"processed={total_processed}, "
                           f"final={total_final}")
                
                # 정지 신호 확인
                if not self.is_running:
                    logger.info("Stop signal received, ending collection")
                    break
                
                # 다음 배치까지 대기
                logger.info(f"Waiting {self.batch_interval} seconds before next batch...")
                await asyncio.sleep(self.batch_interval)
                
                batch_num += 1
                
        except Exception as e:
            logger.error(f"Continuous collection error: {e}")
            
        finally:
            await self.pipeline.so_collector.close()
            
        # 최종 결과 반환
        return self._create_final_result()
    
    def stop_collection(self):
        """수집 정지"""
        self.is_running = False
        logger.info("Continuous collection stop requested")
        
    def get_status(self) -> Dict[str, Any]:
        """현재 상태 반환"""
        if not self.start_time:
            return {"status": "not_started"}
            
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        return {
            "status": "running" if self.is_running else "stopped",
            "elapsed_time": elapsed,
            "batch_count": self.batch_count,
            "total_collected": self.total_collected,
            "total_processed": self.total_processed,
            "total_final": self.total_final,
            "datasets_generated": len(self.datasets_generated),
            "collection_rate": self.total_final / (elapsed / 60) if elapsed > 0 else 0  # items per minute
        }
    
    def _create_final_result(self) -> Dict[str, Any]:
        """최종 결과 생성"""
        elapsed = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        return {
            "status": "completed",
            "execution_summary": {
                "total_batches": self.batch_count,
                "total_execution_time": elapsed,
                "final_status": "stopped" if not self.is_running else "completed"
            },
            "cumulative_data_flow": {
                "total_collected": self.total_collected,
                "total_processed": self.total_processed,
                "total_final": self.total_final
            },
            "datasets_generated": self.datasets_generated,
            "performance_metrics": {
                "items_per_minute": self.total_final / (elapsed / 60) if elapsed > 0 else 0,
                "batches_per_hour": self.batch_count / (elapsed / 3600) if elapsed > 0 else 0,
                "success_rate": self.total_final / self.total_collected * 100 if self.total_collected > 0 else 0
            }
        }