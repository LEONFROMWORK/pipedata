"""
PipeData 배치 처리 최적화
대용량 데이터를 효율적으로 처리하기 위한 배치 최적화 시스템
"""

import os
import json
import sqlite3
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class BatchConfig:
    """배치 처리 설정"""
    batch_size: int = 100
    max_workers: int = 4
    quality_threshold: float = 7.0
    processing_timeout: int = 300  # 5분
    memory_limit_mb: int = 512
    cache_ttl_hours: int = 24

class BatchOptimizer:
    """대용량 데이터 배치 처리 최적화"""
    
    def __init__(self, config: BatchConfig):
        self.config = config
        self.db_path = '../data/combined_dataset.db'
        self.processing_stats = {
            'processed_count': 0,
            'skipped_count': 0,
            'error_count': 0,
            'start_time': None,
            'memory_usage': []
        }
    
    def get_memory_usage(self) -> float:
        """현재 메모리 사용량 조회 (MB)"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            # psutil이 없으면 대략적인 추정
            return 0.0
    
    def check_memory_limit(self) -> bool:
        """메모리 제한 확인"""
        current_memory = self.get_memory_usage()
        if current_memory > self.config.memory_limit_mb:
            logger.warning(f"Memory limit exceeded: {current_memory}MB > {self.config.memory_limit_mb}MB")
            return False
        return True
    
    def create_processing_index(self):
        """처리 성능을 위한 인덱스 생성"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 주요 쿼리 성능을 위한 인덱스
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_quality_processed ON processed_qa_data(quality_score, is_processed)",
                "CREATE INDEX IF NOT EXISTS idx_source_created ON processed_qa_data(source, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_difficulty_quality ON processed_qa_data(difficulty, quality_score)",
                "CREATE INDEX IF NOT EXISTS idx_is_processed ON processed_qa_data(is_processed)"
            ]
            
            for index_sql in indexes:
                conn.execute(index_sql)
            
            conn.commit()
            conn.close()
            logger.info("Processing indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
    
    def get_batch_data(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        """배치 데이터 조회 (메모리 효율적)"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            
            query = """
            SELECT 
                id, question, answer, code_snippets, excel_functions,
                difficulty, quality_score, source, tags, metadata, created_at
            FROM processed_qa_data 
            WHERE quality_score >= ?
            AND is_processed = 1
            ORDER BY id
            LIMIT ? OFFSET ?
            """
            
            cursor = conn.execute(query, [self.config.quality_threshold, limit, offset])
            rows = cursor.fetchall()
            conn.close()
            
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Error fetching batch data: {e}")
            return []
    
    def process_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """개별 아이템 처리"""
        try:
            # 메모리 제한 확인
            if not self.check_memory_limit():
                return None
            
            # 데이터 정리 및 검증
            processed_item = self.clean_and_validate_item(item)
            
            if processed_item:
                self.processing_stats['processed_count'] += 1
                return processed_item
            else:
                self.processing_stats['skipped_count'] += 1
                return None
                
        except Exception as e:
            logger.error(f"Error processing item {item.get('id')}: {e}")
            self.processing_stats['error_count'] += 1
            return None
    
    def clean_and_validate_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """데이터 정리 및 검증"""
        try:
            # 필수 필드 확인
            if not item.get('question') or not item.get('answer'):
                return None
            
            # 텍스트 정리
            question = self.clean_text(item['question'])
            answer = self.clean_text(item['answer'])
            
            if len(question) < 10 or len(answer) < 20:
                return None
            
            # JSON 필드 파싱
            try:
                code_snippets = json.loads(item.get('code_snippets', '[]')) if item.get('code_snippets') else []
                excel_functions = json.loads(item.get('excel_functions', '[]')) if item.get('excel_functions') else []
                tags = json.loads(item.get('tags', '[]')) if item.get('tags') else []
                metadata = json.loads(item.get('metadata', '{}')) if item.get('metadata') else {}
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON in item {item.get('id')}")
                code_snippets = []
                excel_functions = []
                tags = []
                metadata = {}
            
            # 데이터 정규화
            return {
                'id': item['id'],
                'question': question,
                'answer': answer,
                'code_snippets': code_snippets[:10],  # 최대 10개로 제한
                'excel_functions': excel_functions[:20],  # 최대 20개로 제한
                'difficulty': self.normalize_difficulty(item.get('difficulty')),
                'quality_score': float(item.get('quality_score', 0)),
                'source': item.get('source', 'unknown'),
                'tags': tags[:15],  # 최대 15개로 제한
                'metadata': metadata,
                'created_at': item.get('created_at')
            }
            
        except Exception as e:
            logger.error(f"Error cleaning item: {e}")
            return None
    
    def clean_text(self, text: str) -> str:
        """텍스트 정리"""
        if not text:
            return ""
        
        # 기본 정리
        text = str(text).strip()
        
        # 과도한 공백 제거
        import re
        text = re.sub(r'\s+', ' ', text)
        
        # 길이 제한
        if len(text) > 10000:  # 10KB 제한
            text = text[:10000] + "..."
        
        return text
    
    def normalize_difficulty(self, difficulty: str) -> str:
        """난이도 정규화"""
        if not difficulty:
            return 'medium'
        
        difficulty = str(difficulty).lower().strip()
        
        difficulty_map = {
            'beginner': 'easy',
            'basic': 'easy',
            'simple': 'easy',
            'intermediate': 'medium',
            'normal': 'medium',
            'standard': 'medium',
            'advanced': 'hard',
            'complex': 'hard',
            'expert': 'expert',
            'master': 'expert'
        }
        
        return difficulty_map.get(difficulty, 'medium')
    
    def process_batch_parallel(self, batch_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """배치 데이터 병렬 처리"""
        if not batch_data:
            return []
        
        processed_items = []
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # 작업 제출
            future_to_item = {
                executor.submit(self.process_item, item): item 
                for item in batch_data
            }
            
            # 결과 수집
            for future in as_completed(future_to_item, timeout=self.config.processing_timeout):
                try:
                    result = future.result()
                    if result:
                        processed_items.append(result)
                except Exception as e:
                    item = future_to_item[future]
                    logger.error(f"Error processing item {item.get('id')}: {e}")
        
        return processed_items
    
    def optimize_database(self):
        """데이터베이스 최적화"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # VACUUM으로 데이터베이스 크기 최적화
            logger.info("Starting database optimization...")
            conn.execute("VACUUM")
            
            # 통계 업데이트
            conn.execute("ANALYZE")
            
            conn.close()
            logger.info("Database optimization completed")
            
        except Exception as e:
            logger.error(f"Error optimizing database: {e}")
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """처리 통계 조회"""
        current_memory = self.get_memory_usage()
        self.processing_stats['memory_usage'].append(current_memory)
        
        # 메모리 사용량 히스토리를 최근 100개로 제한
        if len(self.processing_stats['memory_usage']) > 100:
            self.processing_stats['memory_usage'] = self.processing_stats['memory_usage'][-100:]
        
        duration = 0
        if self.processing_stats['start_time']:
            duration = (datetime.now() - self.processing_stats['start_time']).total_seconds()
        
        return {
            **self.processing_stats,
            'current_memory_mb': current_memory,
            'avg_memory_mb': sum(self.processing_stats['memory_usage']) / len(self.processing_stats['memory_usage']) if self.processing_stats['memory_usage'] else 0,
            'duration_seconds': duration,
            'items_per_second': self.processing_stats['processed_count'] / duration if duration > 0 else 0
        }
    
    def process_all_data(self, callback=None) -> Dict[str, Any]:
        """전체 데이터 처리"""
        logger.info("Starting batch optimization process...")
        self.processing_stats['start_time'] = datetime.now()
        
        # 인덱스 생성
        self.create_processing_index()
        
        # 전체 데이터 수 조회
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            "SELECT COUNT(*) FROM processed_qa_data WHERE quality_score >= ? AND is_processed = 1",
            [self.config.quality_threshold]
        )
        total_count = cursor.fetchone()[0]
        conn.close()
        
        logger.info(f"Total items to process: {total_count}")
        
        if total_count == 0:
            return self.get_processing_stats()
        
        processed_all = []
        offset = 0
        
        while offset < total_count:
            # 메모리 제한 확인
            if not self.check_memory_limit():
                logger.warning("Memory limit reached, pausing processing...")
                time.sleep(10)  # 10초 대기
                continue
            
            # 배치 데이터 조회
            batch_data = self.get_batch_data(offset, self.config.batch_size)
            
            if not batch_data:
                break
            
            logger.info(f"Processing batch {offset}-{offset + len(batch_data)} of {total_count}")
            
            # 배치 처리
            processed_batch = self.process_batch_parallel(batch_data)
            processed_all.extend(processed_batch)
            
            # 콜백 호출 (진행 상황 보고)
            if callback:
                callback({
                    'processed': len(processed_all),
                    'total': total_count,
                    'current_batch_size': len(processed_batch),
                    'memory_mb': self.get_memory_usage()
                })
            
            offset += len(batch_data)
            
            # 중간 저장 (메모리 절약)
            if len(processed_all) >= 1000:
                # 필요시 중간 저장 로직 구현
                pass
        
        # 데이터베이스 최적화
        self.optimize_database()
        
        final_stats = self.get_processing_stats()
        final_stats['total_processed'] = len(processed_all)
        
        logger.info(f"Batch optimization completed. Processed: {len(processed_all)} items")
        
        return final_stats

def main():
    """메인 함수"""
    config = BatchConfig(
        batch_size=int(os.getenv('BATCH_SIZE', '100')),
        max_workers=int(os.getenv('MAX_WORKERS', '4')),
        quality_threshold=float(os.getenv('QUALITY_THRESHOLD', '7.0')),
        memory_limit_mb=int(os.getenv('MEMORY_LIMIT_MB', '512'))
    )
    
    optimizer = BatchOptimizer(config)
    
    def progress_callback(stats):
        logger.info(f"Progress: {stats['processed']}/{stats['total']} ({stats['processed']/stats['total']*100:.1f}%) - Memory: {stats['memory_mb']:.1f}MB")
    
    stats = optimizer.process_all_data(callback=progress_callback)
    
    logger.info("Final processing statistics:")
    for key, value in stats.items():
        logger.info(f"  {key}: {value}")

if __name__ == "__main__":
    main()