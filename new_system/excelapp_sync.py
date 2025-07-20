"""
PipeData → ExcelApp 자동 동기화 시스템
Railway에서 실행되며 주기적으로 ExcelApp에 데이터를 전송합니다.
"""

import os
import json
import time
import logging
import requests
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import hashlib

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class SyncConfig:
    """동기화 설정"""
    excelapp_api_url: str
    api_token: str
    batch_size: int = 50
    sync_interval_hours: int = 6
    quality_threshold: float = 7.0
    max_retries: int = 3
    retry_delay: int = 60

class ExcelAppSyncer:
    """ExcelApp과의 동기화를 담당하는 클래스"""
    
    def __init__(self, config: SyncConfig):
        self.config = config
        self.db_path = '../data/combined_dataset.db'
        self.sync_state_path = 'sync_state.json'
        
    def load_sync_state(self) -> Dict[str, Any]:
        """동기화 상태 로드"""
        try:
            if os.path.exists(self.sync_state_path):
                with open(self.sync_state_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading sync state: {e}")
        
        return {
            'last_sync': None,
            'last_processed_id': None,
            'total_synced': 0,
            'failed_batches': []
        }
    
    def save_sync_state(self, state: Dict[str, Any]):
        """동기화 상태 저장"""
        try:
            with open(self.sync_state_path, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving sync state: {e}")
    
    def get_new_data(self, last_processed_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """새로운 데이터 조회"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            
            query = """
            SELECT 
                id,
                question,
                answer,
                code_snippets,
                excel_functions,
                difficulty,
                quality_score,
                source,
                tags,
                metadata,
                created_at
            FROM processed_qa_data 
            WHERE quality_score >= ? 
            AND is_processed = 1
            """
            
            params = [self.config.quality_threshold]
            
            if last_processed_id:
                query += " AND id > ?"
                params.append(last_processed_id)
            
            query += " ORDER BY id LIMIT ?"
            params.append(self.config.batch_size)
            
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
            conn.close()
            
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Error fetching new data: {e}")
            return []
    
    def prepare_batch(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """ExcelApp API 형식으로 배치 데이터 준비"""
        prepared_data = []
        
        for item in data:
            try:
                # JSON 필드 파싱
                code_snippets = json.loads(item.get('code_snippets', '[]')) if item.get('code_snippets') else []
                excel_functions = json.loads(item.get('excel_functions', '[]')) if item.get('excel_functions') else []
                tags = json.loads(item.get('tags', '[]')) if item.get('tags') else []
                metadata = json.loads(item.get('metadata', '{}')) if item.get('metadata') else {}
                
                # 난이도 매핑
                difficulty_map = {
                    'beginner': 'easy',
                    'intermediate': 'medium', 
                    'advanced': 'hard',
                    'expert': 'expert'
                }
                difficulty = difficulty_map.get(item.get('difficulty', 'medium'), 'medium')
                
                prepared_item = {
                    'question': item['question'],
                    'answer': item['answer'],
                    'code_snippets': code_snippets,
                    'excel_functions': excel_functions,
                    'difficulty': difficulty,
                    'quality_score': float(item['quality_score']),
                    'source': f"pipedata_{item.get('source', 'unknown')}",
                    'tags': tags,
                    'metadata': {
                        **metadata,
                        'pipedata_id': item['id'],
                        'created_at': item.get('created_at')
                    }
                }
                
                prepared_data.append(prepared_item)
                
            except Exception as e:
                logger.error(f"Error preparing item {item.get('id')}: {e}")
                continue
        
        return {
            'data': prepared_data,
            'batch_info': {
                'timestamp': datetime.utcnow().isoformat(),
                'source': 'pipedata_railway',
                'batch_size': len(prepared_data)
            }
        }
    
    def send_to_excelapp(self, batch_data: Dict[str, Any]) -> bool:
        """ExcelApp에 배치 데이터 전송"""
        headers = {
            'Content-Type': 'application/json',
            'X-PipeData-Token': self.config.api_token
        }
        
        for attempt in range(self.config.max_retries):
            try:
                response = requests.post(
                    self.config.excelapp_api_url,
                    json=batch_data,
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"Batch sent successfully: {result}")
                    return True
                else:
                    logger.error(f"API error (attempt {attempt + 1}): {response.status_code} - {response.text}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error (attempt {attempt + 1}): {e}")
            
            if attempt < self.config.max_retries - 1:
                time.sleep(self.config.retry_delay)
        
        return False
    
    def check_excelapp_status(self) -> bool:
        """ExcelApp API 상태 확인"""
        try:
            headers = {'X-PipeData-Token': self.config.api_token}
            response = requests.get(
                self.config.excelapp_api_url,
                headers=headers,
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Error checking ExcelApp status: {e}")
            return False
    
    def sync_batch(self) -> bool:
        """한 번의 동기화 배치 실행"""
        logger.info("Starting sync batch...")
        
        # ExcelApp 상태 확인
        if not self.check_excelapp_status():
            logger.error("ExcelApp API is not available")
            return False
        
        # 동기화 상태 로드
        state = self.load_sync_state()
        
        # 새 데이터 조회
        new_data = self.get_new_data(state.get('last_processed_id'))
        
        if not new_data:
            logger.info("No new data to sync")
            return True
        
        logger.info(f"Found {len(new_data)} new items to sync")
        
        # 배치 데이터 준비
        batch_data = self.prepare_batch(new_data)
        
        if not batch_data['data']:
            logger.warning("No valid data after preparation")
            return True
        
        # ExcelApp에 전송
        success = self.send_to_excelapp(batch_data)
        
        if success:
            # 상태 업데이트
            state['last_sync'] = datetime.utcnow().isoformat()
            state['last_processed_id'] = new_data[-1]['id']
            state['total_synced'] += len(batch_data['data'])
            
            # 실패한 배치 목록에서 제거
            batch_hash = hashlib.md5(json.dumps(batch_data, sort_keys=True).encode()).hexdigest()
            state['failed_batches'] = [b for b in state.get('failed_batches', []) if b.get('hash') != batch_hash]
            
            self.save_sync_state(state)
            logger.info(f"Sync batch completed successfully. Total synced: {state['total_synced']}")
            return True
        else:
            # 실패한 배치 기록
            batch_hash = hashlib.md5(json.dumps(batch_data, sort_keys=True).encode()).hexdigest()
            failed_batch = {
                'hash': batch_hash,
                'timestamp': datetime.utcnow().isoformat(),
                'data': batch_data,
                'retry_count': 0
            }
            
            if 'failed_batches' not in state:
                state['failed_batches'] = []
            state['failed_batches'].append(failed_batch)
            
            self.save_sync_state(state)
            logger.error("Sync batch failed")
            return False
    
    def retry_failed_batches(self):
        """실패한 배치 재시도"""
        state = self.load_sync_state()
        failed_batches = state.get('failed_batches', [])
        
        if not failed_batches:
            return
        
        logger.info(f"Retrying {len(failed_batches)} failed batches...")
        
        for i, failed_batch in enumerate(failed_batches[:]):
            if failed_batch.get('retry_count', 0) >= self.config.max_retries:
                logger.warning(f"Skipping batch after {self.config.max_retries} retries")
                continue
            
            logger.info(f"Retrying batch {i + 1}/{len(failed_batches)}")
            
            success = self.send_to_excelapp(failed_batch['data'])
            
            if success:
                failed_batches.remove(failed_batch)
                logger.info("Failed batch retry successful")
            else:
                failed_batch['retry_count'] = failed_batch.get('retry_count', 0) + 1
        
        state['failed_batches'] = failed_batches
        self.save_sync_state(state)
    
    def run_continuous_sync(self):
        """연속 동기화 실행"""
        logger.info("Starting continuous sync...")
        
        while True:
            try:
                # 메인 동기화
                self.sync_batch()
                
                # 실패한 배치 재시도
                self.retry_failed_batches()
                
                # 대기
                logger.info(f"Waiting {self.config.sync_interval_hours} hours until next sync...")
                time.sleep(self.config.sync_interval_hours * 3600)
                
            except KeyboardInterrupt:
                logger.info("Sync stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in sync loop: {e}")
                time.sleep(300)  # 5분 대기 후 재시도

def main():
    """메인 함수"""
    # 환경 변수에서 설정 로드
    config = SyncConfig(
        excelapp_api_url=os.getenv('EXCELAPP_API_URL', 'https://your-excelapp-domain.com/api/training/pipedata'),
        api_token=os.getenv('EXCELAPP_API_TOKEN', 'your-api-token'),
        batch_size=int(os.getenv('SYNC_BATCH_SIZE', '50')),
        sync_interval_hours=int(os.getenv('SYNC_INTERVAL_HOURS', '6')),
        quality_threshold=float(os.getenv('QUALITY_THRESHOLD', '7.0'))
    )
    
    syncer = ExcelAppSyncer(config)
    
    # 한 번만 동기화 실행 (테스트용)
    if os.getenv('SYNC_MODE') == 'once':
        syncer.sync_batch()
    else:
        # 연속 동기화 실행
        syncer.run_continuous_sync()

if __name__ == "__main__":
    main()