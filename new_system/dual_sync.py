"""
PipeData → ExcelApp + ExcelApp-Rails 이중 동기화 시스템
Railway에서 실행되며 주기적으로 두 앱 모두에 독립적으로 데이터를 전송합니다.
완전한 독립성 보장: 한 앱이 실패해도 다른 앱은 계속 동작
"""

import os
import json
import time
import logging
import requests
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import hashlib
import concurrent.futures
from threading import Lock

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dual_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class AppConfig:
    """개별 앱 설정"""
    name: str
    api_url: str
    api_token: str
    enabled: bool = True

@dataclass
class DualSyncConfig:
    """이중 동기화 설정"""
    batch_size: int = 50
    sync_interval_hours: int = 6
    quality_threshold: float = 7.0
    max_retries: int = 3
    retry_delay: int = 60
    parallel_send: bool = True

class DualExcelAppSyncer:
    """ExcelApp과 ExcelApp-Rails에 독립적으로 동기화하는 클래스"""
    
    def __init__(self, excelapp_config: AppConfig, rails_config: AppConfig, sync_config: DualSyncConfig):
        self.excelapp = excelapp_config
        self.rails = rails_config
        self.config = sync_config
        self.db_path = '../data/combined_dataset.db'
        self.sync_state_path = 'dual_sync_state.json'
        self.state_lock = Lock()
        
        logger.info(f"Initialized DualSyncer:")
        logger.info(f"  ExcelApp: {self.excelapp.name} ({'enabled' if self.excelapp.enabled else 'disabled'})")
        logger.info(f"  Rails: {self.rails.name} ({'enabled' if self.rails.enabled else 'disabled'})")
        
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
            'excelapp': {
                'last_sync': None,
                'last_processed_id': None,
                'total_synced': 0,
                'failed_batches': [],
                'sync_count': 0,
                'success_rate': 0.0
            },
            'rails': {
                'last_sync': None,
                'last_processed_id': None,
                'total_synced': 0,
                'failed_batches': [],
                'sync_count': 0,
                'success_rate': 0.0
            }
        }
    
    def save_sync_state(self, state: Dict[str, Any]):
        """동기화 상태 저장 (thread-safe)"""
        with self.state_lock:
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
    
    def prepare_batch_for_excelapp(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """ExcelApp API 형식으로 배치 데이터 준비"""
        prepared_data = []
        
        for item in data:
            try:
                # JSON 필드 파싱
                code_snippets = json.loads(item.get('code_snippets', '[]')) if item.get('code_snippets') else []
                excel_functions = json.loads(item.get('excel_functions', '[]')) if item.get('excel_functions') else []
                tags = json.loads(item.get('tags', '[]')) if item.get('tags') else []
                metadata = json.loads(item.get('metadata', '{}')) if item.get('metadata') else {}
                
                # 난이도 매핑 (ExcelApp용)
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
                        'created_at': item.get('created_at'),
                        'sync_target': 'excelapp'
                    }
                }
                
                prepared_data.append(prepared_item)
                
            except Exception as e:
                logger.error(f"Error preparing ExcelApp item {item.get('id')}: {e}")
                continue
        
        return {
            'data': prepared_data,
            'batch_info': {
                'timestamp': datetime.utcnow().isoformat(),
                'source': 'pipedata_railway',
                'target': 'excelapp',
                'batch_size': len(prepared_data)
            }
        }
    
    def prepare_batch_for_rails(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """ExcelApp-Rails API 형식으로 배치 데이터 준비"""
        prepared_data = []
        
        for item in data:
            try:
                # JSON 필드 파싱
                code_snippets = json.loads(item.get('code_snippets', '[]')) if item.get('code_snippets') else []
                excel_functions = json.loads(item.get('excel_functions', '[]')) if item.get('excel_functions') else []
                tags = json.loads(item.get('tags', '[]')) if item.get('tags') else []
                metadata = json.loads(item.get('metadata', '{}')) if item.get('metadata') else {}
                
                # 난이도 매핑 (Rails용 - 동일하지만 명시적 분리)
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
                        'created_at': item.get('created_at'),
                        'sync_target': 'rails'
                    }
                }
                
                prepared_data.append(prepared_item)
                
            except Exception as e:
                logger.error(f"Error preparing Rails item {item.get('id')}: {e}")
                continue
        
        return {
            'data': prepared_data,
            'batch_info': {
                'timestamp': datetime.utcnow().isoformat(),
                'source': 'pipedata_railway',
                'target': 'rails',
                'batch_size': len(prepared_data)
            }
        }
    
    def send_to_app(self, app_config: AppConfig, batch_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """개별 앱에 배치 데이터 전송"""
        if not app_config.enabled:
            logger.info(f"{app_config.name} is disabled, skipping")
            return True, "disabled"
        
        headers = {
            'Content-Type': 'application/json',
            'X-PipeData-Token': app_config.api_token
        }
        
        for attempt in range(self.config.max_retries):
            try:
                logger.info(f"Sending batch to {app_config.name} (attempt {attempt + 1})")
                
                response = requests.post(
                    app_config.api_url,
                    json=batch_data,
                    headers=headers,
                    timeout=60  # 증가된 타임아웃
                )
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"{app_config.name} batch sent successfully: {result}")
                    return True, None
                else:
                    error_msg = f"API error (attempt {attempt + 1}): {response.status_code} - {response.text}"
                    logger.error(f"{app_config.name} {error_msg}")
                    
            except requests.exceptions.RequestException as e:
                error_msg = f"Request error (attempt {attempt + 1}): {e}"
                logger.error(f"{app_config.name} {error_msg}")
            
            if attempt < self.config.max_retries - 1:
                time.sleep(self.config.retry_delay)
        
        return False, f"Failed after {self.config.max_retries} attempts"
    
    def send_to_both_apps(self, excelapp_batch: Dict[str, Any], rails_batch: Dict[str, Any]) -> Dict[str, Any]:
        """두 앱에 병렬 또는 순차적으로 데이터 전송"""
        results = {
            'excelapp': {'success': False, 'error': None},
            'rails': {'success': False, 'error': None}
        }
        
        if self.config.parallel_send:
            # 병렬 전송
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = {}
                
                if self.excelapp.enabled:
                    futures['excelapp'] = executor.submit(self.send_to_app, self.excelapp, excelapp_batch)
                
                if self.rails.enabled:
                    futures['rails'] = executor.submit(self.send_to_app, self.rails, rails_batch)
                
                for app_name, future in futures.items():
                    try:
                        success, error = future.result(timeout=120)  # 2분 타임아웃
                        results[app_name]['success'] = success
                        results[app_name]['error'] = error
                    except concurrent.futures.TimeoutError:
                        results[app_name]['error'] = "Timeout"
                        logger.error(f"{app_name} sync timed out")
                    except Exception as e:
                        results[app_name]['error'] = str(e)
                        logger.error(f"{app_name} sync failed: {e}")
        else:
            # 순차 전송
            if self.excelapp.enabled:
                success, error = self.send_to_app(self.excelapp, excelapp_batch)
                results['excelapp']['success'] = success
                results['excelapp']['error'] = error
            
            if self.rails.enabled:
                success, error = self.send_to_app(self.rails, rails_batch)
                results['rails']['success'] = success
                results['rails']['error'] = error
        
        return results
    
    def update_app_stats(self, state: Dict[str, Any], app_name: str, success: bool, data_count: int):
        """앱별 통계 업데이트"""
        app_state = state[app_name]
        app_state['sync_count'] += 1
        
        if success:
            app_state['last_sync'] = datetime.utcnow().isoformat()
            app_state['total_synced'] += data_count
        
        # 성공률 계산
        if app_state['sync_count'] > 0:
            success_count = app_state['total_synced'] / max(data_count, 1) if data_count > 0 else 0
            app_state['success_rate'] = min(success_count / app_state['sync_count'], 1.0)
    
    def sync_batch(self) -> bool:
        """한 번의 동기화 배치 실행"""
        logger.info("Starting dual sync batch...")
        
        # 동기화 상태 로드
        state = self.load_sync_state()
        
        # 새 데이터 조회 (두 앱 중 더 뒤처진 것 기준)
        excelapp_last_id = state.get('excelapp', {}).get('last_processed_id')
        rails_last_id = state.get('rails', {}).get('last_processed_id')
        
        # 더 적은 ID를 기준으로 데이터 조회
        last_processed_id = None
        if excelapp_last_id and rails_last_id:
            last_processed_id = min(excelapp_last_id, rails_last_id)
        elif excelapp_last_id:
            last_processed_id = excelapp_last_id
        elif rails_last_id:
            last_processed_id = rails_last_id
        
        new_data = self.get_new_data(last_processed_id)
        
        if not new_data:
            logger.info("No new data to sync")
            return True
        
        logger.info(f"Found {len(new_data)} new items to sync")
        
        # 각 앱용 배치 데이터 준비
        excelapp_batch = self.prepare_batch_for_excelapp(new_data)
        rails_batch = self.prepare_batch_for_rails(new_data)
        
        if not excelapp_batch['data'] and not rails_batch['data']:
            logger.warning("No valid data after preparation")
            return True
        
        # 양쪽 앱에 전송
        results = self.send_to_both_apps(excelapp_batch, rails_batch)
        
        # 결과 처리 및 상태 업데이트
        overall_success = False
        
        for app_name, result in results.items():
            if result['success']:
                overall_success = True
                
                # 성공한 경우 상태 업데이트
                state[app_name]['last_processed_id'] = new_data[-1]['id']
                self.update_app_stats(state, app_name, True, len(new_data))
                
                logger.info(f"{app_name} sync successful: {len(new_data)} items")
            else:
                self.update_app_stats(state, app_name, False, 0)
                logger.error(f"{app_name} sync failed: {result['error']}")
        
        # 전체 상태 업데이트
        if overall_success:
            state['last_sync'] = datetime.utcnow().isoformat()
            state['total_synced'] = state.get('total_synced', 0) + len(new_data)
        
        self.save_sync_state(state)
        
        logger.info(f"Dual sync batch completed. ExcelApp: {'✓' if results['excelapp']['success'] else '✗'}, Rails: {'✓' if results['rails']['success'] else '✗'}")
        
        return overall_success
    
    def check_app_status(self, app_config: AppConfig) -> bool:
        """개별 앱 상태 확인"""
        if not app_config.enabled:
            return True
        
        try:
            headers = {'X-PipeData-Token': app_config.api_token}
            response = requests.get(
                app_config.api_url,
                headers=headers,
                timeout=10
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Error checking {app_config.name} status: {e}")
            return False
    
    def print_status_report(self):
        """상태 리포트 출력"""
        state = self.load_sync_state()
        
        logger.info("=== DUAL SYNC STATUS REPORT ===")
        logger.info(f"Last sync: {state.get('last_sync', 'Never')}")
        logger.info(f"Total synced: {state.get('total_synced', 0)}")
        
        for app_name in ['excelapp', 'rails']:
            app_state = state.get(app_name, {})
            app_config = getattr(self, app_name)
            
            logger.info(f"\n{app_config.name}:")
            logger.info(f"  Enabled: {'Yes' if app_config.enabled else 'No'}")
            logger.info(f"  Last sync: {app_state.get('last_sync', 'Never')}")
            logger.info(f"  Total synced: {app_state.get('total_synced', 0)}")
            logger.info(f"  Sync count: {app_state.get('sync_count', 0)}")
            logger.info(f"  Success rate: {app_state.get('success_rate', 0.0):.2%}")
            logger.info(f"  Failed batches: {len(app_state.get('failed_batches', []))}")
        
        logger.info("=============================")
    
    def run_continuous_sync(self):
        """연속 동기화 실행"""
        logger.info("Starting continuous dual sync...")
        self.print_status_report()
        
        while True:
            try:
                # 메인 동기화
                self.sync_batch()
                
                # 상태 리포트 (매 10회마다)
                state = self.load_sync_state()
                total_syncs = sum(state.get(app, {}).get('sync_count', 0) for app in ['excelapp', 'rails'])
                if total_syncs % 10 == 0:
                    self.print_status_report()
                
                # 대기
                logger.info(f"Waiting {self.config.sync_interval_hours} hours until next sync...")
                time.sleep(self.config.sync_interval_hours * 3600)
                
            except KeyboardInterrupt:
                logger.info("Dual sync stopped by user")
                self.print_status_report()
                break
            except Exception as e:
                logger.error(f"Unexpected error in dual sync loop: {e}")
                time.sleep(300)  # 5분 대기 후 재시도

def main():
    """메인 함수"""
    # 환경 변수에서 설정 로드
    excelapp_config = AppConfig(
        name="ExcelApp (Next.js)",
        api_url=os.getenv('EXCELAPP_API_URL', 'https://your-excelapp-domain.com/api/training/pipedata'),
        api_token=os.getenv('EXCELAPP_API_TOKEN', 'your-excelapp-token'),
        enabled=os.getenv('EXCELAPP_ENABLED', 'true').lower() == 'true'
    )
    
    rails_config = AppConfig(
        name="ExcelApp-Rails",
        api_url=os.getenv('RAILS_API_URL', 'https://your-rails-domain.com/api/v1/pipedata'),
        api_token=os.getenv('RAILS_API_TOKEN', 'your-rails-token'),
        enabled=os.getenv('RAILS_ENABLED', 'true').lower() == 'true'
    )
    
    sync_config = DualSyncConfig(
        batch_size=int(os.getenv('SYNC_BATCH_SIZE', '50')),
        sync_interval_hours=int(os.getenv('SYNC_INTERVAL_HOURS', '6')),
        quality_threshold=float(os.getenv('QUALITY_THRESHOLD', '7.0')),
        parallel_send=os.getenv('PARALLEL_SEND', 'true').lower() == 'true'
    )
    
    syncer = DualExcelAppSyncer(excelapp_config, rails_config, sync_config)
    
    # 실행 모드 결정
    if os.getenv('SYNC_MODE') == 'once':
        # 한 번만 동기화 실행 (테스트용)
        syncer.sync_batch()
        syncer.print_status_report()
    else:
        # 연속 동기화 실행
        syncer.run_continuous_sync()

if __name__ == "__main__":
    main()