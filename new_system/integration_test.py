"""
PipeData → ExcelApp 통합 테스트
전체 파이프라인의 동작을 검증합니다.
"""

import os
import sys
import json
import time
import logging
import requests
import sqlite3
from datetime import datetime
from typing import Dict, Any, List
import tempfile

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IntegrationTester:
    """통합 테스트 클래스"""
    
    def __init__(self):
        self.test_results = {
            'passed': 0,
            'failed': 0,
            'errors': [],
            'start_time': datetime.now()
        }
        
        # 테스트 설정
        self.pipedata_api_url = os.getenv('PIPEDATA_API_URL', 'http://localhost:8000')
        self.excelapp_api_url = os.getenv('EXCELAPP_API_URL', 'http://localhost:3000/api/training/pipedata')
        self.api_token = os.getenv('EXCELAPP_API_TOKEN', 'test-token')
    
    def log_test(self, test_name: str, passed: bool, message: str = ""):
        """테스트 결과 로깅"""
        status = "PASS" if passed else "FAIL"
        logger.info(f"[{status}] {test_name}: {message}")
        
        if passed:
            self.test_results['passed'] += 1
        else:
            self.test_results['failed'] += 1
            self.test_results['errors'].append({
                'test': test_name,
                'message': message,
                'timestamp': datetime.now().isoformat()
            })
    
    def test_pipedata_health(self) -> bool:
        """PipeData API 서버 헬스 체크"""
        try:
            response = requests.get(f"{self.pipedata_api_url}/api/health", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                self.log_test("PipeData Health Check", True, f"Status: {data.get('status')}")
                return True
            else:
                self.log_test("PipeData Health Check", False, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test("PipeData Health Check", False, str(e))
            return False
    
    def test_excelapp_health(self) -> bool:
        """ExcelApp API 서버 헬스 체크"""
        try:
            headers = {'X-PipeData-Token': self.api_token}
            response = requests.get(self.excelapp_api_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                self.log_test("ExcelApp Health Check", True, "API responding")
                return True
            else:
                self.log_test("ExcelApp Health Check", False, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test("ExcelApp Health Check", False, str(e))
            return False
    
    def test_database_connection(self) -> bool:
        """데이터베이스 연결 테스트"""
        try:
            db_path = '../data/combined_dataset.db'
            
            if not os.path.exists(db_path):
                self.log_test("Database Connection", False, "Database file not found")
                return False
            
            conn = sqlite3.connect(db_path)
            cursor = conn.execute("SELECT COUNT(*) FROM processed_qa_data")
            count = cursor.fetchone()[0]
            conn.close()
            
            self.log_test("Database Connection", True, f"Found {count} records")
            return True
            
        except Exception as e:
            self.log_test("Database Connection", False, str(e))
            return False
    
    def test_data_preparation(self) -> bool:
        """데이터 준비 테스트"""
        try:
            from excelapp_sync import ExcelAppSyncer, SyncConfig
            
            config = SyncConfig(
                excelapp_api_url=self.excelapp_api_url,
                api_token=self.api_token,
                batch_size=5
            )
            
            syncer = ExcelAppSyncer(config)
            test_data = syncer.get_new_data(None)
            
            if test_data:
                # 배치 데이터 준비 테스트
                batch_data = syncer.prepare_batch(test_data[:2])
                
                if batch_data and batch_data.get('data'):
                    self.log_test("Data Preparation", True, f"Prepared {len(batch_data['data'])} items")
                    return True
                else:
                    self.log_test("Data Preparation", False, "No data prepared")
                    return False
            else:
                self.log_test("Data Preparation", False, "No test data found")
                return False
                
        except Exception as e:
            self.log_test("Data Preparation", False, str(e))
            return False
    
    def test_sync_functionality(self) -> bool:
        """동기화 기능 테스트"""
        try:
            # PipeData에서 동기화 트리거
            response = requests.post(
                f"{self.pipedata_api_url}/api/sync-to-excelapp",
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 'success':
                    self.log_test("Sync Functionality", True, result.get('message'))
                    return True
                else:
                    self.log_test("Sync Functionality", False, result.get('message'))
                    return False
            else:
                self.log_test("Sync Functionality", False, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test("Sync Functionality", False, str(e))
            return False
    
    def test_manual_data_send(self) -> bool:
        """수동 데이터 전송 테스트"""
        try:
            # 테스트 데이터 생성
            test_data = {
                'data': [{
                    'question': 'How to use VLOOKUP function in Excel?',
                    'answer': 'VLOOKUP function is used to search for a value in the first column of a table and return a value in the same row from another column.',
                    'code_snippets': ['=VLOOKUP(A2,B:C,2,FALSE)'],
                    'excel_functions': ['VLOOKUP'],
                    'difficulty': 'medium',
                    'quality_score': 8.5,
                    'source': 'integration_test',
                    'tags': ['vlookup', 'excel', 'lookup'],
                    'metadata': {
                        'test': True,
                        'timestamp': datetime.now().isoformat()
                    }
                }],
                'batch_info': {
                    'timestamp': datetime.now().isoformat(),
                    'source': 'integration_test',
                    'batch_size': 1
                }
            }
            
            headers = {
                'Content-Type': 'application/json',
                'X-PipeData-Token': self.api_token
            }
            
            response = requests.post(
                self.excelapp_api_url,
                json=test_data,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    self.log_test("Manual Data Send", True, f"Created: {result.get('created')}, Duplicates: {result.get('duplicates')}")
                    return True
                else:
                    self.log_test("Manual Data Send", False, "Request failed")
                    return False
            else:
                self.log_test("Manual Data Send", False, f"HTTP {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            self.log_test("Manual Data Send", False, str(e))
            return False
    
    def test_batch_processing(self) -> bool:
        """배치 처리 테스트"""
        try:
            from batch_optimizer import BatchOptimizer, BatchConfig
            
            config = BatchConfig(
                batch_size=10,
                max_workers=2,
                quality_threshold=7.0
            )
            
            optimizer = BatchOptimizer(config)
            
            # 작은 배치로 테스트
            test_data = optimizer.get_batch_data(0, 5)
            
            if test_data:
                processed = optimizer.process_batch_parallel(test_data)
                self.log_test("Batch Processing", True, f"Processed {len(processed)}/{len(test_data)} items")
                return True
            else:
                self.log_test("Batch Processing", False, "No test data available")
                return False
                
        except Exception as e:
            self.log_test("Batch Processing", False, str(e))
            return False
    
    def test_sync_status(self) -> bool:
        """동기화 상태 조회 테스트"""
        try:
            response = requests.get(
                f"{self.pipedata_api_url}/api/sync-status",
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                sync_state = result.get('sync_state', {})
                
                self.log_test("Sync Status", True, f"Total synced: {sync_state.get('total_synced', 0)}")
                return True
            else:
                self.log_test("Sync Status", False, f"HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test("Sync Status", False, str(e))
            return False
    
    def test_error_handling(self) -> bool:
        """오류 처리 테스트"""
        try:
            # 잘못된 토큰으로 테스트
            headers = {
                'Content-Type': 'application/json',
                'X-PipeData-Token': 'invalid-token'
            }
            
            response = requests.post(
                self.excelapp_api_url,
                json={'data': []},
                headers=headers,
                timeout=10
            )
            
            # 401 Unauthorized를 기대
            if response.status_code == 401:
                self.log_test("Error Handling", True, "Correctly rejected invalid token")
                return True
            else:
                self.log_test("Error Handling", False, f"Expected 401, got {response.status_code}")
                return False
                
        except Exception as e:
            self.log_test("Error Handling", False, str(e))
            return False
    
    def run_all_tests(self) -> Dict[str, Any]:
        """모든 테스트 실행"""
        logger.info("Starting integration tests...")
        
        # 테스트 순서 (의존성 고려)
        tests = [
            ("Database Connection", self.test_database_connection),
            ("PipeData Health", self.test_pipedata_health),
            ("ExcelApp Health", self.test_excelapp_health),
            ("Data Preparation", self.test_data_preparation),
            ("Batch Processing", self.test_batch_processing),
            ("Manual Data Send", self.test_manual_data_send),
            ("Sync Functionality", self.test_sync_functionality),
            ("Sync Status", self.test_sync_status),
            ("Error Handling", self.test_error_handling)
        ]
        
        for test_name, test_func in tests:
            logger.info(f"Running test: {test_name}")
            try:
                test_func()
            except Exception as e:
                self.log_test(test_name, False, f"Unexpected error: {str(e)}")
            
            time.sleep(1)  # 테스트 간 간격
        
        # 결과 요약
        self.test_results['end_time'] = datetime.now()
        duration = (self.test_results['end_time'] - self.test_results['start_time']).total_seconds()
        self.test_results['duration_seconds'] = duration
        
        total_tests = self.test_results['passed'] + self.test_results['failed']
        success_rate = (self.test_results['passed'] / total_tests * 100) if total_tests > 0 else 0
        
        logger.info("=" * 50)
        logger.info("INTEGRATION TEST RESULTS")
        logger.info("=" * 50)
        logger.info(f"Total Tests: {total_tests}")
        logger.info(f"Passed: {self.test_results['passed']}")
        logger.info(f"Failed: {self.test_results['failed']}")
        logger.info(f"Success Rate: {success_rate:.1f}%")
        logger.info(f"Duration: {duration:.1f} seconds")
        
        if self.test_results['errors']:
            logger.info("\nFailed Tests:")
            for error in self.test_results['errors']:
                logger.error(f"  - {error['test']}: {error['message']}")
        
        return self.test_results

def main():
    """메인 함수"""
    tester = IntegrationTester()
    results = tester.run_all_tests()
    
    # 결과를 파일로 저장
    with open('integration_test_results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    
    # 종료 코드 설정
    exit_code = 0 if results['failed'] == 0 else 1
    sys.exit(exit_code)

if __name__ == "__main__":
    main()