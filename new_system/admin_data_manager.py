"""
관리자용 데이터 관리 시스템
- 수집된 데이터 검토 및 다운로드
- 수동 전송 승인/거부
- 데이터 품질 관리
"""

import os
import json
import sqlite3
import logging
import hashlib
import zipfile
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import csv
from pathlib import Path

logger = logging.getLogger(__name__)

@dataclass
class AdminConfig:
    """관리자 설정"""
    admin_token: str
    data_retention_days: int = 30
    max_batch_size: int = 1000
    export_formats: List[str] = None
    
    def __post_init__(self):
        if self.export_formats is None:
            self.export_formats = ['json', 'csv', 'excel']

@dataclass
class DataBatch:
    """데이터 배치 정보"""
    batch_id: str
    created_at: datetime
    total_items: int
    avg_quality_score: float
    sources: List[str]
    status: str  # 'pending', 'approved', 'rejected', 'sent'
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    notes: Optional[str] = None

class AdminDataManager:
    """관리자용 데이터 관리 클래스"""
    
    def __init__(self, config: AdminConfig):
        self.config = config
        self.db_path = '../data/combined_dataset.db'
        self.admin_db_path = '../data/admin_management.db'
        self.export_dir = Path('../data/exports')
        self.export_dir.mkdir(exist_ok=True)
        
        self._init_admin_database()
    
    def _init_admin_database(self):
        """관리자 데이터베이스 초기화"""
        try:
            conn = sqlite3.connect(self.admin_db_path)
            
            # 배치 관리 테이블
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_batches (
                    batch_id TEXT PRIMARY KEY,
                    created_at TIMESTAMP,
                    total_items INTEGER,
                    avg_quality_score REAL,
                    sources TEXT,  -- JSON array
                    status TEXT DEFAULT 'pending',
                    reviewed_by TEXT,
                    reviewed_at TIMESTAMP,
                    notes TEXT,
                    metadata TEXT  -- JSON object
                )
            """)
            
            # 관리자 액션 로그
            conn.execute("""
                CREATE TABLE IF NOT EXISTS admin_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id TEXT,
                    action_type TEXT,  -- review, approve, reject, download, etc.
                    target_id TEXT,    -- batch_id or data_id
                    details TEXT,      -- JSON object
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 전송 이력
            conn.execute("""
                CREATE TABLE IF NOT EXISTS transmission_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT,
                    sent_at TIMESTAMP,
                    sent_by TEXT,
                    items_count INTEGER,
                    success_count INTEGER,
                    error_count INTEGER,
                    response_data TEXT,  -- JSON response from ExcelApp
                    FOREIGN KEY (batch_id) REFERENCES data_batches (batch_id)
                )
            """)
            
            conn.commit()
            conn.close()
            logger.info("Admin database initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing admin database: {e}")
    
    def verify_admin_token(self, token: str) -> bool:
        """관리자 토큰 검증"""
        return token == self.config.admin_token
    
    def create_data_batch(self, min_quality: float = 7.0, max_items: int = None) -> str:
        """새로운 데이터 배치 생성"""
        try:
            if max_items is None:
                max_items = self.config.max_batch_size
            
            # 미검토 데이터 조회
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            
            query = """
            SELECT 
                id, question, answer, code_snippets, excel_functions,
                difficulty, quality_score, source, tags, metadata, created_at
            FROM processed_qa_data 
            WHERE quality_score >= ?
            AND is_processed = 1
            AND id NOT IN (
                SELECT DISTINCT data_id FROM reviewed_data WHERE status != 'rejected'
            )
            ORDER BY quality_score DESC, created_at DESC
            LIMIT ?
            """
            
            cursor = conn.execute(query, [min_quality, max_items])
            data_items = [dict(row) for row in cursor.fetchall()]
            conn.close()
            
            if not data_items:
                raise ValueError("No new data available for batch creation")
            
            # 배치 정보 계산
            total_items = len(data_items)
            avg_quality = sum(item['quality_score'] for item in data_items) / total_items
            sources = list(set(item['source'] for item in data_items))
            
            # 배치 ID 생성
            batch_data = json.dumps(data_items, sort_keys=True, default=str)
            batch_id = hashlib.md5(batch_data.encode()).hexdigest()[:16]
            
            # 배치 정보 저장
            admin_conn = sqlite3.connect(self.admin_db_path)
            admin_conn.execute("""
                INSERT OR REPLACE INTO data_batches 
                (batch_id, created_at, total_items, avg_quality_score, sources, status, metadata)
                VALUES (?, ?, ?, ?, ?, 'pending', ?)
            """, [
                batch_id,
                datetime.now(),
                total_items,
                avg_quality,
                json.dumps(sources),
                json.dumps({
                    'data_ids': [item['id'] for item in data_items],
                    'min_quality': min_quality,
                    'created_by': 'system'
                })
            ])
            admin_conn.commit()
            admin_conn.close()
            
            logger.info(f"Created batch {batch_id} with {total_items} items")
            return batch_id
            
        except Exception as e:
            logger.error(f"Error creating data batch: {e}")
            raise
    
    def get_pending_batches(self) -> List[DataBatch]:
        """대기 중인 배치 목록 조회"""
        try:
            conn = sqlite3.connect(self.admin_db_path)
            conn.row_factory = sqlite3.Row
            
            cursor = conn.execute("""
                SELECT * FROM data_batches 
                WHERE status = 'pending'
                ORDER BY created_at DESC
            """)
            
            batches = []
            for row in cursor.fetchall():
                batch = DataBatch(
                    batch_id=row['batch_id'],
                    created_at=datetime.fromisoformat(row['created_at']),
                    total_items=row['total_items'],
                    avg_quality_score=row['avg_quality_score'],
                    sources=json.loads(row['sources']),
                    status=row['status'],
                    reviewed_by=row['reviewed_by'],
                    reviewed_at=datetime.fromisoformat(row['reviewed_at']) if row['reviewed_at'] else None,
                    notes=row['notes']
                )
                batches.append(batch)
            
            conn.close()
            return batches
            
        except Exception as e:
            logger.error(f"Error getting pending batches: {e}")
            return []
    
    def get_batch_data(self, batch_id: str) -> List[Dict[str, Any]]:
        """배치 데이터 상세 조회"""
        try:
            # 배치 메타데이터 조회
            admin_conn = sqlite3.connect(self.admin_db_path)
            admin_conn.row_factory = sqlite3.Row
            
            cursor = admin_conn.execute(
                "SELECT metadata FROM data_batches WHERE batch_id = ?",
                [batch_id]
            )
            
            batch_row = cursor.fetchone()
            if not batch_row:
                raise ValueError(f"Batch {batch_id} not found")
            
            metadata = json.loads(batch_row['metadata'])
            data_ids = metadata.get('data_ids', [])
            admin_conn.close()
            
            # 실제 데이터 조회
            if not data_ids:
                return []
            
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            
            placeholders = ','.join(['?'] * len(data_ids))
            query = f"""
            SELECT 
                id, question, answer, code_snippets, excel_functions,
                difficulty, quality_score, source, tags, metadata, created_at
            FROM processed_qa_data 
            WHERE id IN ({placeholders})
            ORDER BY quality_score DESC
            """
            
            cursor = conn.execute(query, data_ids)
            data_items = [dict(row) for row in cursor.fetchall()]
            conn.close()
            
            return data_items
            
        except Exception as e:
            logger.error(f"Error getting batch data: {e}")
            return []
    
    def export_batch_data(self, batch_id: str, format: str = 'json', admin_id: str = None) -> str:
        """배치 데이터를 파일로 내보내기"""
        try:
            if format not in self.config.export_formats:
                raise ValueError(f"Unsupported format: {format}")
            
            data_items = self.get_batch_data(batch_id)
            if not data_items:
                raise ValueError(f"No data found for batch {batch_id}")
            
            # 파일명 생성
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"batch_{batch_id}_{timestamp}"
            
            if format == 'json':
                filepath = self.export_dir / f"{filename}.json"
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump({
                        'batch_id': batch_id,
                        'exported_at': datetime.now().isoformat(),
                        'total_items': len(data_items),
                        'data': data_items
                    }, f, indent=2, ensure_ascii=False, default=str)
            
            elif format == 'csv':
                filepath = self.export_dir / f"{filename}.csv"
                
                # 데이터 평면화
                flattened_data = []
                for item in data_items:
                    flat_item = {
                        'id': item['id'],
                        'question': item['question'],
                        'answer': item['answer'],
                        'difficulty': item['difficulty'],
                        'quality_score': item['quality_score'],
                        'source': item['source'],
                        'created_at': item['created_at'],
                        'excel_functions': ', '.join(json.loads(item.get('excel_functions', '[]'))),
                        'code_snippets': ' | '.join(json.loads(item.get('code_snippets', '[]'))),
                        'tags': ', '.join(json.loads(item.get('tags', '[]')))
                    }
                    flattened_data.append(flat_item)
                
                df = pd.DataFrame(flattened_data)
                df.to_csv(filepath, index=False, encoding='utf-8')
            
            elif format == 'excel':
                filepath = self.export_dir / f"{filename}.xlsx"
                
                # 메인 데이터 시트
                flattened_data = []
                for item in data_items:
                    flat_item = {
                        'ID': item['id'],
                        'Question': item['question'],
                        'Answer': item['answer'],
                        'Difficulty': item['difficulty'],
                        'Quality Score': item['quality_score'],
                        'Source': item['source'],
                        'Created At': item['created_at'],
                        'Excel Functions': ', '.join(json.loads(item.get('excel_functions', '[]'))),
                        'Code Snippets': ' | '.join(json.loads(item.get('code_snippets', '[]'))),
                        'Tags': ', '.join(json.loads(item.get('tags', '[]')))
                    }
                    flattened_data.append(flat_item)
                
                with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                    # 메인 데이터
                    df_main = pd.DataFrame(flattened_data)
                    df_main.to_excel(writer, sheet_name='Data', index=False)
                    
                    # 통계 시트
                    stats_data = {
                        'Metric': ['Total Items', 'Average Quality', 'Sources', 'Difficulties'],
                        'Value': [
                            len(data_items),
                            f"{sum(item['quality_score'] for item in data_items) / len(data_items):.2f}",
                            ', '.join(set(item['source'] for item in data_items)),
                            ', '.join(set(item['difficulty'] for item in data_items))
                        ]
                    }
                    df_stats = pd.DataFrame(stats_data)
                    df_stats.to_excel(writer, sheet_name='Statistics', index=False)
            
            # 액션 로깅
            self._log_admin_action(
                admin_id or 'system',
                'download',
                batch_id,
                {'format': format, 'filepath': str(filepath), 'items_count': len(data_items)}
            )
            
            logger.info(f"Exported batch {batch_id} to {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Error exporting batch data: {e}")
            raise
    
    def review_batch(self, batch_id: str, action: str, admin_id: str, notes: str = None) -> bool:
        """배치 검토 (승인/거부)"""
        try:
            if action not in ['approve', 'reject']:
                raise ValueError("Action must be 'approve' or 'reject'")
            
            # 배치 상태 업데이트
            admin_conn = sqlite3.connect(self.admin_db_path)
            admin_conn.execute("""
                UPDATE data_batches 
                SET status = ?, reviewed_by = ?, reviewed_at = ?, notes = ?
                WHERE batch_id = ?
            """, [action + 'd', admin_id, datetime.now(), notes, batch_id])
            
            admin_conn.commit()
            admin_conn.close()
            
            # 액션 로깅
            self._log_admin_action(
                admin_id,
                'review',
                batch_id,
                {'action': action, 'notes': notes}
            )
            
            logger.info(f"Batch {batch_id} {action}d by {admin_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error reviewing batch: {e}")
            return False
    
    def send_approved_batch(self, batch_id: str, admin_id: str) -> Dict[str, Any]:
        """승인된 배치를 ExcelApp으로 전송"""
        try:
            # 배치 상태 확인
            admin_conn = sqlite3.connect(self.admin_db_path)
            admin_conn.row_factory = sqlite3.Row
            
            cursor = admin_conn.execute(
                "SELECT status FROM data_batches WHERE batch_id = ?",
                [batch_id]
            )
            
            batch_row = cursor.fetchone()
            if not batch_row:
                raise ValueError(f"Batch {batch_id} not found")
            
            if batch_row['status'] != 'approved':
                raise ValueError(f"Batch {batch_id} is not approved for sending")
            
            admin_conn.close()
            
            # 배치 데이터 조회 및 전송
            from excelapp_sync import ExcelAppSyncer, SyncConfig
            
            config = SyncConfig(
                excelapp_api_url=os.getenv('EXCELAPP_API_URL'),
                api_token=os.getenv('EXCELAPP_API_TOKEN')
            )
            
            syncer = ExcelAppSyncer(config)
            data_items = self.get_batch_data(batch_id)
            
            # 배치 데이터 준비
            batch_data = syncer.prepare_batch(data_items)
            batch_data['batch_info']['manual_send'] = True
            batch_data['batch_info']['sent_by'] = admin_id
            batch_data['batch_info']['batch_id'] = batch_id
            
            # ExcelApp으로 전송
            success = syncer.send_to_excelapp(batch_data)
            
            if success:
                # 배치 상태를 'sent'로 변경
                admin_conn = sqlite3.connect(self.admin_db_path)
                admin_conn.execute(
                    "UPDATE data_batches SET status = 'sent' WHERE batch_id = ?",
                    [batch_id]
                )
                
                # 전송 이력 기록
                admin_conn.execute("""
                    INSERT INTO transmission_history 
                    (batch_id, sent_at, sent_by, items_count, success_count, error_count, response_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [
                    batch_id,
                    datetime.now(),
                    admin_id,
                    len(data_items),
                    len(data_items),
                    0,
                    json.dumps({'success': True, 'manual_send': True})
                ])
                
                admin_conn.commit()
                admin_conn.close()
                
                # 액션 로깅
                self._log_admin_action(
                    admin_id,
                    'send',
                    batch_id,
                    {'items_count': len(data_items), 'success': True}
                )
                
                return {
                    'success': True,
                    'batch_id': batch_id,
                    'items_sent': len(data_items),
                    'message': 'Batch sent successfully'
                }
            else:
                # 전송 실패 기록
                admin_conn = sqlite3.connect(self.admin_db_path)
                admin_conn.execute("""
                    INSERT INTO transmission_history 
                    (batch_id, sent_at, sent_by, items_count, success_count, error_count, response_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [
                    batch_id,
                    datetime.now(),
                    admin_id,
                    len(data_items),
                    0,
                    len(data_items),
                    json.dumps({'success': False, 'error': 'Send failed'})
                ])
                admin_conn.commit()
                admin_conn.close()
                
                return {
                    'success': False,
                    'batch_id': batch_id,
                    'message': 'Failed to send batch to ExcelApp'
                }
            
        except Exception as e:
            logger.error(f"Error sending batch: {e}")
            return {
                'success': False,
                'batch_id': batch_id,
                'message': f'Error: {str(e)}'
            }
    
    def get_transmission_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """전송 이력 조회"""
        try:
            conn = sqlite3.connect(self.admin_db_path)
            conn.row_factory = sqlite3.Row
            
            cursor = conn.execute("""
                SELECT th.*, db.total_items, db.avg_quality_score, db.sources
                FROM transmission_history th
                LEFT JOIN data_batches db ON th.batch_id = db.batch_id
                ORDER BY th.sent_at DESC
                LIMIT ?
            """, [limit])
            
            history = []
            for row in cursor.fetchall():
                history.append(dict(row))
            
            conn.close()
            return history
            
        except Exception as e:
            logger.error(f"Error getting transmission history: {e}")
            return []
    
    def get_admin_stats(self) -> Dict[str, Any]:
        """관리자 대시보드 통계"""
        try:
            admin_conn = sqlite3.connect(self.admin_db_path)
            admin_conn.row_factory = sqlite3.Row
            
            # 배치 통계
            cursor = admin_conn.execute("""
                SELECT 
                    COUNT(*) as total_batches,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_batches,
                    SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) as approved_batches,
                    SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as sent_batches,
                    SUM(total_items) as total_items,
                    AVG(avg_quality_score) as overall_avg_quality
                FROM data_batches
            """)
            
            batch_stats = dict(cursor.fetchone())
            
            # 전송 통계
            cursor = admin_conn.execute("""
                SELECT 
                    COUNT(*) as total_transmissions,
                    SUM(items_count) as total_items_sent,
                    SUM(success_count) as total_success,
                    SUM(error_count) as total_errors
                FROM transmission_history
                WHERE sent_at >= datetime('now', '-30 days')
            """)
            
            transmission_stats = dict(cursor.fetchone())
            
            admin_conn.close()
            
            return {
                'batch_stats': batch_stats,
                'transmission_stats': transmission_stats,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting admin stats: {e}")
            return {}
    
    def _log_admin_action(self, admin_id: str, action_type: str, target_id: str, details: Dict[str, Any]):
        """관리자 액션 로깅"""
        try:
            conn = sqlite3.connect(self.admin_db_path)
            conn.execute("""
                INSERT INTO admin_actions 
                (admin_id, action_type, target_id, details)
                VALUES (?, ?, ?, ?)
            """, [admin_id, action_type, target_id, json.dumps(details)])
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error logging admin action: {e}")
    
    def cleanup_old_data(self):
        """오래된 데이터 정리"""
        try:
            cutoff_date = datetime.now() - timedelta(days=self.config.data_retention_days)
            
            conn = sqlite3.connect(self.admin_db_path)
            
            # 오래된 배치 정리
            cursor = conn.execute(
                "DELETE FROM data_batches WHERE created_at < ? AND status IN ('sent', 'rejected')",
                [cutoff_date]
            )
            deleted_batches = cursor.rowcount
            
            # 오래된 액션 로그 정리
            cursor = conn.execute(
                "DELETE FROM admin_actions WHERE timestamp < ?",
                [cutoff_date]
            )
            deleted_actions = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            logger.info(f"Cleaned up {deleted_batches} old batches and {deleted_actions} old actions")
            
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")

def main():
    """테스트용 메인 함수"""
    config = AdminConfig(
        admin_token=os.getenv('ADMIN_TOKEN', 'test-admin-token'),
        data_retention_days=30
    )
    
    manager = AdminDataManager(config)
    
    # 테스트: 새 배치 생성
    try:
        batch_id = manager.create_data_batch(min_quality=7.0, max_items=10)
        print(f"Created batch: {batch_id}")
        
        # 배치 데이터 내보내기
        filepath = manager.export_batch_data(batch_id, 'json', 'test-admin')
        print(f"Exported to: {filepath}")
        
        # 배치 승인
        manager.review_batch(batch_id, 'approve', 'test-admin', 'Test approval')
        print(f"Approved batch: {batch_id}")
        
    except Exception as e:
        print(f"Test error: {e}")

if __name__ == "__main__":
    main()