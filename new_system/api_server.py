#!/usr/bin/env python3
"""
BigData Pipeline API Server
대시보드와 통신하기 위한 Flask API 서버
"""

import os
import sys
import json
import threading
import subprocess
import asyncio
from datetime import datetime
from pathlib import Path
from flask import Flask, request, jsonify
from flask_cors import CORS

# 현재 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pipeline.main_pipeline import ExcelQAPipeline
from pipeline.continuous_collector import ContinuousCollector
from config import Config

app = Flask(__name__)
CORS(app)  # CORS 활성화

# Railway 배포를 위한 포트 설정
PORT = int(os.environ.get('PORT', 8000))

# 전역 변수로 파이프라인 상태 관리
pipeline_status = {
    "status": "idle",
    "current_stage": "대기",
    "collected_count": 0,
    "processed_count": 0,
    "quality_filtered_count": 0,
    "final_count": 0,
    "errors": [],
    "last_execution": None,
    "next_scheduled": None
}

pipeline_logs = []
pipeline_thread = None
stop_pipeline_flag = False

# 지속적 수집 관리
continuous_collector = None
continuous_mode = False

def log_message(message):
    """로그 메시지 추가"""
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    log_entry = f"{timestamp} {message}"
    pipeline_logs.append(log_entry)
    # 최대 100개 로그만 유지
    if len(pipeline_logs) > 100:
        pipeline_logs.pop(0)
    print(log_entry)

@app.route('/api/status', methods=['GET'])
def get_status():
    """파이프라인 상태 조회"""
    try:
        # 캐시 통계
        cache_stats = {
            "total_entries": 0,
            "estimated_size_bytes": 0
        }
        
        # 최근 데이터셋 파일들
        output_dir = Path("/Users/kevin/bigdata/new_system/output")
        recent_datasets = []
        
        if output_dir.exists():
            for file_path in output_dir.glob("*.jsonl"):
                try:
                    stat = file_path.stat()
                    line_count = 0
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            line_count = sum(1 for _ in f)
                    except:
                        pass
                    
                    recent_datasets.append({
                        "filename": file_path.name,
                        "path": str(file_path),
                        "size_bytes": stat.st_size,
                        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                        "line_count": line_count,
                        "metadata": {
                            "source": "reddit" if "reddit" in file_path.name else "combined",
                            "format": "TRD"
                        }
                    })
                except Exception as e:
                    log_message(f"파일 정보 읽기 오류: {file_path} - {e}")
        
        # 최신 파일 순으로 정렬
        recent_datasets.sort(key=lambda x: x["modified"], reverse=True)
        recent_datasets = recent_datasets[:10]  # 최근 10개만
        
        response = {
            "pipeline_status": pipeline_status["status"],
            "cache_stats": cache_stats,
            "recent_datasets": recent_datasets,
            "execution_info": {
                "current_stage": pipeline_status["current_stage"],
                "collected_count": pipeline_status["collected_count"],
                "processed_count": pipeline_status["processed_count"],
                "quality_filtered_count": pipeline_status["quality_filtered_count"],
                "final_count": pipeline_status["final_count"],
                "errors": pipeline_status["errors"],
                "last_execution": pipeline_status["last_execution"],
                "next_scheduled": pipeline_status["next_scheduled"]
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return jsonify(response)
    
    except Exception as e:
        log_message(f"상태 조회 오류: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/logs', methods=['GET'])
def get_logs():
    """파이프라인 로그 조회"""
    try:
        return jsonify({
            "logs": pipeline_logs[-50:],  # 최근 50개 로그만 반환
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/run-continuous', methods=['POST'])
def run_continuous():
    """지속적 데이터 수집 시작"""
    global pipeline_thread, pipeline_status, continuous_collector, continuous_mode
    
    try:
        # 이미 실행 중인지 확인
        if pipeline_status["status"] == "running":
            return jsonify({
                "status": "error", 
                "message": "파이프라인이 이미 실행 중입니다."
            }), 400
        
        # 요청 파라미터 파싱
        data = request.get_json() or {}
        sources_param = data.get('sources', ['reddit', 'oppadu'])  # 오빠두 추가
        
        if isinstance(sources_param, list):
            sources = sources_param
        else:
            sources = sources_param.split(',')
            
        max_per_batch = int(data.get('max_per_batch', 5))
        
        log_message(f"지속적 수집 시작: sources={sources}, max_per_batch={max_per_batch}")
        
        # 지속적 수집 모드 활성화
        continuous_mode = True
        continuous_collector = ContinuousCollector()
        
        # 파이프라인 상태 업데이트
        pipeline_status.update({
            "status": "running",
            "current_stage": "지속적 수집 시작",
            "collected_count": 0,
            "processed_count": 0,
            "quality_filtered_count": 0,
            "final_count": 0,
            "errors": []
        })
        
        # 백그라운드에서 실행
        def run_continuous_collection():
            global pipeline_status, continuous_mode
            
            try:
                # asyncio 루프 생성
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                # 지속적 수집 실행
                result = loop.run_until_complete(
                    continuous_collector.start_continuous_collection(
                        sources=sources,
                        max_per_batch=max_per_batch
                    )
                )
                
                # 최종 상태 업데이트
                if result:
                    cumulative = result.get('cumulative_data_flow', {})
                    pipeline_status.update({
                        "status": "completed",
                        "current_stage": "지속적 수집 완료",
                        "collected_count": cumulative.get('total_collected', 0),
                        "processed_count": cumulative.get('total_processed', 0),
                        "final_count": cumulative.get('total_final', 0)
                    })
                    
                    log_message(f"지속적 수집 완료: 총 {cumulative.get('total_final', 0)}개 항목 생성")
                
            except Exception as e:
                pipeline_status.update({
                    "status": "error",
                    "current_stage": "오류 발생",
                    "errors": [str(e)]
                })
                log_message(f"지속적 수집 오류: {e}")
            
            finally:
                continuous_mode = False
                
        pipeline_thread = threading.Thread(target=run_continuous_collection)
        pipeline_thread.start()
        
        return jsonify({
            "status": "started",
            "message": f"지속적 수집이 시작되었습니다. 데이터 소스: {', '.join(sources)}"
        })
        
    except Exception as e:
        log_message(f"지속적 수집 시작 실패: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"지속적 수집 시작 실패: {str(e)}"
        }), 500

@app.route('/api/run-pipeline', methods=['POST'])
def run_pipeline():
    """파이프라인 실행"""
    global pipeline_thread, pipeline_status
    
    try:
        # 이미 실행 중인지 확인
        if pipeline_status["status"] == "running":
            return jsonify({
                "status": "error",
                "message": "파이프라인이 이미 실행 중입니다."
            }), 400
        
        # 요청 파라미터 파싱
        data = request.get_json() or {}
        sources_param = data.get('sources', 'stackoverflow,reddit,oppadu')  # 오빠두 추가
        
        # sources가 리스트인지 문자열인지 확인
        if isinstance(sources_param, list):
            sources = sources_param
        else:
            sources = sources_param.split(',')
        
        max_pages = int(data.get('max_pages', 10))
        target_count = int(data.get('target_count', 100))
        incremental = data.get('incremental', True)
        hours_back = int(data.get('hours_back', 24))
        
        log_message(f"파이프라인 시작 요청: sources={sources}, max_pages={max_pages}, target_count={target_count}")
        
        # 정지 플래그 초기화
        stop_pipeline_flag = False
        
        # 파이프라인 상태 업데이트
        pipeline_status.update({
            "status": "running",
            "current_stage": "초기화",
            "collected_count": 0,
            "processed_count": 0,
            "quality_filtered_count": 0,
            "final_count": 0,
            "errors": [],
            "last_execution": datetime.now().isoformat()
        })
        
        # 백그라운드에서 파이프라인 실행
        def run_pipeline_thread():
            async def async_pipeline_execution():
                try:
                    # 정지 플래그 확인
                    if stop_pipeline_flag:
                        pipeline_status.update({
                            "status": "stopped",
                            "current_stage": "정지됨"
                        })
                        log_message("파이프라인이 시작 전에 정지되었습니다")
                        return
                    
                    pipeline_status["current_stage"] = "데이터 수집"
                    log_message("BigData 파이프라인 시작")
                    
                    # 실제 파이프라인 실행
                    pipeline = ExcelQAPipeline()
                    
                    # 정지 플래그 확인
                    if stop_pipeline_flag:
                        pipeline_status.update({
                            "status": "stopped",
                            "current_stage": "정지됨"
                        })
                        log_message("파이프라인이 정지되었습니다")
                        return
                    
                    # 설정 업데이트
                    if 'reddit' in sources:
                        pipeline_status["current_stage"] = "Reddit 데이터 수집"
                        log_message("Reddit 데이터 수집 시작")
                    
                    if 'stackoverflow' in sources:
                        pipeline_status["current_stage"] = "StackOverflow 데이터 수집"
                        log_message("StackOverflow 데이터 수집 시작")
                    
                    # 정지 플래그 확인
                    if stop_pipeline_flag:
                        pipeline_status.update({
                            "status": "stopped",
                            "current_stage": "정지됨"
                        })
                        log_message("파이프라인이 데이터 수집 전에 정지되었습니다")
                        return
                    
                    # 파이프라인 실행 (await 사용)
                    # Set from_date to collect from last 30 days for better results
                    from datetime import timedelta
                    from_date = datetime.now() - timedelta(days=30)
                    
                    log_message(f"파이프라인 실행 시작: sources={sources}, max_pages={max_pages}, target_count={target_count}, from_date={from_date}")
                    result = await pipeline.run_full_pipeline(
                        from_date=from_date,
                        sources=sources,
                        max_pages=max_pages,
                        target_count=target_count
                    )
                    
                    log_message(f"파이프라인 실행 결과: {result}")
                    
                    # 결과에서 실제 데이터 플로우 정보 추출
                    data_flow = result.get("data_flow", {})
                    final_count = data_flow.get("final_output", 0)
                    collected_count = data_flow.get("collected", 0)
                    processed_count = data_flow.get("processed", 0)
                    
                    # 결과 업데이트
                    pipeline_status.update({
                        "status": "completed",
                        "current_stage": "완료",
                        "final_count": final_count,
                        "collected_count": collected_count,
                        "processed_count": processed_count
                    })
                    
                    log_message(f"파이프라인 완료: 수집={collected_count}, 처리={processed_count}, 최종={final_count}개")
                    
                except Exception as e:
                    pipeline_status.update({
                        "status": "error",
                        "current_stage": "오류",
                        "errors": [str(e)]
                    })
                    log_message(f"파이프라인 실행 오류: {e}")
                
                finally:
                    if pipeline_status["status"] == "running":
                        pipeline_status["status"] = "idle"
            
            # 새로운 이벤트 루프에서 비동기 함수 실행
            try:
                # 기존 이벤트 루프가 있으면 새로 생성
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(async_pipeline_execution())
            except Exception as e:
                log_message(f"이벤트 루프 오류: {e}")
                pipeline_status.update({
                    "status": "error",
                    "current_stage": "오류",
                    "errors": [str(e)]
                })
            finally:
                try:
                    loop.close()
                except:
                    pass
        
        pipeline_thread = threading.Thread(target=run_pipeline_thread)
        pipeline_thread.daemon = True
        pipeline_thread.start()
        
        return jsonify({
            "status": "started",
            "message": f"파이프라인이 시작되었습니다. 데이터 소스: {', '.join(sources)}"
        })
        
    except Exception as e:
        log_message(f"파이프라인 시작 오류: {e}")
        pipeline_status["status"] = "error"
        pipeline_status["errors"] = [str(e)]
        return jsonify({
            "status": "error",
            "message": f"파이프라인 시작 실패: {e}"
        }), 500

@app.route('/api/stop-pipeline', methods=['POST'])
def stop_pipeline():
    """파이프라인 정지 (일반 및 지속적 수집 모두)"""
    global stop_pipeline_flag, pipeline_status, continuous_collector, continuous_mode
    
    try:
        if pipeline_status["status"] != "running":
            return jsonify({
                "status": "error",
                "message": "실행 중인 파이프라인이 없습니다."
            }), 400
        
        # 지속적 수집 모드인 경우
        if continuous_mode and continuous_collector:
            continuous_collector.stop_collection()
            log_message("지속적 수집 정지 요청 받음")
        else:
            # 일반 파이프라인 정지
            stop_pipeline_flag = True
            log_message("파이프라인 정지 요청 받음")
        
        # 상태 업데이트
        pipeline_status.update({
            "status": "stopping",
            "current_stage": "정지 중"
        })
        
        return jsonify({
            "status": "stopping",
            "message": "파이프라인 정지가 요청되었습니다."
        })
        
    except Exception as e:
        log_message(f"파이프라인 정지 오류: {e}")
        return jsonify({
            "status": "error",
            "message": f"파이프라인 정지 실패: {e}"
        }), 500

@app.route('/api/datasets', methods=['GET'])
def get_datasets():
    """데이터셋 목록 조회"""
    try:
        output_dir = Path("/Users/kevin/bigdata/new_system/output")
        datasets = []
        
        if output_dir.exists():
            for file_path in output_dir.glob("*.jsonl"):
                try:
                    stat = file_path.stat()
                    line_count = 0
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            line_count = sum(1 for _ in f)
                    except:
                        pass
                    
                    datasets.append({
                        "filename": file_path.name,
                        "path": str(file_path),
                        "size_bytes": stat.st_size,
                        "line_count": line_count,
                        "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                        "format": "TRD",
                        "source": "reddit" if "reddit" in file_path.name else "combined"
                    })
                except Exception as e:
                    log_message(f"데이터셋 정보 읽기 오류: {file_path} - {e}")
        
        # 생성일 순으로 정렬
        datasets.sort(key=lambda x: x["created"], reverse=True)
        
        return jsonify({"datasets": datasets})
        
    except Exception as e:
        log_message(f"데이터셋 조회 오류: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/cache/cleanup', methods=['POST'])
def cleanup_cache():
    """캐시 정리"""
    try:
        # 캐시 파일들 정리
        cache_files = [
            "/Users/kevin/bigdata/new_system/data/cache.db",
            "/Users/kevin/bigdata/new_system/data/test_cache.db"
        ]
        
        total_freed = 0
        for cache_file in cache_files:
            if os.path.exists(cache_file):
                size = os.path.getsize(cache_file)
                os.remove(cache_file)
                total_freed += size
                log_message(f"캐시 파일 삭제: {cache_file} ({size} bytes)")
        
        message = f"캐시가 정리되었습니다. {total_freed / (1024*1024):.1f}MB의 공간이 확보되었습니다."
        log_message(message)
        
        return jsonify({
            "status": "success",
            "message": message,
            "bytes_freed": total_freed
        })
        
    except Exception as e:
        log_message(f"캐시 정리 오류: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """헬스 체크"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    })

# ExcelApp 동기화 관련 엔드포인트
@app.route('/api/sync-to-excelapp', methods=['POST'])
def sync_to_excelapp():
    """ExcelApp에 데이터 동기화"""
    try:
        from excelapp_sync import ExcelAppSyncer, SyncConfig
        
        # 설정 로드
        config = SyncConfig(
            excelapp_api_url=os.getenv('EXCELAPP_API_URL'),
            api_token=os.getenv('EXCELAPP_API_TOKEN'),
            batch_size=int(os.getenv('SYNC_BATCH_SIZE', '50')),
            quality_threshold=float(os.getenv('QUALITY_THRESHOLD', '7.0'))
        )
        
        syncer = ExcelAppSyncer(config)
        success = syncer.sync_batch()
        
        if success:
            return jsonify({
                "status": "success",
                "message": "ExcelApp 동기화가 성공적으로 완료되었습니다."
            })
        else:
            return jsonify({
                "status": "error", 
                "message": "ExcelApp 동기화 중 오류가 발생했습니다."
            }), 500
            
    except Exception as e:
        log_message(f"ExcelApp 동기화 오류: {e}")
        return jsonify({
            "status": "error",
            "message": f"동기화 오류: {str(e)}"
        }), 500

@app.route('/api/sync-status', methods=['GET'])
def sync_status():
    """동기화 상태 조회"""
    try:
        from excelapp_sync import ExcelAppSyncer, SyncConfig
        
        config = SyncConfig(
            excelapp_api_url=os.getenv('EXCELAPP_API_URL'),
            api_token=os.getenv('EXCELAPP_API_TOKEN')
        )
        
        syncer = ExcelAppSyncer(config)
        state = syncer.load_sync_state()
        
        return jsonify({
            "status": "success",
            "sync_state": state,
            "excelapp_status": syncer.check_excelapp_status()
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# ============================================================================
# 관리자용 API 엔드포인트
# ============================================================================

def verify_admin_auth():
    """관리자 인증 확인"""
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return False
    
    token = auth_header.split(' ')[1]
    admin_token = os.getenv('ADMIN_TOKEN')
    
    return token == admin_token

@app.route('/api/admin/batches/create', methods=['POST'])
def admin_create_batch():
    """관리자: 새 데이터 배치 생성"""
    if not verify_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        from admin_data_manager import AdminDataManager, AdminConfig
        
        config = AdminConfig(
            admin_token=os.getenv('ADMIN_TOKEN'),
            data_retention_days=int(os.getenv('DATA_RETENTION_DAYS', '30'))
        )
        
        manager = AdminDataManager(config)
        
        # 요청 파라미터
        data = request.get_json() or {}
        min_quality = data.get('min_quality', 7.0)
        max_items = data.get('max_items', 1000)
        
        batch_id = manager.create_data_batch(min_quality, max_items)
        
        return jsonify({
            "status": "success",
            "batch_id": batch_id,
            "message": f"Batch created with ID: {batch_id}"
        })
        
    except Exception as e:
        log_message(f"Admin batch creation error: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/admin/batches/pending', methods=['GET'])
def admin_get_pending_batches():
    """관리자: 대기 중인 배치 목록 조회"""
    if not verify_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        from admin_data_manager import AdminDataManager, AdminConfig
        
        config = AdminConfig(admin_token=os.getenv('ADMIN_TOKEN'))
        manager = AdminDataManager(config)
        
        batches = manager.get_pending_batches()
        
        # DataBatch 객체를 dict로 변환
        batch_list = []
        for batch in batches:
            batch_dict = {
                'batch_id': batch.batch_id,
                'created_at': batch.created_at.isoformat(),
                'total_items': batch.total_items,
                'avg_quality_score': batch.avg_quality_score,
                'sources': batch.sources,
                'status': batch.status,
                'reviewed_by': batch.reviewed_by,
                'reviewed_at': batch.reviewed_at.isoformat() if batch.reviewed_at else None,
                'notes': batch.notes
            }
            batch_list.append(batch_dict)
        
        return jsonify({
            "status": "success",
            "batches": batch_list
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/admin/batches/<batch_id>/data', methods=['GET'])
def admin_get_batch_data(batch_id):
    """관리자: 배치 데이터 상세 조회"""
    if not verify_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        from admin_data_manager import AdminDataManager, AdminConfig
        
        config = AdminConfig(admin_token=os.getenv('ADMIN_TOKEN'))
        manager = AdminDataManager(config)
        
        data_items = manager.get_batch_data(batch_id)
        
        return jsonify({
            "status": "success",
            "batch_id": batch_id,
            "total_items": len(data_items),
            "data": data_items
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/admin/batches/<batch_id>/export', methods=['POST'])
def admin_export_batch(batch_id):
    """관리자: 배치 데이터 내보내기"""
    if not verify_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        from admin_data_manager import AdminDataManager, AdminConfig
        
        config = AdminConfig(admin_token=os.getenv('ADMIN_TOKEN'))
        manager = AdminDataManager(config)
        
        # 요청 파라미터
        data = request.get_json() or {}
        format_type = data.get('format', 'json')
        admin_id = data.get('admin_id', 'unknown')
        
        filepath = manager.export_batch_data(batch_id, format_type, admin_id)
        
        return jsonify({
            "status": "success",
            "batch_id": batch_id,
            "format": format_type,
            "filepath": filepath,
            "message": f"Data exported to {filepath}"
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/admin/batches/<batch_id>/review', methods=['POST'])
def admin_review_batch(batch_id):
    """관리자: 배치 검토 (승인/거부)"""
    if not verify_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        from admin_data_manager import AdminDataManager, AdminConfig
        
        config = AdminConfig(admin_token=os.getenv('ADMIN_TOKEN'))
        manager = AdminDataManager(config)
        
        # 요청 파라미터
        data = request.get_json()
        if not data:
            return jsonify({"error": "Missing request data"}), 400
        
        action = data.get('action')  # 'approve' or 'reject'
        admin_id = data.get('admin_id')
        notes = data.get('notes', '')
        
        if not action or not admin_id:
            return jsonify({"error": "Missing required fields: action, admin_id"}), 400
        
        success = manager.review_batch(batch_id, action, admin_id, notes)
        
        if success:
            return jsonify({
                "status": "success",
                "batch_id": batch_id,
                "action": action,
                "message": f"Batch {action}ed successfully"
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Failed to review batch"
            }), 500
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/admin/batches/<batch_id>/send', methods=['POST'])
def admin_send_batch(batch_id):
    """관리자: 승인된 배치 수동 전송"""
    if not verify_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        from admin_data_manager import AdminDataManager, AdminConfig
        
        config = AdminConfig(admin_token=os.getenv('ADMIN_TOKEN'))
        manager = AdminDataManager(config)
        
        # 요청 파라미터
        data = request.get_json() or {}
        admin_id = data.get('admin_id', 'unknown')
        
        result = manager.send_approved_batch(batch_id, admin_id)
        
        if result['success']:
            return jsonify({
                "status": "success",
                **result
            })
        else:
            return jsonify({
                "status": "error",
                **result
            }), 500
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/admin/transmission-history', methods=['GET'])
def admin_get_transmission_history():
    """관리자: 전송 이력 조회"""
    if not verify_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        from admin_data_manager import AdminDataManager, AdminConfig
        
        config = AdminConfig(admin_token=os.getenv('ADMIN_TOKEN'))
        manager = AdminDataManager(config)
        
        limit = request.args.get('limit', 50, type=int)
        history = manager.get_transmission_history(limit)
        
        return jsonify({
            "status": "success",
            "history": history
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/admin/stats', methods=['GET'])
def admin_get_stats():
    """관리자: 대시보드 통계"""
    if not verify_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        from admin_data_manager import AdminDataManager, AdminConfig
        
        config = AdminConfig(admin_token=os.getenv('ADMIN_TOKEN'))
        manager = AdminDataManager(config)
        
        stats = manager.get_admin_stats()
        
        return jsonify({
            "status": "success",
            "stats": stats
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/admin/cleanup', methods=['POST'])
def admin_cleanup_data():
    """관리자: 오래된 데이터 정리"""
    if not verify_admin_auth():
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        from admin_data_manager import AdminDataManager, AdminConfig
        
        config = AdminConfig(admin_token=os.getenv('ADMIN_TOKEN'))
        manager = AdminDataManager(config)
        
        manager.cleanup_old_data()
        
        return jsonify({
            "status": "success",
            "message": "Old data cleaned up successfully"
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    log_message("BigData Pipeline API Server 시작")
    log_message(f"API 서버 URL: http://0.0.0.0:{PORT}")
    
    # Flask 앱 실행
    app.run(
        host='0.0.0.0',
        port=PORT,
        debug=False,
        threaded=True
    )