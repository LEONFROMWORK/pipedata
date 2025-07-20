#!/usr/bin/env python3
"""
지속적 수집 기능 테스트
"""
import asyncio
import json
import logging
import time
from pathlib import Path
import sys
import aiohttp

# Add project root to path
sys.path.append(str(Path(__file__).parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class ContinuousCollectionTester:
    """지속적 수집 테스트 클래스"""
    
    def __init__(self, base_url="http://127.0.0.1:8000"):
        self.base_url = base_url
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def check_api_server(self):
        """API 서버 상태 확인"""
        try:
            async with self.session.get(f"{self.base_url}/api/status") as response:
                if response.status == 200:
                    data = await response.json()
                    return True, data
                else:
                    return False, f"HTTP {response.status}"
        except Exception as e:
            return False, str(e)
    
    async def start_continuous_collection(self, sources=["reddit"], max_per_batch=3):
        """지속적 수집 시작"""
        payload = {
            "sources": sources,
            "max_per_batch": max_per_batch
        }
        
        try:
            async with self.session.post(
                f"{self.base_url}/api/run-continuous",
                json=payload,
                headers={"Content-Type": "application/json"}
            ) as response:
                
                if response.status == 200:
                    data = await response.json()
                    return True, data
                else:
                    error_text = await response.text()
                    return False, f"HTTP {response.status}: {error_text}"
                    
        except Exception as e:
            return False, str(e)
    
    async def get_pipeline_status(self):
        """파이프라인 상태 조회"""
        try:
            async with self.session.get(f"{self.base_url}/api/status") as response:
                if response.status == 200:
                    return await response.json()
                else:
                    return None
        except Exception as e:
            logger.error(f"Status check failed: {e}")
            return None
    
    async def stop_continuous_collection(self):
        """지속적 수집 정지"""
        try:
            async with self.session.post(f"{self.base_url}/api/stop-pipeline") as response:
                if response.status == 200:
                    data = await response.json()
                    return True, data
                else:
                    error_text = await response.text()
                    return False, f"HTTP {response.status}: {error_text}"
                    
        except Exception as e:
            return False, str(e)
    
    async def monitor_collection(self, duration_seconds=60):
        """지속적 수집 모니터링"""
        logger.info(f"🔍 {duration_seconds}초간 지속적 수집 모니터링 시작")
        
        start_time = time.time()
        last_counts = {}
        
        while time.time() - start_time < duration_seconds:
            status = await self.get_pipeline_status()
            
            if status:
                current_counts = {
                    'collected': status.get('collected_count', 0),
                    'processed': status.get('processed_count', 0),
                    'final': status.get('final_count', 0),
                    'status': status.get('status', 'unknown')
                }
                
                # 변화 감지
                if current_counts != last_counts:
                    logger.info(f"📊 상태 업데이트: {current_counts}")
                    
                    if current_counts['status'] == 'error':
                        errors = status.get('errors', [])
                        logger.error(f"❌ 오류 발생: {errors}")
                        break
                    
                    elif current_counts['status'] == 'completed':
                        logger.info("✅ 수집 완료")
                        break
                    
                    last_counts = current_counts
            
            await asyncio.sleep(5)  # 5초마다 체크
        
        logger.info("🏁 모니터링 종료")
        return last_counts

async def test_continuous_collection():
    """지속적 수집 전체 테스트"""
    
    logger.info("🚀 지속적 수집 기능 테스트 시작")
    logger.info("=" * 60)
    
    async with ContinuousCollectionTester() as tester:
        
        # 1. API 서버 상태 확인
        logger.info("1️⃣ API 서버 상태 확인")
        is_running, status_data = await tester.check_api_server()
        
        if not is_running:
            logger.error(f"❌ API 서버 연결 실패: {status_data}")
            logger.info("   대시보드가 실행 중인지 확인하세요:")
            logger.info("   cd /Users/kevin/bigdata/dashboard-ui && npm run dev")
            return
        
        logger.info(f"✅ API 서버 정상 작동: {status_data.get('status', 'unknown')}")
        
        # 2. 기존 파이프라인 정지 (혹시 실행 중인 경우)
        logger.info("\n2️⃣ 기존 파이프라인 정지")
        stop_success, stop_result = await tester.stop_continuous_collection()
        logger.info(f"   정지 결과: {stop_result}")
        
        # 잠시 대기
        await asyncio.sleep(2)
        
        # 3. 지속적 수집 시작
        logger.info("\n3️⃣ 지속적 수집 시작")
        sources = ["reddit"]  # Reddit만 테스트 (빠른 응답)
        max_per_batch = 3     # 작은 배치로 테스트
        
        start_success, start_result = await tester.start_continuous_collection(
            sources=sources,
            max_per_batch=max_per_batch
        )
        
        if not start_success:
            logger.error(f"❌ 지속적 수집 시작 실패: {start_result}")
            return
        
        logger.info(f"✅ 지속적 수집 시작됨: {start_result}")
        
        # 4. 수집 모니터링
        logger.info("\n4️⃣ 수집 진행 모니터링 (30초)")
        final_counts = await tester.monitor_collection(duration_seconds=30)
        
        # 5. 수집 정지
        logger.info("\n5️⃣ 지속적 수집 정지")
        stop_success, stop_result = await tester.stop_continuous_collection()
        logger.info(f"   정지 결과: {stop_result}")
        
        # 6. 최종 상태 확인
        logger.info("\n6️⃣ 최종 상태 확인")
        final_status = await tester.get_pipeline_status()
        
        if final_status:
            logger.info(f"   최종 상태: {final_status.get('status')}")
            logger.info(f"   수집된 데이터: {final_status.get('collected_count', 0)}개")
            logger.info(f"   처리된 데이터: {final_status.get('processed_count', 0)}개")
            logger.info(f"   최종 출력: {final_status.get('final_count', 0)}개")
        
        # 7. 생성된 파일 확인
        logger.info("\n7️⃣ 생성된 파일 확인")
        output_dir = Path("/Users/kevin/bigdata/data/output")
        
        # 최신 파일 찾기
        jsonl_files = list(output_dir.rglob("*.jsonl"))
        if jsonl_files:
            latest_file = max(jsonl_files, key=lambda x: x.stat().st_mtime)
            
            # 파일 크기 및 라인 수 확인
            file_size = latest_file.stat().st_size
            with open(latest_file, 'r') as f:
                line_count = sum(1 for _ in f)
            
            logger.info(f"   최신 파일: {latest_file.name}")
            logger.info(f"   파일 크기: {file_size} bytes")
            logger.info(f"   데이터 항목: {line_count}개")
            
            # 샘플 데이터 확인
            if line_count > 0:
                with open(latest_file, 'r') as f:
                    sample = json.loads(f.readline())
                    logger.info(f"   샘플 질문: {sample.get('user_question', '')[:50]}...")
                    logger.info(f"   답변 길이: {len(sample.get('assistant_response', ''))} 문자")
        else:
            logger.warning("   ⚠️ 생성된 JSONL 파일을 찾을 수 없음")

async def quick_continuous_test():
    """빠른 지속적 수집 테스트 (API만)"""
    
    logger.info("⚡ 빠른 지속적 수집 API 테스트")
    logger.info("=" * 40)
    
    async with ContinuousCollectionTester() as tester:
        
        # API 서버 확인
        is_running, status = await tester.check_api_server()
        if not is_running:
            logger.error(f"❌ API 서버 미실행: {status}")
            return
        
        logger.info("✅ API 서버 정상")
        
        # 지속적 수집 시작 요청
        start_success, result = await tester.start_continuous_collection(
            sources=["reddit"],
            max_per_batch=1
        )
        
        if start_success:
            logger.info(f"✅ 지속적 수집 시작: {result}")
            
            # 5초 후 상태 확인
            await asyncio.sleep(5)
            status = await tester.get_pipeline_status()
            logger.info(f"📊 5초 후 상태: {status.get('status', 'unknown')}")
            
            # 정지
            await tester.stop_continuous_collection()
            logger.info("🛑 테스트 완료")
            
        else:
            logger.error(f"❌ 지속적 수집 시작 실패: {result}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "quick":
        # 빠른 테스트
        asyncio.run(quick_continuous_test())
    else:
        # 전체 테스트
        asyncio.run(test_continuous_collection())