#!/usr/bin/env python3
"""
파이프라인 정지 후 지속적 수집 테스트
"""
import asyncio
import aiohttp
import logging
import time
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def stop_and_test_continuous():
    """파이프라인 정지 후 지속적 수집 테스트"""
    
    base_url = "http://127.0.0.1:8000"
    
    async with aiohttp.ClientSession() as session:
        
        # 1. 현재 상태 확인
        logger.info("📊 현재 파이프라인 상태 확인")
        async with session.get(f"{base_url}/api/status") as response:
            if response.status == 200:
                status = await response.json()
                logger.info(f"   현재 상태: {status.get('status')}")
                logger.info(f"   현재 단계: {status.get('current_stage')}")
            else:
                logger.error("   상태 확인 실패")
                return
        
        # 2. 파이프라인 정지
        logger.info("\n🛑 파이프라인 정지")
        async with session.post(f"{base_url}/api/stop-pipeline") as response:
            if response.status == 200:
                result = await response.json()
                logger.info(f"   정지 결과: {result}")
            else:
                error = await response.text()
                logger.error(f"   정지 실패: {error}")
        
        # 3. 잠시 대기
        logger.info("\n⏳ 3초 대기...")
        await asyncio.sleep(3)
        
        # 4. 상태 재확인
        logger.info("\n📊 정지 후 상태 확인")
        async with session.get(f"{base_url}/api/status") as response:
            if response.status == 200:
                status = await response.json()
                logger.info(f"   정지 후 상태: {status.get('status')}")
            else:
                logger.error("   상태 확인 실패")
                return
        
        # 5. 지속적 수집 시작
        logger.info("\n🚀 지속적 수집 시작")
        payload = {
            "sources": ["reddit"],
            "max_per_batch": 2
        }
        
        async with session.post(
            f"{base_url}/api/run-continuous",
            json=payload,
            headers={"Content-Type": "application/json"}
        ) as response:
            
            if response.status == 200:
                result = await response.json()
                logger.info(f"   ✅ 지속적 수집 시작: {result}")
            else:
                error = await response.text()
                logger.error(f"   ❌ 시작 실패: {error}")
                return
        
        # 6. 10초간 모니터링
        logger.info("\n👀 10초간 진행 모니터링")
        for i in range(4):  # 10초 = 4 x 2.5초
            await asyncio.sleep(2.5)
            
            async with session.get(f"{base_url}/api/status") as response:
                if response.status == 200:
                    status = await response.json()
                    logger.info(f"   [{i+1}] 상태: {status.get('status')} | "
                              f"수집: {status.get('collected_count', 0)} | "
                              f"처리: {status.get('processed_count', 0)} | "
                              f"출력: {status.get('final_count', 0)}")
        
        # 7. 정지
        logger.info("\n🛑 테스트 완료 - 정지")
        async with session.post(f"{base_url}/api/stop-pipeline") as response:
            if response.status == 200:
                result = await response.json()
                logger.info(f"   정지 완료: {result}")
        
        # 8. 최종 상태
        logger.info("\n📋 최종 상태")
        async with session.get(f"{base_url}/api/status") as response:
            if response.status == 200:
                status = await response.json()
                logger.info(f"   최종 상태: {status.get('status')}")
                logger.info(f"   총 수집: {status.get('collected_count', 0)}개")
                logger.info(f"   총 처리: {status.get('processed_count', 0)}개")
                logger.info(f"   최종 출력: {status.get('final_count', 0)}개")

if __name__ == "__main__":
    asyncio.run(stop_and_test_continuous())