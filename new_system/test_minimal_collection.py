#!/usr/bin/env python3
"""
최소한의 수집 테스트
"""
import asyncio
import logging
from pathlib import Path
import sys
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from pipeline.main_pipeline import ExcelQAPipeline
from core.cache import APICache, LocalCache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_minimal_collection():
    """최소한의 데이터 수집 테스트"""
    
    logger.info("🧪 최소한의 수집 테스트")
    logger.info("=" * 40)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/minimal_test.db"))
    cache = APICache(local_cache)
    
    # 파이프라인 초기화 (파라미터 없음)
    pipeline = ExcelQAPipeline()
    
    # 매우 제한적인 수집 설정
    from_date = datetime.now() - timedelta(days=7)
    
    try:
        logger.info("🇰🇷 오빠두 한국 커뮤니티 데이터 수집 시작 (최대 1페이지, 3개 항목)")
        
        result = await pipeline.run_full_pipeline(
            from_date=from_date,
            max_pages=1,           # 1페이지로 제한 (오빠두 테스트)
            target_count=3,        # 3개로 제한
            sources=["oppadu"]     # 오빠두만 테스트
        )
        
        logger.info(f"✅ 수집 완료!")
        logger.info(f"   데이터 플로우: {result.get('data_flow', {})}")
        logger.info(f"   최종 출력: {result.get('final_output_path', 'None')}")
        
        # 생성된 파일 확인
        output_path = result.get('final_output_path')
        if output_path and Path(output_path).exists():
            with open(output_path, 'r') as f:
                lines = f.readlines()
            logger.info(f"   파일 라인 수: {len(lines)}")
            
            if lines:
                import json
                sample = json.loads(lines[0])
                logger.info(f"   샘플 질문: {sample.get('user_question', '')[:50]}...")
                logger.info(f"   답변 길이: {len(sample.get('assistant_response', ''))} 문자")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ 수집 실패: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

if __name__ == "__main__":
    asyncio.run(test_minimal_collection())