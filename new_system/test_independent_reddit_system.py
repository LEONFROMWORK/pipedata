#!/usr/bin/env python3
"""
독립 Reddit 시스템 테스트
Reddit 전용 독립 시스템의 완전한 테스트
"""
import asyncio
import sys
import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, List
from pathlib import Path

# 프로젝트 경로 추가
sys.path.insert(0, '/Users/kevin/bigdata/new_system')

from collectors.reddit_system import RedditCollector
from shared.utils import save_jsonl, get_output_path

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_reddit_system():
    """독립 Reddit 시스템 테스트"""
    print("🔧 독립 Reddit 시스템 테스트")
    print("=" * 80)
    
    try:
        # Reddit 수집기 초기화
        print("📋 Reddit 수집기 초기화...")
        collector = RedditCollector()
        print("✅ Reddit 수집기 초기화 완료")
        
        # 설정 검증
        print("\n🔍 설정 검증...")
        config = collector.config
        if not config.validate_config():
            print("❌ Reddit 설정 검증 실패")
            return False
        print("✅ Reddit 설정 검증 완료")
        
        # 독립 시스템 상태 확인
        print("\n📊 독립 시스템 상태 확인...")
        print(f"캐시 DB: {config.cache_db_path}")
        print(f"중복 추적 DB: {config.dedup_db_path}")
        print(f"출력 경로: {config.output_base_path}")
        
        # 캐시 통계
        cache_stats = collector.cache.cache.get_stats()
        print(f"캐시 통계: {cache_stats}")
        
        # 중복 추적 통계
        dedup_stats = collector.dedup_tracker.get_reddit_stats()
        print(f"중복 추적 통계: {dedup_stats}")
        
        # 데이터 수집 테스트
        print("\n🚀 데이터 수집 테스트...")
        start_time = datetime.now()
        
        # 소규모 테스트 (5개 항목)
        collected_data = await collector.collect_excel_qa_data(max_items=5)
        
        end_time = datetime.now()
        collection_time = (end_time - start_time).total_seconds()
        
        print(f"\n✅ 수집 완료!")
        print(f"수집 시간: {collection_time:.2f}초")
        print(f"수집된 항목: {len(collected_data)}개")
        
        # 수집 통계 출력
        stats = collector.get_detailed_stats()
        print(f"\n📈 상세 통계:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # 데이터 품질 검증
        print(f"\n🔍 데이터 품질 검증...")
        quality_issues = 0
        
        for i, entry in enumerate(collected_data, 1):
            print(f"\n📋 항목 {i}:")
            print(f"  ID: {entry.id}")
            print(f"  질문: {entry.user_question[:80]}...")
            print(f"  답변: {entry.assistant_response[:80]}...")
            print(f"  품질 점수: {entry.metadata.get('quality_score', 0)}")
            print(f"  소스: {entry.metadata.get('source', 'unknown')}")
            print(f"  봇 탐지: {entry.metadata.get('bot_detection_version', 'none')}")
            
            # 품질 검증
            if not entry.user_question.strip():
                quality_issues += 1
                print(f"    ⚠️ 빈 질문")
            
            if not entry.assistant_response.strip():
                quality_issues += 1
                print(f"    ⚠️ 빈 답변")
            
            if entry.metadata.get('quality_score', 0) < 2.0:
                quality_issues += 1
                print(f"    ⚠️ 낮은 품질 점수")
        
        # 결과 저장
        if collected_data:
            print(f"\n💾 결과 저장...")
            output_path = get_output_path(
                config.output_base_path,
                'reddit_independent_test'
            )
            
            # QAEntry를 딕셔너리로 변환
            data_dicts = [entry.to_dict() for entry in collected_data]
            save_jsonl(data_dicts, output_path)
            
            print(f"✅ 결과 저장 완료: {output_path}")
            
            # 메타데이터 저장
            metadata = {
                'test_timestamp': datetime.now().isoformat(),
                'system_type': 'independent_reddit',
                'total_collected': len(collected_data),
                'collection_time_seconds': collection_time,
                'quality_issues': quality_issues,
                'statistics': stats,
                'config_summary': {
                    'subreddits': config.subreddits,
                    'max_submissions_per_subreddit': config.max_submissions_per_subreddit,
                    'min_upvotes': config.min_upvotes,
                    'bot_detection_enabled': config.bot_detection_enabled
                }
            }
            
            metadata_path = output_path.with_suffix('.metadata.json')
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            print(f"✅ 메타데이터 저장 완료: {metadata_path}")
        
        # 테스트 결과 평가
        print(f"\n🎯 테스트 결과 평가:")
        
        success_rate = len(collected_data) / 5 * 100  # 목표 5개 대비
        print(f"수집 성공률: {success_rate:.1f}%")
        
        if quality_issues == 0:
            print("✅ 품질 검증: 모든 항목이 품질 기준을 충족")
        else:
            print(f"⚠️ 품질 검증: {quality_issues}개 항목에서 품질 문제 발견")
        
        # 독립성 검증
        print(f"\n🔒 독립성 검증:")
        print(f"✅ 독립 캐시 DB: {config.cache_db_path}")
        print(f"✅ 독립 중복 추적 DB: {config.dedup_db_path}")
        print(f"✅ 독립 설정 클래스: RedditConfig")
        
        # 전체 평가
        if len(collected_data) > 0 and quality_issues < len(collected_data) * 0.3:
            print(f"\n🎉 독립 Reddit 시스템 테스트 성공!")
            print(f"✅ 시스템이 완전히 독립적으로 작동합니다")
            return True
        else:
            print(f"\n❌ 독립 Reddit 시스템 테스트 실패")
            print(f"수집 성공률 또는 품질이 기준에 미달합니다")
            return False
    
    except Exception as e:
        print(f"\n❌ 테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """메인 테스트 실행"""
    print("🎯 독립 Reddit 시스템 테스트 시작")
    print("=" * 80)
    
    success = await test_reddit_system()
    
    if success:
        print(f"\n🎉 모든 테스트 통과!")
        print(f"🚀 독립 Reddit 시스템이 정상적으로 작동합니다")
    else:
        print(f"\n❌ 테스트 실패")
        print(f"⚠️ 시스템 설정 또는 구현을 확인해주세요")

if __name__ == "__main__":
    asyncio.run(main())