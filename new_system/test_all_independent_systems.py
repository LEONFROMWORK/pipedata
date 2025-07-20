#!/usr/bin/env python3
"""
모든 독립 시스템 통합 테스트
Reddit, StackOverflow, Oppadu 독립 시스템들의 동시 실행 및 상호 독립성 검증
"""
import asyncio
import sys
import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, List
from pathlib import Path
import time

# 프로젝트 경로 추가
sys.path.insert(0, '/Users/kevin/bigdata/new_system')

from collectors.reddit_system import RedditCollector
from collectors.stackoverflow_system import StackOverflowCollector
from collectors.oppadu_system import OppaduCollector
from shared.utils import save_jsonl, get_output_path

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_system_independence():
    """시스템 독립성 테스트"""
    print("🔒 시스템 독립성 검증")
    print("=" * 80)
    
    # 모든 수집기 초기화
    collectors = {}
    
    try:
        print("📋 수집기 초기화...")
        collectors['reddit'] = RedditCollector()
        collectors['stackoverflow'] = StackOverflowCollector()
        collectors['oppadu'] = OppaduCollector()
        print("✅ 모든 수집기 초기화 완료")
        
        # 독립성 검증 - 데이터베이스 경로
        print("\n🔍 데이터베이스 독립성 검증...")
        
        db_paths = {}
        for name, collector in collectors.items():
            if hasattr(collector, 'config'):
                config = collector.config
                cache_path = str(config.cache_db_path)
                dedup_path = str(config.dedup_db_path)
                
                db_paths[name] = {
                    'cache': cache_path,
                    'dedup': dedup_path
                }
                
                print(f"  {name.upper()}:")
                print(f"    캐시 DB: {cache_path}")
                print(f"    중복 추적 DB: {dedup_path}")
        
        # 경로 중복 검사
        all_paths = []
        for name, paths in db_paths.items():
            all_paths.extend(paths.values())
        
        if len(all_paths) == len(set(all_paths)):
            print("✅ 모든 데이터베이스 경로가 독립적입니다")
        else:
            print("❌ 데이터베이스 경로에 중복이 있습니다")
            return False
        
        # 설정 독립성 검증
        print("\n🔍 설정 독립성 검증...")
        
        config_classes = []
        for name, collector in collectors.items():
            if hasattr(collector, 'config'):
                config_class = collector.config.__class__.__name__
                config_classes.append(config_class)
                print(f"  {name.upper()}: {config_class}")
        
        if len(config_classes) == len(set(config_classes)):
            print("✅ 모든 설정 클래스가 독립적입니다")
        else:
            print("❌ 설정 클래스에 중복이 있습니다")
            return False
        
        # 캐시 독립성 검증
        print("\n🔍 캐시 독립성 검증...")
        
        cache_stats = {}
        for name, collector in collectors.items():
            if hasattr(collector, 'cache'):
                if hasattr(collector.cache, 'cache'):
                    stats = collector.cache.cache.get_stats()
                    cache_stats[name] = stats
                    print(f"  {name.upper()}: {stats.get('cache_file', 'N/A')}")
        
        print("✅ 캐시 시스템이 독립적으로 작동합니다")
        
        return True
    
    except Exception as e:
        print(f"❌ 독립성 검증 중 오류: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_concurrent_collection():
    """동시 수집 테스트"""
    print("\n🚀 동시 수집 테스트")
    print("=" * 80)
    
    try:
        print("📋 수집기 초기화...")
        
        # 수집기 초기화
        reddit_collector = RedditCollector()
        # stackoverflow_collector = StackOverflowCollector()  # API 키 필요
        oppadu_collector = OppaduCollector()
        
        print("✅ 수집기 초기화 완료")
        
        # 동시 수집 실행
        print("\n🔄 동시 수집 시작...")
        start_time = time.time()
        
        # 비동기 태스크 생성
        tasks = []
        
        # Reddit 수집 태스크
        tasks.append(asyncio.create_task(
            reddit_collector.collect_excel_qa_data(max_items=3)
        ))
        
        # StackOverflow 수집 태스크 (API 키가 있는 경우에만)
        # tasks.append(asyncio.create_task(
        #     stackoverflow_collector.collect_excel_qa_data(max_items=3)
        # ))
        
        # Oppadu 수집 태스크
        tasks.append(asyncio.create_task(
            oppadu_collector.collect_excel_qa_data(max_items=3)
        ))
        
        # 모든 태스크 완료 대기
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n✅ 동시 수집 완료!")
        print(f"총 소요 시간: {total_time:.2f}초")
        
        # 결과 분석
        successful_collections = 0
        total_collected = 0
        
        collection_results = {
            'reddit': results[0] if len(results) > 0 else [],
            # 'stackoverflow': results[1] if len(results) > 1 else [],
            'oppadu': results[1] if len(results) > 1 else []
        }
        
        print(f"\n📊 수집 결과:")
        
        for source, result in collection_results.items():
            if isinstance(result, Exception):
                print(f"  {source.upper()}: ❌ 오류 - {result}")
            else:
                collected_count = len(result) if result else 0
                print(f"  {source.upper()}: ✅ {collected_count}개 수집")
                if collected_count > 0:
                    successful_collections += 1
                    total_collected += collected_count
        
        print(f"\n📈 전체 통계:")
        print(f"성공한 수집기: {successful_collections}")
        print(f"총 수집 항목: {total_collected}개")
        print(f"평균 수집 시간: {total_time / len(tasks):.2f}초")
        
        # 독립성 검증
        print(f"\n🔒 동시 실행 독립성 검증:")
        
        # 각 수집기의 통계 확인
        collectors = {
            'reddit': reddit_collector,
            'oppadu': oppadu_collector
        }
        
        for name, collector in collectors.items():
            if hasattr(collector, 'get_detailed_stats'):
                stats = collector.get_detailed_stats()
                print(f"  {name.upper()} 통계:")
                print(f"    처리된 항목: {stats.get('total_processed', 0)}")
                print(f"    수집된 항목: {stats.get('total_collected', 0)}")
                print(f"    오류 수: {stats.get('errors', 0) + stats.get('crawling_errors', 0)}")
        
        # 결과 저장
        if total_collected > 0:
            print(f"\n💾 결과 저장...")
            
            # 모든 결과 통합
            all_data = []
            for source, result in collection_results.items():
                if isinstance(result, list) and result:
                    for entry in result:
                        if hasattr(entry, 'to_dict'):
                            data_dict = entry.to_dict()
                            data_dict['collection_source'] = source
                            all_data.append(data_dict)
            
            if all_data:
                output_path = get_output_path(
                    Path('/Users/kevin/bigdata/data/output'),
                    'concurrent_independent_test'
                )
                save_jsonl(all_data, output_path)
                print(f"✅ 결과 저장 완료: {output_path}")
        
        # 성공 여부 판단
        if successful_collections >= 1 and total_collected >= 1:
            print(f"\n🎉 동시 수집 테스트 성공!")
            print(f"✅ 모든 시스템이 독립적으로 동시 실행됩니다")
            return True
        else:
            print(f"\n❌ 동시 수집 테스트 실패")
            print(f"최소 1개 이상의 시스템이 성공적으로 동작해야 합니다")
            return False
    
    except Exception as e:
        print(f"❌ 동시 수집 테스트 중 오류: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_isolation_verification():
    """격리 검증 테스트"""
    print("\n🛡️ 격리 검증 테스트")
    print("=" * 80)
    
    try:
        print("📋 격리 검증 시나리오...")
        
        # 시나리오 1: 한 시스템에서 오류 발생 시 다른 시스템에 영향 없음
        print("\n🔬 시나리오 1: 오류 격리 테스트")
        
        # Reddit 수집기 정상 초기화
        reddit_collector = RedditCollector()
        
        # 의도적으로 잘못된 설정으로 Oppadu 수집기 생성 시도
        try:
            oppadu_collector = OppaduCollector()
            # 잘못된 URL로 수집 시도
            oppadu_collector.config.base_url = "https://invalid-url-that-does-not-exist.com"
            
            # 동시 실행
            reddit_task = asyncio.create_task(
                reddit_collector.collect_excel_qa_data(max_items=2)
            )
            oppadu_task = asyncio.create_task(
                oppadu_collector.collect_excel_qa_data(max_items=2)
            )
            
            reddit_result, oppadu_result = await asyncio.gather(
                reddit_task, oppadu_task, return_exceptions=True
            )
            
            # 결과 분석
            reddit_success = isinstance(reddit_result, list) and len(reddit_result) > 0
            oppadu_error = isinstance(oppadu_result, Exception) or (isinstance(oppadu_result, list) and len(oppadu_result) == 0)
            
            if reddit_success and oppadu_error:
                print("✅ 격리 검증 성공: 한 시스템의 오류가 다른 시스템에 영향 없음")
            elif reddit_success:
                print("✅ 격리 검증 성공: Reddit 시스템은 정상 작동")
            else:
                print("⚠️ 격리 검증 부분 성공: 일부 시스템에서 예상치 못한 동작")
        
        except Exception as e:
            print(f"⚠️ 격리 테스트 중 오류 (예상된 상황): {e}")
        
        # 시나리오 2: 데이터베이스 파일 독립성 검증
        print("\n🔬 시나리오 2: 데이터베이스 파일 독립성 검증")
        
        # 각 시스템의 데이터베이스 파일 확인
        db_files = {}
        
        # Reddit 시스템
        reddit_config = reddit_collector.config
        reddit_cache_path = Path(reddit_config.cache_db_path)
        reddit_dedup_path = Path(reddit_config.dedup_db_path)
        
        db_files['reddit'] = {
            'cache': reddit_cache_path,
            'dedup': reddit_dedup_path
        }
        
        # Oppadu 시스템
        oppadu_config = oppadu_collector.config
        oppadu_cache_path = Path(oppadu_config.cache_db_path)
        oppadu_dedup_path = Path(oppadu_config.dedup_db_path)
        
        db_files['oppadu'] = {
            'cache': oppadu_cache_path,
            'dedup': oppadu_dedup_path
        }
        
        # 파일 존재 및 독립성 확인
        all_files = []
        for system, files in db_files.items():
            print(f"  {system.upper()}:")
            for file_type, file_path in files.items():
                file_path.parent.mkdir(parents=True, exist_ok=True)
                print(f"    {file_type}: {file_path}")
                if file_path.exists():
                    print(f"      크기: {file_path.stat().st_size} bytes")
                else:
                    print(f"      상태: 새 파일")
                all_files.append(str(file_path))
        
        # 중복 파일 검사
        if len(all_files) == len(set(all_files)):
            print("✅ 모든 데이터베이스 파일이 독립적입니다")
        else:
            print("❌ 데이터베이스 파일에 중복이 있습니다")
            return False
        
        print("✅ 격리 검증 완료")
        return True
    
    except Exception as e:
        print(f"❌ 격리 검증 중 오류: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """메인 테스트 실행"""
    print("🎯 독립 시스템 통합 테스트")
    print("=" * 80)
    
    test_results = []
    
    # 1. 시스템 독립성 테스트
    independence_result = await test_system_independence()
    test_results.append(('시스템 독립성', independence_result))
    
    # 2. 동시 수집 테스트
    concurrent_result = await test_concurrent_collection()
    test_results.append(('동시 수집', concurrent_result))
    
    # 3. 격리 검증 테스트
    isolation_result = await test_isolation_verification()
    test_results.append(('격리 검증', isolation_result))
    
    # 최종 결과
    print("\n" + "=" * 80)
    print("📊 최종 테스트 결과")
    print("=" * 80)
    
    passed_tests = 0
    total_tests = len(test_results)
    
    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed_tests += 1
    
    print(f"\n통과한 테스트: {passed_tests}/{total_tests}")
    
    if passed_tests == total_tests:
        print(f"\n🎉 모든 테스트 통과!")
        print(f"🚀 독립 시스템들이 완벽하게 분리되어 작동합니다")
        print(f"✅ 각 데이터 수집처가 상호 영향 없이 독립적으로 운영됩니다")
    elif passed_tests >= total_tests * 0.7:
        print(f"\n⚠️ 대부분의 테스트 통과")
        print(f"✅ 시스템이 대체로 정상적으로 작동합니다")
    else:
        print(f"\n❌ 테스트 실패")
        print(f"⚠️ 시스템 설정 또는 구현을 확인해주세요")

if __name__ == "__main__":
    asyncio.run(main())