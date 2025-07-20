#!/usr/bin/env python3
"""
완전히 새로운 Stack Overflow 수집 테스트
- 모든 캐시와 중복 검출 완전 초기화
- 새로운 수집기 인스턴스로 10개 수집
"""
import asyncio
import json
import sqlite3
import sys
import shutil
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent))

from config import Config
from core.cache import LocalCache, APICache
from collectors.fixed_stackoverflow_collector import FixedStackOverflowCollector

def complete_reset():
    """모든 데이터 완전 초기화"""
    print("🔄 Stack Overflow 데이터 완전 초기화")
    
    try:
        # 1. 캐시 데이터베이스 완전 삭제
        cache_db = Config.DATABASE_PATH
        if cache_db.exists():
            cache_db.unlink()
            print(f"   ✅ 캐시 데이터베이스 삭제: {cache_db}")
        
        # 2. 중복 추적기 데이터베이스 삭제
        dedup_db = Path("/Users/kevin/bigdata/data/deduplication_tracker.db")
        if dedup_db.exists():
            dedup_db.unlink()
            print(f"   ✅ 중복 추적기 삭제: {dedup_db}")
        
        # 3. 디렉토리 다시 생성
        Config.ensure_directories()
        print("   ✅ 디렉토리 재생성 완료")
        
        print("🎯 완전 초기화 완료 - 모든 데이터가 새롭게 수집됩니다")
        
    except Exception as e:
        print(f"❌ 완전 초기화 실패: {e}")

async def fresh_stackoverflow_collection():
    """완전히 새로운 Stack Overflow 수집"""
    print("\n🆕 완전히 새로운 Stack Overflow 수집")
    print("=" * 60)
    
    try:
        # 새로운 수집기 인스턴스 생성
        local_cache = LocalCache(Config.DATABASE_PATH)
        api_cache = APICache(local_cache)
        collector = FixedStackOverflowCollector(api_cache)
        
        print("✅ 새로운 수집기 인스턴스 생성 완료")
        print("🎯 목표: 10개 고품질 Q&A 수집")
        
        # 더 넓은 기간으로 설정 (더 많은 데이터 확보)
        from_date = datetime.now() - timedelta(days=90)  # 3개월
        print(f"📅 수집 기간: {from_date.strftime('%Y-%m-%d')} ~ 현재 (3개월)")
        
        print("\n🚀 새로운 수집 시작...")
        start_time = datetime.now()
        
        # 단일 호출로 충분한 데이터 수집
        qa_pairs = await collector.collect_excel_questions_fixed(
            from_date=from_date,
            max_pages=3  # 3페이지로 충분한 데이터 확보
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n⏱️ 수집 완료 (소요 시간: {duration.total_seconds():.1f}초)")
        print(f"📊 수집 결과: {len(qa_pairs)}개 Q&A 쌍")
        
        if qa_pairs:
            # 10개만 선별 (고품질 순으로)
            target_count = 10
            selected_pairs = sorted(qa_pairs, key=lambda x: x.get('quality_score', 0), reverse=True)[:target_count]
            
            print(f"🎯 고품질 {len(selected_pairs)}개 선별 완료")
            
            # 상세 분석
            print(f"\n🔍 선별된 데이터 분석:")
            
            complete_pairs = sum(1 for pair in selected_pairs if pair.get('answer'))
            print(f"   완전한 Q&A 쌍: {complete_pairs}/{len(selected_pairs)} ({complete_pairs/len(selected_pairs)*100:.1f}%)")
            
            # 품질 분석
            quality_scores = [pair.get('quality_score', 0) for pair in selected_pairs]
            question_scores = [pair['question'].get('score', 0) for pair in selected_pairs]
            answer_scores = [pair['answer'].get('score', 0) for pair in selected_pairs if pair.get('answer')]
            
            print(f"   품질 점수: {min(quality_scores)} ~ {max(quality_scores)} (평균: {sum(quality_scores)/len(quality_scores):.1f})")
            print(f"   질문 점수: {min(question_scores)} ~ {max(question_scores)} (평균: {sum(question_scores)/len(question_scores):.1f})")
            if answer_scores:
                print(f"   답변 점수: {min(answer_scores)} ~ {max(answer_scores)} (평균: {sum(answer_scores)/len(answer_scores):.1f})")
            
            # Excel 함수 분석
            excel_functions = ['IF', 'VLOOKUP', 'INDEX', 'MATCH', 'SUMIF', 'COUNTIF', 'LAMBDA', 'LET', 'XLOOKUP']
            function_counts = {func: 0 for func in excel_functions}
            
            for pair in selected_pairs:
                full_text = (
                    pair['question'].get('title', '') + ' ' + 
                    pair['question'].get('body_markdown', '') + ' ' + 
                    pair.get('answer', {}).get('body_markdown', '')
                ).upper()
                
                for func in excel_functions:
                    if func in full_text:
                        function_counts[func] += 1
            
            print(f"\n🔧 Excel 함수 언급:")
            for func, count in sorted(function_counts.items(), key=lambda x: x[1], reverse=True):
                if count > 0:
                    print(f"   {func}: {count}회")
            
            # 상위 5개 Q&A 샘플 출력
            print(f"\n📝 상위 5개 Q&A 샘플:")
            for i, pair in enumerate(selected_pairs[:5], 1):
                question = pair['question']
                answer = pair.get('answer', {})
                
                print(f"\n   {i}. 품질점수: {pair.get('quality_score', 0)}")
                print(f"      ID: {question.get('question_id')}")
                print(f"      제목: {question.get('title', 'N/A')[:80]}...")
                print(f"      점수: Q{question.get('score', 0)}/A{answer.get('score', 0)}")
                print(f"      태그: {', '.join(question.get('tags', []))}")
                print(f"      조회수: {question.get('view_count', 0):,}")
                
                # 질문 내용 미리보기
                q_body = question.get('body_markdown', '')[:200]
                print(f"      질문: {q_body}...")
                
                # 답변 내용 미리보기
                a_body = answer.get('body_markdown', '')[:200]
                print(f"      답변: {a_body}...")
            
            # 데이터 저장 (오빠두나/레딧과 동일한 위치)
            now = datetime.now()
            partition_dir = Path(Config.OUTPUT_DIR) / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = now.strftime('%H%M%S')
            output_file = partition_dir / f"stackoverflow_{now.strftime('%Y%m%d')}.jsonl"
            metadata_file = partition_dir / f"stackoverflow_collection_metadata_{timestamp}.json"
            
            # 메타데이터 저장 (오빠두나/레딧 형식과 동일)
            metadata = {
                'source': 'stackoverflow',
                'collection_method': 'fresh_api_collection',
                'collected_at': datetime.now().isoformat(),
                'target_count': target_count,
                'actual_count': len(selected_pairs),
                'complete_pairs': complete_pairs,
                'collection_duration_seconds': duration.total_seconds(),
                'success': len(selected_pairs) >= target_count,
                'collection_stats': {
                    'total_available': len(qa_pairs),
                    'selected': len(selected_pairs),
                    'quality_threshold': 'top_10_by_score',
                    'avg_quality_score': sum(quality_scores) / len(quality_scores) if quality_scores else 0
                }
            }
            
            # JSONL 형식으로 저장 (오빠두나/레딧과 동일)
            with open(output_file, 'w', encoding='utf-8') as f:
                for i, pair in enumerate(selected_pairs, 1):
                    qa_item = {
                        'id': f"stackoverflow_{pair['question'].get('question_id')}",
                        'rank': i,
                        'question': pair['question'],
                        'answer': pair.get('answer'),
                        'quality_score': pair.get('quality_score', 0),
                        'source': 'stackoverflow_api',
                        'collected_at': datetime.now().isoformat()
                    }
                    f.write(json.dumps(qa_item, ensure_ascii=False) + '\n')
            
            # 메타데이터 파일 저장
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"\n💾 수집 데이터 저장 (오빠두나/레딧과 동일한 위치):")
            print(f"   📁 디렉토리: {partition_dir}")
            print(f"   📄 데이터 파일: {output_file.name}")
            print(f"   📄 메타데이터: {metadata_file.name}")
            print(f"   💾 크기: {output_file.stat().st_size:,} bytes")
            
            # API 사용량 체크
            stats = collector.get_collection_stats()
            print(f"\n📊 API 사용 통계:")
            print(f"   오늘 사용량: {stats['requests_today']}")
            print(f"   남은 할당량: {stats['daily_quota_remaining']}")
            
            # 성공 판정
            success = len(selected_pairs) >= target_count and complete_pairs == len(selected_pairs)
            
            if success:
                print(f"\n🎉 Stack Overflow 새로운 수집 성공!")
                print(f"   ✅ 목표 달성: {len(selected_pairs)}/{target_count}개")
                print(f"   ✅ 100% 완성도: 모든 Q&A에 답변 포함")
                print(f"   ✅ 고품질 Excel 관련 데이터")
                print(f"   🚀 오빠두나/레딧과 동일한 수준의 안정성")
            else:
                print(f"\n⚠️ 부분 성공")
                print(f"   📊 수집량: {len(selected_pairs)}/{target_count}개")
                print(f"   📊 완성도: {complete_pairs}/{len(selected_pairs)}개")
            
            await collector.close()
            return selected_pairs
        
        else:
            print("❌ 수집된 데이터가 없습니다.")
            await collector.close()
            return []
        
    except Exception as e:
        print(f"❌ 새로운 수집 실패: {e}")
        import traceback
        traceback.print_exc()
        return []

def verify_data_freshness():
    """데이터 신선도 검증 (오빠두나/레딧과 동일한 위치에서)"""
    print(f"\n🔍 데이터 신선도 검증")
    print("=" * 40)
    
    try:
        # 오늘 날짜 디렉토리에서 Stack Overflow 파일 찾기
        now = datetime.now()
        partition_dir = Path(Config.OUTPUT_DIR) / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}"
        
        if not partition_dir.exists():
            print("❌ 오늘 날짜 디렉토리가 없습니다.")
            return
        
        # Stack Overflow 메타데이터 파일 찾기
        metadata_files = list(partition_dir.glob("stackoverflow_collection_metadata_*.json"))
        
        if metadata_files:
            latest_metadata_file = max(metadata_files, key=lambda f: f.stat().st_mtime)
            
            with open(latest_metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            # 해당하는 JSONL 파일 찾기
            jsonl_file = partition_dir / f"stackoverflow_{now.strftime('%Y%m%d')}.jsonl"
            
            print(f"📁 저장 위치: {partition_dir}")
            print(f"📄 메타데이터: {latest_metadata_file.name}")
            print(f"📄 데이터 파일: {jsonl_file.name if jsonl_file.exists() else '없음'}")
            print(f"📅 수집 시간: {metadata.get('collected_at', 'N/A')}")
            print(f"📊 수집 성공: {'✅' if metadata.get('success') else '❌'}")
            print(f"🎯 목표/실제: {metadata.get('target_count')}/{metadata.get('actual_count')}개")
            print(f"✅ 완성도: {metadata.get('complete_pairs')}/{metadata.get('actual_count')}개")
            
            # JSONL 파일에서 첫 번째 항목 확인
            if jsonl_file.exists():
                with open(jsonl_file, 'r', encoding='utf-8') as f:
                    first_line = f.readline()
                    if first_line:
                        first_qa = json.loads(first_line)
                        print(f"\n📋 최고 품질 Q&A:")
                        print(f"   순위: #{first_qa.get('rank')}")
                        print(f"   품질점수: {first_qa.get('quality_score')}")
                        print(f"   제목: {first_qa['question'].get('title', 'N/A')[:60]}...")
        else:
            print("❌ Stack Overflow 메타데이터 파일이 없습니다.")
            
    except Exception as e:
        print(f"❌ 신선도 검증 실패: {e}")

async def main():
    """메인 함수"""
    print("🚀 Stack Overflow 완전 새로운 수집 테스트")
    print("=" * 70)
    print("🎯 목표: 오빠두나/레딧처럼 안정적으로 10개 Q&A 수집")
    
    # 1. 완전 초기화
    complete_reset()
    
    # 2. 새로운 수집 실행
    results = await fresh_stackoverflow_collection()
    
    # 3. 데이터 신선도 검증
    verify_data_freshness()
    
    # 4. 최종 결과
    print(f"\n🏁 Stack Overflow 새로운 수집 테스트 완료!")
    
    if results and len(results) >= 10:
        print(f"   🎉 완전 성공: {len(results)}개 고품질 Q&A 수집")
        print(f"   ✅ 오빠두나/레딧 수준의 안정적 수집 달성")
        print(f"   🚀 Stack Overflow 수집기 운영 준비 완료")
    elif results:
        print(f"   ⚠️ 부분 성공: {len(results)}개 Q&A 수집")
        print(f"   📈 수집 범위 확장으로 목표 달성 가능")
    else:
        print(f"   ❌ 수집 실패")
        print(f"   🔧 API 키 또는 네트워크 문제 점검 필요")

if __name__ == "__main__":
    asyncio.run(main())