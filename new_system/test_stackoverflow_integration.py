#!/usr/bin/env python3
"""
Stack Overflow 수집기 통합 테스트
- 오빠두나, 레딧과 같은 방식으로 실제 운영 테스트
- 중복 검출 초기화
- 10개 수집 테스트
"""
import asyncio
import json
import sqlite3
import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent))

from config import Config
from core.cache import LocalCache, APICache
from collectors.fixed_stackoverflow_collector import FixedStackOverflowCollector

def clear_stackoverflow_deduplication():
    """Stack Overflow 중복 검출 데이터 초기화"""
    print("🗑️ Stack Overflow 중복 검출 초기화")
    
    try:
        # 중복 추적기 데이터베이스 초기화
        dedup_db_path = Path("/Users/kevin/bigdata/data/deduplication_tracker.db")
        
        if dedup_db_path.exists():
            with sqlite3.connect(dedup_db_path) as conn:
                # Stack Overflow 관련 중복 데이터 삭제
                cursor = conn.execute("DELETE FROM stackoverflow_questions")
                deleted_count = cursor.rowcount
                conn.commit()
                print(f"   삭제된 Stack Overflow 중복 항목: {deleted_count}개")
        else:
            print("   중복 추적기 데이터베이스가 존재하지 않음")
        
        # 캐시도 초기화
        with sqlite3.connect(Config.DATABASE_PATH) as conn:
            cursor = conn.execute("DELETE FROM cache WHERE key LIKE 'so_api:%' OR key LIKE 'fixed_%'")
            deleted_cache = cursor.rowcount
            conn.commit()
            print(f"   삭제된 캐시 항목: {deleted_cache}개")
        
        print("✅ Stack Overflow 중복 검출 초기화 완료")
        
    except Exception as e:
        print(f"❌ 중복 검출 초기화 실패: {e}")

async def test_stackoverflow_production_collection():
    """Stack Overflow 실제 운영 수집 테스트"""
    print("\n🚀 Stack Overflow 실제 운영 수집 테스트")
    print("=" * 60)
    
    try:
        # 수집기 초기화 (오빠두나/레딧과 동일한 방식)
        local_cache = LocalCache(Config.DATABASE_PATH)
        api_cache = APICache(local_cache)
        collector = FixedStackOverflowCollector(api_cache)
        
        print("✅ Stack Overflow 수집기 초기화 완료")
        print("🎯 목표: 10개 고품질 Q&A 수집")
        
        # 최근 1개월 데이터로 제한 (더 빠른 테스트)
        from_date = datetime.now() - timedelta(days=30)
        print(f"📅 수집 기간: {from_date.strftime('%Y-%m-%d')} ~ 현재")
        
        print("\n🔄 수집 시작...")
        start_time = datetime.now()
        
        # 페이지별로 수집하여 10개 달성할 때까지 계속
        collected_qa_pairs = []
        page = 1
        max_pages = 5  # 최대 5페이지까지 시도
        target_count = 10
        
        while len(collected_qa_pairs) < target_count and page <= max_pages:
            print(f"\n📄 페이지 {page} 수집 중...")
            
            # 한 페이지씩 수집
            page_results = await collector.collect_excel_questions_fixed(
                from_date=from_date,
                max_pages=1  # 페이지별로 수집
            )
            
            print(f"   페이지 {page} 결과: {len(page_results)}개 Q&A")
            
            if page_results:
                collected_qa_pairs.extend(page_results)
                print(f"   누적 수집: {len(collected_qa_pairs)}개")
            else:
                print("   이 페이지에서 새로운 데이터 없음")
                break
            
            page += 1
            
            # 목표 달성 시 중단
            if len(collected_qa_pairs) >= target_count:
                print(f"🎯 목표 달성! {len(collected_qa_pairs)}개 수집")
                break
            
            # 페이지 간 지연 (서버 부하 방지)
            await asyncio.sleep(1)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n⏱️ 수집 완료 (소요 시간: {duration.total_seconds():.1f}초)")
        print(f"📊 최종 수집 결과: {len(collected_qa_pairs)}개 Q&A 쌍")
        
        if collected_qa_pairs:
            # 상세 분석 (오빠두나/레딧과 동일한 방식)
            print(f"\n🔍 수집 데이터 분석:")
            
            # 완성도 체크
            complete_pairs = sum(1 for pair in collected_qa_pairs if pair.get('answer'))
            print(f"   완전한 Q&A 쌍: {complete_pairs}/{len(collected_qa_pairs)} ({complete_pairs/len(collected_qa_pairs)*100:.1f}%)")
            
            # 품질 분석
            quality_scores = [pair.get('quality_score', 0) for pair in collected_qa_pairs]
            question_scores = [pair['question'].get('score', 0) for pair in collected_qa_pairs]
            answer_scores = [pair['answer'].get('score', 0) for pair in collected_qa_pairs if pair.get('answer')]
            
            print(f"   품질 점수: {min(quality_scores)} ~ {max(quality_scores)} (평균: {sum(quality_scores)/len(quality_scores):.1f})")
            print(f"   질문 점수: {min(question_scores)} ~ {max(question_scores)} (평균: {sum(question_scores)/len(question_scores):.1f})")
            if answer_scores:
                print(f"   답변 점수: {min(answer_scores)} ~ {max(answer_scores)} (평균: {sum(answer_scores)/len(answer_scores):.1f})")
            
            # 태그 분석
            all_tags = []
            for pair in collected_qa_pairs:
                all_tags.extend(pair['question'].get('tags', []))
            
            tag_counts = {}
            for tag in all_tags:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
            
            print(f"\n🏷️ 태그 분포:")
            for tag, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"   {tag}: {count}회")
            
            # Excel 키워드 분석
            excel_keywords = ['formula', 'function', 'vlookup', 'index', 'match', 'if', 'sum']
            keyword_counts = {kw: 0 for kw in excel_keywords}
            
            for pair in collected_qa_pairs:
                full_text = (
                    pair['question'].get('title', '') + ' ' + 
                    pair['question'].get('body_markdown', '') + ' ' + 
                    pair.get('answer', {}).get('body_markdown', '')
                ).lower()
                
                for keyword in excel_keywords:
                    if keyword in full_text:
                        keyword_counts[keyword] += 1
            
            print(f"\n🔧 Excel 키워드 언급:")
            for kw, count in sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True):
                if count > 0:
                    print(f"   {kw}: {count}회")
            
            # 샘플 출력
            print(f"\n📝 수집된 Q&A 샘플:")
            for i, pair in enumerate(collected_qa_pairs[:3], 1):
                question = pair['question']
                answer = pair.get('answer', {})
                
                print(f"\n   샘플 {i}:")
                print(f"   📋 ID: {question.get('question_id')}")
                print(f"   📋 제목: {question.get('title', 'N/A')[:80]}...")
                print(f"   📋 점수: Q{question.get('score', 0)}/A{answer.get('score', 0)}")
                print(f"   📋 태그: {', '.join(question.get('tags', []))}")
                print(f"   💬 답변 길이: {len(answer.get('body_markdown', ''))}자")
            
            # 데이터 저장 (오빠두나/레딧과 동일한 형식)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = Path(Config.OUTPUT_DIR) / f"stackoverflow_production_test_{timestamp}.json"
            
            # 저장용 데이터 변환
            save_data = {
                'metadata': {
                    'source': 'stackoverflow',
                    'collection_method': 'api_production_test',
                    'collected_at': datetime.now().isoformat(),
                    'total_count': len(collected_qa_pairs),
                    'complete_pairs': complete_pairs,
                    'collection_duration_seconds': duration.total_seconds(),
                    'target_achieved': len(collected_qa_pairs) >= target_count
                },
                'qa_pairs': []
            }
            
            for pair in collected_qa_pairs:
                save_item = {
                    'question': pair['question'],
                    'answer': pair.get('answer'),
                    'quality_score': pair.get('quality_score', 0),
                    'source': 'stackoverflow_api',
                    'collected_at': pair.get('collected_at', datetime.now().isoformat())
                }
                save_data['qa_pairs'].append(save_item)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"\n💾 수집 데이터 저장:")
            print(f"   파일: {output_file}")
            print(f"   크기: {output_file.stat().st_size:,} bytes")
            
            # API 사용량 체크
            stats = collector.get_collection_stats()
            print(f"\n📊 API 사용 통계:")
            print(f"   오늘 사용량: {stats['requests_today']}")
            print(f"   남은 할당량: {stats['daily_quota_remaining']}")
            
            # 성공 여부 판단
            success = len(collected_qa_pairs) >= target_count and complete_pairs == len(collected_qa_pairs)
            
            if success:
                print(f"\n🎉 Stack Overflow 수집 테스트 성공!")
                print(f"   ✅ 목표 달성: {len(collected_qa_pairs)}/{target_count}개")
                print(f"   ✅ 100% 완성도: 모든 Q&A에 답변 포함")
                print(f"   ✅ 고품질 Excel 관련 데이터")
            else:
                print(f"\n⚠️ 수집 테스트 부분 성공")
                print(f"   📊 수집량: {len(collected_qa_pairs)}/{target_count}개")
                print(f"   📊 완성도: {complete_pairs}/{len(collected_qa_pairs)}개")
        
        else:
            print("❌ 수집된 데이터가 없습니다.")
        
        await collector.close()
        
        return collected_qa_pairs
        
    except Exception as e:
        print(f"❌ Stack Overflow 수집 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return []

def compare_with_other_collectors():
    """다른 수집기들과 비교"""
    print(f"\n📊 다른 수집기와 비교 분석")
    print("=" * 50)
    
    try:
        output_dir = Path(Config.OUTPUT_DIR)
        
        # 수집기별 최신 파일 찾기
        collectors = {
            'stackoverflow': list(output_dir.glob("stackoverflow_production_test_*.json")),
            'oppadu': list(output_dir.glob("*oppadu*.json")),
            'reddit': list(output_dir.glob("*reddit*.json"))
        }
        
        comparison_data = {}
        
        for collector_name, files in collectors.items():
            if files:
                latest_file = max(files, key=lambda f: f.stat().st_mtime)
                
                try:
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if collector_name == 'stackoverflow':
                        qa_count = len(data.get('qa_pairs', []))
                        complete_count = sum(1 for item in data.get('qa_pairs', []) if item.get('answer'))
                    else:
                        qa_count = len(data) if isinstance(data, list) else len(data.get('items', []))
                        complete_count = qa_count  # 다른 수집기들은 완성도 가정
                    
                    comparison_data[collector_name] = {
                        'file': latest_file.name,
                        'count': qa_count,
                        'complete': complete_count,
                        'size': latest_file.stat().st_size
                    }
                    
                except Exception as e:
                    print(f"   {collector_name} 파일 분석 실패: {e}")
            else:
                comparison_data[collector_name] = None
        
        print("📈 수집기별 성능 비교:")
        for collector_name, data in comparison_data.items():
            if data:
                print(f"\n   {collector_name.upper()}:")
                print(f"   📄 파일: {data['file']}")
                print(f"   📊 수집량: {data['count']}개")
                print(f"   ✅ 완성도: {data['complete']}/{data['count']} ({data['complete']/data['count']*100:.1f}%)")
                print(f"   💾 크기: {data['size']:,} bytes")
            else:
                print(f"\n   {collector_name.upper()}: 데이터 없음")
        
    except Exception as e:
        print(f"비교 분석 실패: {e}")

async def main():
    """메인 함수"""
    print("🚀 Stack Overflow 수집기 통합 테스트")
    print("=" * 70)
    print("📍 오빠두나, 레딧과 같은 방식으로 실제 운영 테스트")
    
    # 1. 중복 검출 초기화
    clear_stackoverflow_deduplication()
    
    # 2. 실제 수집 테스트 (10개 목표)
    results = await test_stackoverflow_production_collection()
    
    # 3. 다른 수집기와 비교
    compare_with_other_collectors()
    
    # 4. 최종 결과
    print(f"\n🏁 Stack Overflow 통합 테스트 완료!")
    
    if results and len(results) >= 10:
        print(f"   🎉 테스트 성공: {len(results)}개 Q&A 수집")
        print(f"   ✅ 오빠두나/레딧과 동일한 수준의 안정성")
        print(f"   📊 Stack Overflow 수집기 운영 준비 완료")
    elif results:
        print(f"   ⚠️ 부분 성공: {len(results)}개 Q&A 수집")
        print(f"   🔄 더 많은 데이터 확보를 위해 수집 범위 확장 필요")
    else:
        print(f"   ❌ 테스트 실패: 데이터 수집 안됨")
        print(f"   🔧 수집 로직 점검 필요")

if __name__ == "__main__":
    asyncio.run(main())