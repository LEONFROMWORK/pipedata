#!/usr/bin/env python3
"""
대규모 Stack Overflow 데이터 수집 테스트
- 캐시 초기화
- 더 넓은 기간 설정
- 더 많은 페이지 수집
- 실시간 진행 상황 모니터링
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

def clear_stackoverflow_cache():
    """Stack Overflow 관련 캐시 모두 삭제"""
    print("🗑️ Stack Overflow 캐시 초기화")
    
    try:
        with sqlite3.connect(Config.DATABASE_PATH) as conn:
            # 기존 Stack Overflow 캐시 삭제
            cursor = conn.execute("DELETE FROM cache WHERE key LIKE 'so_api:%' OR key LIKE 'fixed_%'")
            deleted_count = cursor.rowcount
            conn.commit()
            
            print(f"   삭제된 캐시 항목: {deleted_count}개")
            
            # 중복 추적기도 초기화
            dedup_db = Path("/Users/kevin/bigdata/data/deduplication_tracker.db")
            if dedup_db.exists():
                with sqlite3.connect(dedup_db) as dedup_conn:
                    dedup_conn.execute("DELETE FROM stackoverflow_questions")
                    dedup_conn.commit()
                    print("   중복 추적기 초기화 완료")
                    
    except Exception as e:
        print(f"   캐시 초기화 오류: {e}")

async def large_scale_collection():
    """대규모 수집 테스트"""
    print("📈 대규모 Stack Overflow 데이터 수집 테스트")
    print("=" * 60)
    
    try:
        # 수집기 초기화
        local_cache = LocalCache(Config.DATABASE_PATH)
        api_cache = APICache(local_cache)
        collector = FixedStackOverflowCollector(api_cache)
        
        print("✅ 수집기 초기화 완료")
        
        # 더 넓은 기간 설정 (3개월)
        from_date = datetime.now() - timedelta(days=90)
        print(f"📅 수집 기간: {from_date.strftime('%Y-%m-%d')} ~ 현재 (3개월)")
        
        # 더 많은 페이지 수집
        max_pages = 5
        print(f"📄 수집 페이지: 최대 {max_pages}페이지")
        
        # 수집 시작
        print("\n🚀 수집 시작...")
        start_time = datetime.now()
        
        qa_pairs = await collector.collect_excel_questions_fixed(
            from_date=from_date,
            max_pages=max_pages
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n⏱️ 수집 완료 (소요 시간: {duration.total_seconds():.1f}초)")
        print(f"📊 총 수집 결과: {len(qa_pairs)}개 Q&A 쌍")
        
        if not qa_pairs:
            print("❌ 수집된 데이터가 없습니다.")
            await collector.close()
            return
        
        # 상세 분석
        print(f"\n🔍 상세 데이터 분석:")
        
        # 완성도 분석
        complete_pairs = sum(1 for pair in qa_pairs if pair.get('answer'))
        print(f"   완전한 Q&A 쌍: {complete_pairs}/{len(qa_pairs)} ({complete_pairs/len(qa_pairs)*100:.1f}%)")
        
        # 점수 분석
        question_scores = [pair['question'].get('score', 0) for pair in qa_pairs]
        answer_scores = [pair['answer'].get('score', 0) for pair in qa_pairs if pair.get('answer')]
        quality_scores = [pair.get('quality_score', 0) for pair in qa_pairs]
        
        print(f"   질문 점수 범위: {min(question_scores)} ~ {max(question_scores)} (평균: {sum(question_scores)/len(question_scores):.1f})")
        if answer_scores:
            print(f"   답변 점수 범위: {min(answer_scores)} ~ {max(answer_scores)} (평균: {sum(answer_scores)/len(answer_scores):.1f})")
        print(f"   품질 점수 범위: {min(quality_scores)} ~ {max(quality_scores)} (평균: {sum(quality_scores)/len(quality_scores):.1f})")
        
        # 태그 분석
        all_tags = []
        for pair in qa_pairs:
            all_tags.extend(pair['question'].get('tags', []))
        
        tag_counts = {}
        for tag in all_tags:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        print(f"\n🏷️ 태그 분포 (상위 10개):")
        for tag, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"   {tag}: {count}회")
        
        # Excel 함수 분석
        print(f"\n🔧 Excel 함수 언급 분석:")
        excel_functions = ['VLOOKUP', 'INDEX', 'MATCH', 'SUMIF', 'COUNTIF', 'IF', 'PIVOT', 'XLOOKUP', 'LAMBDA']
        function_counts = {func: 0 for func in excel_functions}
        
        for pair in qa_pairs:
            question = pair['question']
            answer = pair.get('answer', {})
            
            # 텍스트 합치기
            q_text = (question.get('title', '') + ' ' + question.get('body_markdown', '')).upper()
            a_text = answer.get('body_markdown', answer.get('body', '')).upper()
            full_text = q_text + ' ' + a_text
            
            for func in excel_functions:
                if func in full_text:
                    function_counts[func] += 1
        
        for func, count in sorted(function_counts.items(), key=lambda x: x[1], reverse=True):
            if count > 0:
                print(f"   {func}: {count}회")
        
        # 고품질 Q&A 선별 (품질 점수 상위)
        high_quality = sorted(qa_pairs, key=lambda x: x.get('quality_score', 0), reverse=True)[:5]
        
        print(f"\n⭐ 고품질 Q&A (상위 5개):")
        for i, pair in enumerate(high_quality, 1):
            question = pair['question']
            answer = pair.get('answer', {})
            
            print(f"\n   {i}. 품질점수: {pair.get('quality_score', 0)}")
            print(f"      질문: {question.get('title', 'N/A')[:100]}...")
            print(f"      Q점수: {question.get('score', 0)} | A점수: {answer.get('score', 0) if answer else 'N/A'}")
            print(f"      태그: {', '.join(question.get('tags', []))}")
        
        # 데이터 저장
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = Path(Config.OUTPUT_DIR) / f"large_scale_stackoverflow_{timestamp}.json"
        
        # JSON 직렬화 가능한 형태로 변환
        save_data = []
        for pair in qa_pairs:
            save_item = {
                'question': pair['question'],
                'answer': pair.get('answer'),
                'quality_score': pair.get('quality_score', 0),
                'is_complete': bool(pair.get('answer')),
                'collected_at': datetime.now().isoformat()
            }
            save_data.append(save_item)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n💾 대규모 데이터 저장:")
        print(f"   파일: {output_file}")
        print(f"   크기: {output_file.stat().st_size:,} bytes")
        
        # 요약 리포트
        summary_file = Path(Config.OUTPUT_DIR) / f"collection_report_{timestamp}.txt"
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("Stack Overflow 대규모 수집 리포트\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"수집 일시: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_time.strftime('%H:%M:%S')}\n")
            f.write(f"수집 기간: {from_date.strftime('%Y-%m-%d')} ~ 현재 (90일)\n")
            f.write(f"소요 시간: {duration.total_seconds():.1f}초\n")
            f.write(f"수집 페이지: {max_pages}페이지\n\n")
            f.write(f"총 Q&A 쌍: {len(qa_pairs)}개\n")
            f.write(f"완전한 쌍: {complete_pairs}개 ({complete_pairs/len(qa_pairs)*100:.1f}%)\n")
            f.write(f"평균 품질 점수: {sum(quality_scores)/len(quality_scores):.1f}\n\n")
            f.write(f"데이터 파일: {output_file.name}\n")
        
        print(f"   리포트: {summary_file}")
        
        # 수집 통계
        stats = collector.get_collection_stats()
        print(f"\n📊 API 사용 통계:")
        print(f"   오늘 사용량: {stats['requests_today']}")
        print(f"   남은 할당량: {stats['daily_quota_remaining']}")
        
        await collector.close()
        
        print(f"\n🎉 대규모 수집 테스트 완료!")
        print(f"   수집된 고품질 Excel Q&A: {complete_pairs}개")
        
        return qa_pairs
        
    except Exception as e:
        print(f"❌ 대규모 수집 실패: {e}")
        import traceback
        traceback.print_exc()

def main():
    """메인 함수"""
    print("🚀 Stack Overflow 대규모 수집 테스트")
    print("=" * 70)
    
    # 1. 캐시 초기화
    clear_stackoverflow_cache()
    
    # 2. 대규모 수집 실행
    result = asyncio.run(large_scale_collection())
    
    print(f"\n🏁 전체 테스트 완료!")

if __name__ == "__main__":
    main()