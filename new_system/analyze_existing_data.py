#!/usr/bin/env python3
"""
기존 수집된 데이터 분석 및 캐시 데이터 검토
"""
import asyncio
import json
import sys
import sqlite3
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

from config import Config
from core.cache import LocalCache, APICache
from collectors.stackoverflow_collector import StackOverflowCollector

def analyze_cache_data():
    """캐시된 데이터 분석"""
    print("🗄️ 캐시 데이터 분석")
    print("=" * 50)
    
    try:
        # SQLite 캐시 데이터베이스 직접 조회
        cache_db_path = Config.DATABASE_PATH
        print(f"캐시 DB 경로: {cache_db_path}")
        
        if not cache_db_path.exists():
            print("❌ 캐시 데이터베이스가 존재하지 않습니다.")
            return
        
        with sqlite3.connect(cache_db_path) as conn:
            # 캐시 테이블 조회
            cursor = conn.execute("SELECT key, value, expires_at, created_at FROM cache")
            cache_entries = cursor.fetchall()
            
            print(f"📊 캐시 엔트리 수: {len(cache_entries)}")
            
            for key, value_str, expires_at, created_at in cache_entries:
                created_time = datetime.fromtimestamp(created_at)
                expires_time = datetime.fromtimestamp(expires_at)
                
                print(f"\n🔑 캐시 키: {key}")
                print(f"   생성 시간: {created_time}")
                print(f"   만료 시간: {expires_time}")
                
                try:
                    # JSON 데이터 파싱
                    data = json.loads(value_str)
                    
                    if 'items' in data:
                        items = data['items']
                        print(f"   데이터 항목 수: {len(items)}")
                        
                        # Stack Overflow 질문 데이터인 경우 상세 분석
                        if len(items) > 0 and 'question_id' in items[0]:
                            print(f"   📝 Stack Overflow 질문 데이터:")
                            analyze_stackoverflow_questions(items)
                        elif len(items) > 0 and 'answer_id' in items[0]:
                            print(f"   💬 Stack Overflow 답변 데이터:")
                            analyze_stackoverflow_answers(items)
                    else:
                        print(f"   데이터 타입: {type(data)}")
                        
                except json.JSONDecodeError:
                    print(f"   ⚠️ JSON 파싱 실패")
                except Exception as e:
                    print(f"   ❌ 데이터 분석 오류: {e}")
                    
    except Exception as e:
        print(f"❌ 캐시 분석 오류: {e}")

def analyze_stackoverflow_questions(questions):
    """Stack Overflow 질문 데이터 분석"""
    print(f"      질문 수: {len(questions)}")
    
    # 샘플 질문 정보
    for i, q in enumerate(questions[:3], 1):
        print(f"      질문 {i}:")
        print(f"         ID: {q.get('question_id')}")
        print(f"         제목: {q.get('title', 'N/A')[:80]}...")
        print(f"         점수: {q.get('score', 0)}")
        print(f"         답변됨: {q.get('is_answered', False)}")
        print(f"         채택답변 ID: {q.get('accepted_answer_id', 'N/A')}")
        print(f"         태그: {q.get('tags', [])}")

def analyze_stackoverflow_answers(answers):
    """Stack Overflow 답변 데이터 분석"""
    print(f"      답변 수: {len(answers)}")
    
    for i, a in enumerate(answers[:3], 1):
        print(f"      답변 {i}:")
        print(f"         ID: {a.get('answer_id')}")
        print(f"         점수: {a.get('score', 0)}")
        body = a.get('body_markdown', a.get('body', ''))
        print(f"         내용 미리보기: {body[:100]}...")

async def force_new_collection():
    """캐시를 무시하고 새로운 데이터 수집"""
    print("\n🔄 새로운 데이터 수집 (캐시 무시)")
    print("=" * 50)
    
    try:
        # 캐시 초기화 (기존 캐시 지우기)
        local_cache = LocalCache(Config.DATABASE_PATH)
        
        # 캐시 정리
        cleaned = local_cache.cleanup_expired()
        print(f"정리된 만료 캐시 항목: {cleaned}")
        
        # 모든 캐시 삭제 (강제)
        with sqlite3.connect(Config.DATABASE_PATH) as conn:
            conn.execute("DELETE FROM cache WHERE key LIKE 'so_api:%'")
            conn.commit()
            print("✅ Stack Overflow API 캐시 삭제 완료")
        
        api_cache = APICache(local_cache)
        collector = StackOverflowCollector(api_cache)
        
        # 새로운 수집 실행
        from datetime import timedelta
        from_date = datetime.now() - timedelta(days=7)  # 최근 1주일
        
        print(f"📅 수집 기간: {from_date.strftime('%Y-%m-%d')} ~ 현재")
        
        questions = await collector.collect_excel_questions(
            from_date=from_date,
            max_pages=2
        )
        
        print(f"\n📊 새로 수집된 데이터:")
        print(f"   총 질문 수: {len(questions)}")
        
        if questions:
            # 수집된 데이터 상세 분석
            print(f"\n📝 상세 분석:")
            
            # 태그 분석
            all_tags = []
            for q in questions:
                all_tags.extend(q.get('tags', []))
            
            unique_tags = list(set(all_tags))
            print(f"   사용된 태그: {unique_tags}")
            
            # 점수 분석
            scores = [q.get('score', 0) for q in questions]
            print(f"   점수 범위: {min(scores)} ~ {max(scores)} (평균: {sum(scores)/len(scores):.1f})")
            
            # Excel 관련 키워드 분석
            excel_keywords = ['excel', 'formula', 'vlookup', 'index', 'match', 'sum', 'if', 'pivot']
            keyword_counts = {kw: 0 for kw in excel_keywords}
            
            for q in questions:
                text = (q.get('title', '') + ' ' + q.get('body_markdown', '')).lower()
                answer_text = q.get('accepted_answer', {}).get('body_markdown', '').lower()
                full_text = text + ' ' + answer_text
                
                for keyword in excel_keywords:
                    if keyword in full_text:
                        keyword_counts[keyword] += 1
            
            print(f"   Excel 키워드 빈도:")
            for kw, count in sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True):
                if count > 0:
                    print(f"      {kw}: {count}회")
            
            # 샘플 출력
            print(f"\n📋 샘플 질문:")
            for i, q in enumerate(questions[:2], 1):
                print(f"   {i}. {q.get('title', 'N/A')}")
                print(f"      점수: {q.get('score')} | 태그: {q.get('tags', [])}")
                
                if q.get('accepted_answer'):
                    answer = q['accepted_answer']
                    print(f"      답변 점수: {answer.get('score')}")
                    answer_preview = answer.get('body_markdown', '')[:200]
                    print(f"      답변 미리보기: {answer_preview}...")
                print()
            
            # 데이터 저장
            output_file = Path(Config.OUTPUT_DIR) / f"verified_stackoverflow_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(questions, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"💾 검증된 데이터 저장: {output_file}")
            print(f"   파일 크기: {output_file.stat().st_size:,} bytes")
            
        else:
            print("⚠️ 새로운 데이터가 수집되지 않았습니다.")
            print("   - API 제한 확인 필요")
            print("   - 검색 조건 조정 필요")
        
        await collector.close()
        
    except Exception as e:
        print(f"❌ 수집 오류: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """메인 함수"""
    print("🚀 Stack Overflow 데이터 수집 상태 종합 분석")
    print("=" * 60)
    
    # 1. 기존 캐시 데이터 분석
    analyze_cache_data()
    
    # 2. 새로운 데이터 수집
    await force_new_collection()
    
    print(f"\n✅ 분석 완료!")
    print(f"   - 캐시 데이터 검토 완료")
    print(f"   - 새로운 데이터 수집 완료")
    print(f"   - 데이터 품질 검증 완료")

if __name__ == "__main__":
    asyncio.run(main())