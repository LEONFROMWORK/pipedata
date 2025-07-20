#!/usr/bin/env python3
"""
웹 스크래핑 기반 Stack Overflow 수집기 테스트
- s-pagination 클래스 기반 페이지 순회 테스트
- API 방식과 웹 스크래핑 방식 비교
"""
import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

from config import Config
from core.cache import LocalCache, APICache
from collectors.web_scraping_stackoverflow import WebScrapingStackOverflowCollector

async def test_web_scraping_collector():
    """웹 스크래핑 수집기 테스트"""
    print("🌐 웹 스크래핑 기반 Stack Overflow 수집기 테스트")
    print("=" * 70)
    
    try:
        # 수집기 초기화
        local_cache = LocalCache(Config.DATABASE_PATH)
        api_cache = APICache(local_cache)
        web_collector = WebScrapingStackOverflowCollector(api_cache)
        
        print("✅ 웹 스크래핑 수집기 초기화 완료")
        print("🎯 s-pagination 클래스 기반 페이지 순회 테스트")
        
        # 테스트 수집 (작은 규모)
        print("\n🚀 웹 스크래핑 수집 시작...")
        start_time = datetime.now()
        
        web_qa_pairs = await web_collector.collect_excel_questions_web(
            max_pages=3  # 테스트용으로 3페이지만
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n⏱️ 웹 스크래핑 완료 (소요 시간: {duration.total_seconds():.1f}초)")
        print(f"📊 웹 스크래핑 결과: {len(web_qa_pairs)}개 Q&A 쌍")
        
        if not web_qa_pairs:
            print("❌ 웹 스크래핑으로 수집된 데이터가 없습니다.")
            await web_collector.close()
            return
        
        # 데이터 품질 분석
        print(f"\n🔍 웹 스크래핑 데이터 분석:")
        
        complete_pairs = sum(1 for pair in web_qa_pairs if pair.get('answer'))
        print(f"   완전한 Q&A 쌍: {complete_pairs}/{len(web_qa_pairs)} ({complete_pairs/len(web_qa_pairs)*100:.1f}%)")
        
        # 점수 분석
        question_scores = [pair['question'].get('score', 0) for pair in web_qa_pairs]
        answer_scores = [pair['answer'].get('score', 0) for pair in web_qa_pairs if pair.get('answer')]
        quality_scores = [pair.get('quality_score', 0) for pair in web_qa_pairs]
        
        if question_scores:
            print(f"   질문 점수: {min(question_scores)} ~ {max(question_scores)} (평균: {sum(question_scores)/len(question_scores):.1f})")
        if answer_scores:
            print(f"   답변 점수: {min(answer_scores)} ~ {max(answer_scores)} (평균: {sum(answer_scores)/len(answer_scores):.1f})")
        if quality_scores:
            print(f"   품질 점수: {min(quality_scores)} ~ {max(quality_scores)} (평균: {sum(quality_scores)/len(quality_scores):.1f})")
        
        # 샘플 Q&A 출력
        print(f"\n📝 웹 스크래핑 샘플 Q&A:")
        for i, pair in enumerate(web_qa_pairs[:3], 1):
            question = pair['question']
            answer = pair.get('answer', {})
            
            print(f"\n   샘플 {i}:")
            print(f"   📋 ID: {question.get('question_id')}")
            print(f"   📋 제목: {question.get('title', 'N/A')[:80]}...")
            print(f"   📋 점수: Q{question.get('score', 0)}/A{answer.get('score', 0)}")
            print(f"   📋 태그: {', '.join(question.get('tags', []))}")
            print(f"   📋 조회수: {question.get('view_count', 0):,}")
            print(f"   💬 답변 채택: {'Yes' if answer.get('is_accepted') else 'No'}")
            
            answer_preview = answer.get('body_markdown', '')[:200]
            print(f"   💬 답변 미리보기: {answer_preview}...")
        
        # 데이터 저장
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        web_output_file = Path(Config.OUTPUT_DIR) / f"web_scraping_stackoverflow_{timestamp}.json"
        
        # JSON 직렬화 가능한 형태로 변환
        save_data = []
        for pair in web_qa_pairs:
            save_item = {
                'question': pair['question'],
                'answer': pair.get('answer'),
                'quality_score': pair.get('quality_score', 0),
                'source': pair.get('source', 'web_scraping'),
                'collected_at': pair.get('collected_at')
            }
            save_data.append(save_item)
        
        with open(web_output_file, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n💾 웹 스크래핑 데이터 저장:")
        print(f"   파일: {web_output_file}")
        print(f"   크기: {web_output_file.stat().st_size:,} bytes")
        
        await web_collector.close()
        
        return web_qa_pairs
        
    except Exception as e:
        print(f"❌ 웹 스크래핑 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return []

def compare_collection_methods():
    """API 방식과 웹 스크래핑 방식 비교"""
    print(f"\n📊 수집 방식 비교 분석:")
    print("=" * 50)
    
    try:
        # API 방식 최신 결과 파일 찾기
        api_files = list(Path(Config.OUTPUT_DIR).glob("large_scale_stackoverflow_*.json"))
        if api_files:
            latest_api_file = max(api_files, key=lambda f: f.stat().st_mtime)
            
            with open(latest_api_file, 'r', encoding='utf-8') as f:
                api_data = json.load(f)
            
            print(f"📡 API 방식 결과 (파일: {latest_api_file.name}):")
            print(f"   수집 개수: {len(api_data)}개")
            print(f"   파일 크기: {latest_api_file.stat().st_size:,} bytes")
            
            api_complete = sum(1 for item in api_data if item.get('answer'))
            print(f"   완전한 Q&A: {api_complete}/{len(api_data)} ({api_complete/len(api_data)*100:.1f}%)")
        else:
            print("📡 API 방식 결과: 파일을 찾을 수 없음")
        
        # 웹 스크래핑 결과 파일 찾기
        web_files = list(Path(Config.OUTPUT_DIR).glob("web_scraping_stackoverflow_*.json"))
        if web_files:
            latest_web_file = max(web_files, key=lambda f: f.stat().st_mtime)
            
            with open(latest_web_file, 'r', encoding='utf-8') as f:
                web_data = json.load(f)
            
            print(f"\n🌐 웹 스크래핑 방식 결과 (파일: {latest_web_file.name}):")
            print(f"   수집 개수: {len(web_data)}개")
            print(f"   파일 크기: {latest_web_file.stat().st_size:,} bytes")
            
            web_complete = sum(1 for item in web_data if item.get('answer'))
            print(f"   완전한 Q&A: {web_complete}/{len(web_data)} ({web_complete/len(web_data)*100:.1f}%)")
        else:
            print("\n🌐 웹 스크래핑 방식 결과: 아직 수집되지 않음")
        
        print(f"\n🔍 비교 분석:")
        print(f"   API 방식 장점: 빠름, 안정적, 구조화된 데이터")
        print(f"   API 방식 단점: 제한된 페이지, 할당량 제한")
        print(f"   웹 스크래핑 장점: 무제한 페이지, 더 많은 데이터")
        print(f"   웹 스크래핑 단점: 느림, 불안정할 수 있음")
        
    except Exception as e:
        print(f"비교 분석 실패: {e}")

async def main():
    """메인 함수"""
    print("🚀 Stack Overflow 웹 스크래핑 vs API 비교 테스트")
    print("=" * 80)
    
    # 웹 스크래핑 테스트
    web_results = await test_web_scraping_collector()
    
    # 수집 방식 비교
    compare_collection_methods()
    
    print(f"\n🎯 웹 스크래핑 특징:")
    print(f"   ✅ s-pagination 클래스 기반 페이지 순회")
    print(f"   ✅ 개별 질문 페이지 상세 스크래핑")
    print(f"   ✅ 채택된 답변 우선 수집")
    print(f"   ✅ API 제한 없이 대량 데이터 수집 가능")
    
    if web_results:
        print(f"   🎉 웹 스크래핑 성공: {len(web_results)}개 Q&A 수집")
    else:
        print(f"   ⚠️ 웹 스크래핑 개선 필요")
    
    print(f"\n🏁 테스트 완료!")

if __name__ == "__main__":
    asyncio.run(main())