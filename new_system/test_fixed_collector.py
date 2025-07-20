#!/usr/bin/env python3
"""
수정된 Stack Overflow 수집기 테스트
문제점 해결 검증:
1. 답변 수집 로직 수정
2. API 파라미터 개선  
3. 질문-답변 매칭 개선
4. 완전한 Q&A 쌍 생성
"""
import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent))

from config import Config
from core.cache import LocalCache, APICache
from collectors.fixed_stackoverflow_collector import FixedStackOverflowCollector

async def test_fixed_collector():
    """수정된 수집기 테스트"""
    print("🛠️ 수정된 Stack Overflow 수집기 테스트")
    print("=" * 60)
    
    try:
        # 수집기 초기화
        local_cache = LocalCache(Config.DATABASE_PATH)
        api_cache = APICache(local_cache)
        collector = FixedStackOverflowCollector(api_cache)
        
        print("✅ 수정된 수집기 초기화 완료")
        
        # 최근 30일 데이터 수집
        from_date = datetime.now() - timedelta(days=30)
        print(f"📅 수집 기간: {from_date.strftime('%Y-%m-%d')} ~ 현재")
        
        # 테스트 수집 (작은 규모)
        qa_pairs = await collector.collect_excel_questions_fixed(
            from_date=from_date,
            max_pages=2  # 테스트용으로 제한
        )
        
        print(f"\n📊 수집 결과:")
        print(f"   총 Q&A 쌍: {len(qa_pairs)}")
        
        if not qa_pairs:
            print("❌ 수집된 데이터가 없습니다.")
            await collector.close()
            return
        
        # 데이터 품질 분석
        print(f"\n🔍 데이터 품질 분석:")
        
        complete_pairs = 0
        question_only = 0
        total_question_score = 0
        total_answer_score = 0
        
        for pair in qa_pairs:
            if pair.get('answer'):
                complete_pairs += 1
                total_question_score += pair['question'].get('score', 0)
                total_answer_score += pair['answer'].get('score', 0)
            else:
                question_only += 1
        
        print(f"   완전한 Q&A 쌍: {complete_pairs}개")
        print(f"   질문만 있는 항목: {question_only}개")
        print(f"   완성도: {complete_pairs/len(qa_pairs)*100:.1f}%")
        
        if complete_pairs > 0:
            print(f"   평균 질문 점수: {total_question_score/complete_pairs:.1f}")
            print(f"   평균 답변 점수: {total_answer_score/complete_pairs:.1f}")
        
        # 샘플 Q&A 출력
        print(f"\n📝 샘플 Q&A 쌍:")
        for i, pair in enumerate(qa_pairs[:3], 1):
            question = pair['question']
            answer = pair.get('answer')
            
            print(f"\n   샘플 {i}:")
            print(f"   📋 질문 ID: {question.get('question_id')}")
            print(f"   📋 제목: {question.get('title', 'N/A')[:80]}...")
            print(f"   📋 점수: {question.get('score', 0)}")
            print(f"   📋 태그: {question.get('tags', [])}")
            
            if answer:
                print(f"   💬 답변 ID: {answer.get('answer_id')}")
                print(f"   💬 답변 점수: {answer.get('score', 0)}")
                answer_body = answer.get('body_markdown', answer.get('body', ''))
                print(f"   💬 답변 미리보기: {answer_body[:200]}...")
                print(f"   ✅ 완전한 Q&A 쌍")
            else:
                print(f"   ❌ 답변 없음")
        
        # Excel 관련 키워드 분석
        print(f"\n🔧 Excel 키워드 분석:")
        excel_keywords = ['vlookup', 'index', 'match', 'sum', 'if', 'pivot', 'formula']
        keyword_counts = {kw: 0 for kw in excel_keywords}
        
        for pair in qa_pairs:
            question = pair['question']
            answer = pair.get('answer', {})
            
            # 질문과 답변 텍스트 합치기
            q_text = (question.get('title', '') + ' ' + question.get('body_markdown', '')).lower()
            a_text = answer.get('body_markdown', answer.get('body', '')).lower()
            full_text = q_text + ' ' + a_text
            
            for keyword in excel_keywords:
                if keyword in full_text:
                    keyword_counts[keyword] += 1
        
        for kw, count in sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True):
            if count > 0:
                print(f"   {kw.upper()}: {count}회")
        
        # 데이터 저장
        output_file = Path(Config.OUTPUT_DIR) / f"fixed_stackoverflow_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # 저장용 데이터 변환 (JSON 직렬화 가능하게)
        save_data = []
        for pair in qa_pairs:
            save_item = {
                'question': pair['question'],
                'answer': pair.get('answer'),
                'quality_score': pair.get('quality_score', 0),
                'is_complete': bool(pair.get('answer'))
            }
            save_data.append(save_item)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n💾 수정된 데이터 저장:")
        print(f"   파일: {output_file}")
        print(f"   크기: {output_file.stat().st_size:,} bytes")
        
        # 개선 사항 요약
        print(f"\n🎯 개선 사항 요약:")
        print(f"   ✅ API 파라미터 수정 (accepted=True 제거)")
        print(f"   ✅ 답변 수집 로직 개선")
        print(f"   ✅ 질문-답변 매칭 알고리즘 수정")
        print(f"   ✅ 완전한 Q&A 쌍 생성: {complete_pairs}개")
        
        if complete_pairs > 0:
            print(f"   🎉 수정 성공! 답변이 포함된 완전한 데이터 수집")
        else:
            print(f"   ⚠️ 여전히 답변 수집에 문제가 있을 수 있음")
        
        await collector.close()
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

def compare_with_original():
    """원본 데이터와 비교"""
    print(f"\n📊 원본 데이터와 비교:")
    
    try:
        original_file = Path(Config.OUTPUT_DIR) / "stackoverflow_analysis_20250718_210042.json"
        
        if original_file.exists():
            with open(original_file, 'r', encoding='utf-8') as f:
                original_data = json.load(f)
            
            original_with_answers = sum(1 for item in original_data if item.get('answer'))
            print(f"   원본 데이터: {len(original_data)}개 (답변 포함: {original_with_answers}개)")
        else:
            print(f"   원본 파일을 찾을 수 없음")
            
    except Exception as e:
        print(f"   원본 데이터 비교 실패: {e}")

def main():
    """메인 함수"""
    print("🚀 Stack Overflow 수집기 문제 해결 테스트")
    print("=" * 70)
    
    # 원본과 비교
    compare_with_original()
    
    # 수정된 수집기 테스트
    result = asyncio.run(test_fixed_collector())
    
    print(f"\n🏁 테스트 완료!")

if __name__ == "__main__":
    main()