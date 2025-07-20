#!/usr/bin/env python3
"""
하이브리드 수집 시스템 테스트
- API 방식을 메인으로 사용
- 웹 스크래핑은 보조적으로 활용
- 더 많은 데이터 확보를 위한 다양한 전략 테스트
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

async def test_extended_api_collection():
    """확장된 API 수집 테스트 - 더 많은 페이지와 다양한 태그"""
    print("🔧 확장된 API 수집 테스트")
    print("=" * 60)
    
    try:
        # 수집기 초기화
        local_cache = LocalCache(Config.DATABASE_PATH)
        api_cache = APICache(local_cache)
        collector = FixedStackOverflowCollector(api_cache)
        
        print("✅ 확장된 API 수집기 초기화 완료")
        
        # 더 넓은 기간과 더 많은 페이지로 수집
        from_date = datetime.now() - timedelta(days=180)  # 6개월
        print(f"📅 확장 수집 기간: {from_date.strftime('%Y-%m-%d')} ~ 현재 (6개월)")
        
        print("\n🚀 확장 API 수집 시작...")
        start_time = datetime.now()
        
        # 더 많은 페이지 수집
        qa_pairs = await collector.collect_excel_questions_fixed(
            from_date=from_date,
            max_pages=10  # 더 많은 페이지
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n⏱️ 확장 API 수집 완료 (소요 시간: {duration.total_seconds():.1f}초)")
        print(f"📊 총 수집 결과: {len(qa_pairs)}개 Q&A 쌍")
        
        if qa_pairs:
            # 상세 분석
            print(f"\n🔍 확장 수집 데이터 분석:")
            
            complete_pairs = sum(1 for pair in qa_pairs if pair.get('answer'))
            print(f"   완전한 Q&A 쌍: {complete_pairs}/{len(qa_pairs)} ({complete_pairs/len(qa_pairs)*100:.1f}%)")
            
            # 점수 분포
            question_scores = [pair['question'].get('score', 0) for pair in qa_pairs]
            answer_scores = [pair['answer'].get('score', 0) for pair in qa_pairs if pair.get('answer')]
            quality_scores = [pair.get('quality_score', 0) for pair in qa_pairs]
            
            print(f"   질문 점수: {min(question_scores)} ~ {max(question_scores)} (평균: {sum(question_scores)/len(question_scores):.1f})")
            if answer_scores:
                print(f"   답변 점수: {min(answer_scores)} ~ {max(answer_scores)} (평균: {sum(answer_scores)/len(answer_scores):.1f})")
            print(f"   품질 점수: {min(quality_scores)} ~ {max(quality_scores)} (평균: {sum(quality_scores)/len(quality_scores):.1f})")
            
            # 고품질 데이터 분석
            high_quality = [pair for pair in qa_pairs if pair.get('quality_score', 0) >= 5]
            print(f"   고품질 Q&A (점수 5+): {len(high_quality)}개")
            
            # Excel 함수 분석
            excel_functions = ['VLOOKUP', 'INDEX', 'MATCH', 'SUMIF', 'COUNTIF', 'IF', 'PIVOT', 'XLOOKUP', 'LAMBDA', 'LET']
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
            
            print(f"\n📊 Excel 함수 언급 빈도:")
            for func, count in sorted(function_counts.items(), key=lambda x: x[1], reverse=True):
                if count > 0:
                    print(f"   {func}: {count}회")
            
            # 복잡도별 분류
            complexity_analysis = {
                'basic': 0,      # 기본 함수 (IF, SUM)
                'intermediate': 0,  # 중급 함수 (VLOOKUP, INDEX/MATCH)
                'advanced': 0    # 고급 함수 (LAMBDA, LET, 배열 함수)
            }
            
            for pair in qa_pairs:
                full_text = (
                    pair['question'].get('title', '') + ' ' + 
                    pair['question'].get('body_markdown', '') + ' ' + 
                    pair.get('answer', {}).get('body_markdown', '')
                ).upper()
                
                if any(func in full_text for func in ['LAMBDA', 'LET', 'DYNAMIC', 'SPILL']):
                    complexity_analysis['advanced'] += 1
                elif any(func in full_text for func in ['VLOOKUP', 'INDEX', 'MATCH', 'XLOOKUP']):
                    complexity_analysis['intermediate'] += 1
                else:
                    complexity_analysis['basic'] += 1
            
            print(f"\n📈 복잡도별 분류:")
            for level, count in complexity_analysis.items():
                print(f"   {level.capitalize()}: {count}개 ({count/len(qa_pairs)*100:.1f}%)")
            
            # 데이터 저장
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            extended_file = Path(Config.OUTPUT_DIR) / f"extended_api_collection_{timestamp}.json"
            
            save_data = []
            for pair in qa_pairs:
                save_item = {
                    'question': pair['question'],
                    'answer': pair.get('answer'),
                    'quality_score': pair.get('quality_score', 0),
                    'source': 'extended_api',
                    'collected_at': datetime.now().isoformat()
                }
                save_data.append(save_item)
            
            with open(extended_file, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"\n💾 확장 수집 데이터 저장:")
            print(f"   파일: {extended_file}")
            print(f"   크기: {extended_file.stat().st_size:,} bytes")
            
            # API 사용량 확인
            stats = collector.get_collection_stats()
            print(f"\n📊 API 사용 통계:")
            print(f"   오늘 사용량: {stats['requests_today']}")
            print(f"   남은 할당량: {stats['daily_quota_remaining']}")
        
        await collector.close()
        
        return qa_pairs
        
    except Exception as e:
        print(f"❌ 확장 API 수집 실패: {e}")
        import traceback
        traceback.print_exc()
        return []

def analyze_collection_efficiency():
    """수집 효율성 분석"""
    print(f"\n📈 수집 효율성 종합 분석")
    print("=" * 60)
    
    try:
        output_dir = Path(Config.OUTPUT_DIR)
        
        # 모든 수집 결과 파일 찾기
        all_files = {
            'api_basic': list(output_dir.glob("fixed_stackoverflow_data_*.json")),
            'api_large': list(output_dir.glob("large_scale_stackoverflow_*.json")),
            'api_extended': list(output_dir.glob("extended_api_collection_*.json")),
            'web_scraping': list(output_dir.glob("web_scraping_stackoverflow_*.json"))
        }
        
        total_qa_pairs = 0
        total_file_size = 0
        
        print("📁 수집 결과 파일 분석:")
        
        for method, files in all_files.items():
            if files:
                latest_file = max(files, key=lambda f: f.stat().st_mtime)
                
                try:
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    file_size = latest_file.stat().st_size
                    complete_pairs = sum(1 for item in data if item.get('answer'))
                    
                    print(f"\n   {method.replace('_', ' ').title()}:")
                    print(f"   📄 파일: {latest_file.name}")
                    print(f"   📊 Q&A 수: {len(data)}개")
                    print(f"   ✅ 완전한 쌍: {complete_pairs}개 ({complete_pairs/len(data)*100:.1f}%)")
                    print(f"   💾 크기: {file_size:,} bytes")
                    
                    total_qa_pairs += complete_pairs
                    total_file_size += file_size
                    
                except Exception as e:
                    print(f"   ❌ {method} 파일 분석 실패: {e}")
            else:
                print(f"\n   {method.replace('_', ' ').title()}: 파일 없음")
        
        print(f"\n🎯 전체 수집 성과:")
        print(f"   총 Q&A 쌍: {total_qa_pairs:,}개")
        print(f"   총 데이터 크기: {total_file_size:,} bytes ({total_file_size/1024/1024:.1f} MB)")
        
        if total_qa_pairs > 0:
            print(f"   평균 Q&A 크기: {total_file_size/total_qa_pairs:.0f} bytes/쌍")
        
        # 권장사항
        print(f"\n💡 수집 전략 권장사항:")
        print(f"   ✅ API 방식: 안정적이고 효율적 (메인 전략)")
        print(f"   🔄 확장 수집: 더 긴 기간, 더 많은 페이지로 데이터 확보")
        print(f"   🌐 웹 스크래핑: API 제한 시 보조 수단 (403 오류 해결 필요)")
        print(f"   📊 품질 관리: 답변이 포함된 완전한 Q&A만 수집")
        
    except Exception as e:
        print(f"❌ 효율성 분석 실패: {e}")

async def main():
    """메인 함수"""
    print("🚀 Stack Overflow 하이브리드 수집 시스템 종합 테스트")
    print("=" * 80)
    
    # 확장된 API 수집 테스트
    extended_results = await test_extended_api_collection()
    
    # 수집 효율성 분석
    analyze_collection_efficiency()
    
    print(f"\n🎉 하이브리드 수집 시스템 검증 완료!")
    
    if extended_results:
        print(f"   ✅ 확장 API 수집 성공: {len(extended_results)}개 Q&A")
        print(f"   🎯 API 방식이 가장 효율적이고 안정적")
        print(f"   📈 더 많은 데이터가 필요할 때 기간과 페이지 확장 권장")
    else:
        print(f"   ⚠️ 추가 수집 전략 필요")
    
    print(f"\n🏁 테스트 완료!")

if __name__ == "__main__":
    asyncio.run(main())