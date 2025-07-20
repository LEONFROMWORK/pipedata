#!/usr/bin/env python3
"""
Reddit 데이터 수집 디버깅 스크립트
"""
import os
import sys
import asyncio
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.cache import LocalCache, APICache
from collectors.reddit_collector import RedditCollector

async def debug_reddit_collection():
    """Reddit 데이터 수집 과정을 디버깅"""
    
    print("🔍 Reddit 데이터 수집 디버깅 시작...")
    
    try:
        # Initialize cache and collector
        cache = LocalCache(Path("data/cache.db"))
        api_cache = APICache(cache)
        collector = RedditCollector(api_cache)
        
        print("✅ Reddit 컬렉터 초기화 완료")
        
        # Collect some Reddit data
        print("📥 Reddit 데이터 수집 중...")
        results = await collector.collect_excel_discussions(
            from_date=None,
            max_submissions=2
        )
        
        print(f"📊 수집된 결과 개수: {len(results)}")
        
        # Analyze each result
        for i, result in enumerate(results):
            print(f"\n--- Result {i+1} ---")
            print(f"🔍 Type: {type(result)}")
            print(f"🔍 Has submission: {hasattr(result, 'submission')}")
            print(f"🔍 Has solution: {hasattr(result, 'solution')}")
            
            if hasattr(result, 'submission'):
                submission = result.submission
                print(f"📝 Submission type: {type(submission)}")
                
                if hasattr(submission, 'title'):
                    title = submission.title or ''
                    body = submission.selftext or ''
                    print(f"📝 Title: '{title[:100]}...'")
                    print(f"📝 Body: '{body[:100]}...'")
                    print(f"📝 Title length: {len(title)}, Body length: {len(body)}")
                else:
                    # Dict case
                    title = submission.get('title', '')
                    body = submission.get('selftext', '')
                    print(f"📝 Title (dict): '{title[:100]}...'")
                    print(f"📝 Body (dict): '{body[:100]}...'")
                    print(f"📝 Title length: {len(title)}, Body length: {len(body)}")
            
            if hasattr(result, 'solution'):
                solution = result.solution
                print(f"💬 Solution type: {type(solution)}")
                
                if hasattr(solution, 'body'):
                    answer = solution.body or ''
                    print(f"💬 Answer: '{answer[:100]}...'")
                    print(f"💬 Answer length: {len(answer)}")
                else:
                    # Dict case
                    answer = solution.get('body', '')
                    print(f"💬 Answer (dict): '{answer[:100]}...'")
                    print(f"💬 Answer length: {len(answer)}")
        
        print(f"\n✅ 디버깅 완료. 총 {len(results)}개 결과 분석됨")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_reddit_collection())