#!/usr/bin/env python3
"""
Debug Stack Overflow collection to identify the issue
"""
import asyncio
import logging
from datetime import datetime, timedelta
from core.cache import LocalCache, APICache
from collectors.stackoverflow_collector import StackOverflowCollector

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def debug_stackoverflow():
    """Debug Stack Overflow collection"""
    print("=== Stack Overflow Collection Debug ===")
    
    # Initialize cache and collector
    local_cache = LocalCache("./data/debug_cache.db")
    api_cache = APICache(local_cache)
    collector = StackOverflowCollector(api_cache)
    
    try:
        print("1. Testing API connection and basic request...")
        
        # Test with last 30 days to get more data
        from_date = datetime.now() - timedelta(days=30)
        print(f"Collection period: {from_date} to {datetime.now()}")
        
        # Get just first page for debugging
        print("\n2. Calling collect_excel_questions with max_pages=1...")
        results = await collector.collect_excel_questions(
            from_date=from_date, 
            max_pages=1
        )
        
        print(f"\n3. Results: {len(results)} items collected")
        
        if results:
            print("\nFirst result sample:")
            first_result = results[0]
            print(f"- Question ID: {first_result.get('question_id')}")
            print(f"- Title: {first_result.get('title', '')[:100]}...")
            print(f"- Is Answered: {first_result.get('is_answered')}")
            print(f"- Has Accepted Answer: {first_result.get('accepted_answer_id') is not None}")
            print(f"- Score: {first_result.get('score')}")
        else:
            print("No results - investigating further...")
            
            # Let's test the API directly
            print("\n4. Testing direct API call...")
            import httpx
            from urllib.parse import urlencode
            from config import Config
            
            params = {
                'site': 'stackoverflow',
                'order': 'desc',
                'sort': 'creation',
                'tagged': 'excel-formula',
                'page': 1,
                'pagesize': 10,
                'fromdate': int(from_date.timestamp()),
                'key': Config.STACKOVERFLOW_API_KEY
            }
            
            url = f"https://api.stackexchange.com/2.3/questions?" + urlencode(params)
            
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                data = response.json()
                
                print(f"API Response Status: {response.status_code}")
                print(f"Items returned: {len(data.get('items', []))}")
                print(f"Has more: {data.get('has_more', False)}")
                print(f"Quota remaining: {data.get('quota_remaining', 'unknown')}")
                
                if data.get('items'):
                    sample_question = data['items'][0]
                    print(f"\nSample question:")
                    print(f"- ID: {sample_question.get('question_id')}")
                    print(f"- Title: {sample_question.get('title', '')[:100]}...")
                    print(f"- Is Answered: {sample_question.get('is_answered')}")
                    print(f"- Accepted Answer ID: {sample_question.get('accepted_answer_id')}")
                    print(f"- Score: {sample_question.get('score')}")
                    print(f"- Tags: {sample_question.get('tags', [])}")
                
    except Exception as e:
        print(f"Error during debug: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await collector.close()

if __name__ == "__main__":
    asyncio.run(debug_stackoverflow())