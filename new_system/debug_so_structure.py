#!/usr/bin/env python3
"""
Debug Stack Overflow data structure
"""
import asyncio
import logging
from datetime import datetime, timedelta
import json

from core.cache import LocalCache, APICache
from collectors.stackoverflow_collector import StackOverflowCollector

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def debug_so_structure():
    """Debug Stack Overflow data structure returned by collector"""
    print("=== Stack Overflow Data Structure Debug ===")
    
    try:
        local_cache = LocalCache("./data/debug_cache.db")
        api_cache = APICache(local_cache)
        collector = StackOverflowCollector(api_cache)
        
        from_date = datetime.now() - timedelta(days=30)
        
        so_pairs = await collector.collect_excel_questions(
            from_date=from_date,
            max_pages=1
        )
        
        print(f"Collected {len(so_pairs)} Stack Overflow items")
        
        if so_pairs:
            print("\n=== FIRST ITEM STRUCTURE ===")
            first_item = so_pairs[0]
            print(f"Type: {type(first_item)}")
            print(f"Keys: {list(first_item.keys()) if isinstance(first_item, dict) else 'Not a dict'}")
            
            print("\n=== FULL FIRST ITEM (JSON) ===")
            print(json.dumps(first_item, indent=2, default=str)[:2000] + "..." if len(str(first_item)) > 2000 else json.dumps(first_item, indent=2, default=str))
            
            # Check specific fields that pipeline expects
            print("\n=== FIELD CHECK ===")
            print(f"Has 'question' key: {'question' in first_item}")
            print(f"Has 'answer' key: {'answer' in first_item}")
            print(f"Has 'source' key: {'source' in first_item}")
            
            # Show all top-level keys
            print(f"\nAll keys: {list(first_item.keys())}")
            
        else:
            print("No items collected")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await collector.close()

if __name__ == "__main__":
    asyncio.run(debug_so_structure())