#!/usr/bin/env python3
"""
Debug API environment to match exactly what API server does
"""
import asyncio
import logging
from datetime import datetime, timedelta

from pipeline.main_pipeline import ExcelQAPipeline

# Set up logging same as API server
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def debug_api_environment():
    """Debug exactly what API server does"""
    print("=== API Environment Debug ===")
    
    try:
        # Exact same parameters as API server
        sources = ['stackoverflow']
        max_pages = 1
        target_count = 3
        
        print(f"API parameters: sources={sources}, max_pages={max_pages}, target_count={target_count}")
        
        # Create pipeline exactly like API server
        pipeline = ExcelQAPipeline()
        
        # Call run_full_pipeline WITHOUT from_date (like old API server did)
        print("Calling run_full_pipeline WITHOUT from_date (old API behavior)...")
        result = await pipeline.run_full_pipeline(
            sources=sources,
            max_pages=max_pages,
            target_count=target_count
        )
        
        print(f"Result status: {result['execution_summary']['status']}")
        print(f"Data flow: {result['data_flow']}")
        print(f"Errors: {result['errors']}")
        
        if result['data_flow']['collected'] == 0:
            print("\n=== 0 collection confirmed! Testing with 30-day from_date ===")
            
            # Test with 30-day from_date
            from_date = datetime.now() - timedelta(days=30)
            print(f"Testing with from_date: {from_date}")
            
            result2 = await pipeline.run_full_pipeline(
                from_date=from_date,
                sources=sources,
                max_pages=max_pages,
                target_count=target_count
            )
            
            print(f"With 30-day from_date:")
            print(f"Result status: {result2['execution_summary']['status']}")
            print(f"Data flow: {result2['data_flow']}")
            print(f"Final count: {result2['data_flow']['final_output']}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await pipeline.so_collector.close()

if __name__ == "__main__":
    asyncio.run(debug_api_environment())