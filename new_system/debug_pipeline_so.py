#!/usr/bin/env python3
"""
Debug Pipeline Stack Overflow Collection
"""
import asyncio
import logging
from datetime import datetime, timedelta

from pipeline.main_pipeline import ExcelQAPipeline

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def debug_pipeline_so():
    """Debug Stack Overflow collection in pipeline context"""
    print("=== Pipeline Stack Overflow Debug ===")
    
    try:
        pipeline = ExcelQAPipeline()
        
        print("1. Testing multi-source collection with stackoverflow only...")
        
        from_date = datetime.now() - timedelta(days=30)
        sources = ['stackoverflow']
        max_pages = 1
        
        print(f"Parameters: from_date={from_date}, max_pages={max_pages}, sources={sources}")
        
        # Call the multi-source collection method directly
        raw_qa_pairs = await pipeline._run_multi_source_collection(
            from_date=from_date,
            max_pages=max_pages, 
            sources=sources
        )
        
        print(f"2. Raw collection results: {len(raw_qa_pairs)} items")
        
        if raw_qa_pairs:
            print("Sample item:")
            sample = raw_qa_pairs[0]
            print(f"- Source: {sample.get('source')}")
            print(f"- Question ID: {sample.get('question_id', 'N/A')}")
            print(f"- Title: {sample.get('title', 'N/A')[:100]}...")
            print(f"- Has Answer: {sample.get('accepted_answer') is not None}")
        else:
            print("No items collected - checking collector directly...")
            
            # Test the collector directly within pipeline context
            so_pairs = await pipeline.so_collector.collect_excel_questions(
                from_date=from_date,
                max_pages=max_pages
            )
            print(f"Direct collector test: {len(so_pairs)} items")
            
            if so_pairs:
                print("Direct collector sample:")
                sample = so_pairs[0]
                print(f"- Question ID: {sample.get('question_id')}")
                print(f"- Title: {sample.get('title', '')[:100]}...")
                print(f"- Is Answered: {sample.get('is_answered')}")
                print(f"- Has Accepted Answer: {sample.get('accepted_answer_id') is not None}")
        
    except Exception as e:
        print(f"Error during debug: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await pipeline.so_collector.close()

if __name__ == "__main__":
    asyncio.run(debug_pipeline_so())