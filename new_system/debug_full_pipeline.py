#!/usr/bin/env python3
"""
Debug Full Pipeline to find where SO data gets lost
"""
import asyncio
import logging
from datetime import datetime, timedelta

from pipeline.main_pipeline import ExcelQAPipeline

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def debug_full_pipeline():
    """Debug the full pipeline to find where Stack Overflow data disappears"""
    print("=== Full Pipeline Stack Overflow Debug ===")
    
    try:
        pipeline = ExcelQAPipeline()
        
        from_date = datetime.now() - timedelta(days=30)
        sources = ['stackoverflow']
        max_pages = 1
        target_count = 10
        
        print(f"Running full pipeline: sources={sources}, max_pages={max_pages}, target_count={target_count}")
        
        # Run the full pipeline
        result = await pipeline.run_full_pipeline(
            from_date=from_date,
            max_pages=max_pages,
            target_count=target_count,
            sources=sources
        )
        
        print("\n=== FINAL PIPELINE RESULT ===")
        print(f"Result keys: {list(result.keys())}")
        
        if 'data_flow' in result:
            data_flow = result['data_flow']
            print(f"Data Flow:")
            print(f"  - Collected: {data_flow.get('collected', 0)}")
            print(f"  - Processed: {data_flow.get('processed', 0)}")  
            print(f"  - Quality Filtered: {data_flow.get('quality_filtered', 0)}")
            print(f"  - Deduplicated: {data_flow.get('deduplicated', 0)}")
            print(f"  - Final Output: {data_flow.get('final_output', 0)}")
        
        print(f"Pipeline State:")
        state = pipeline.state
        print(f"  - Current Stage: {state.current_stage}")
        print(f"  - Collected Count: {state.collected_count}")
        print(f"  - Processed Count: {state.processed_count}")
        print(f"  - Quality Filtered Count: {state.quality_filtered_count}")
        print(f"  - Final Count: {state.final_count}")
        print(f"  - Errors: {state.errors}")
        
        if 'dataset_path' in result:
            print(f"Dataset generated at: {result['dataset_path']}")
        
        print(f"Success: {result.get('success', False)}")
        
    except Exception as e:
        print(f"Error during full pipeline debug: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await pipeline.so_collector.close()

if __name__ == "__main__":
    asyncio.run(debug_full_pipeline())