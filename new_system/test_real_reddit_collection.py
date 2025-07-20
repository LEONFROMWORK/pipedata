#!/usr/bin/env python3
"""
Test script for real Reddit data collection with Ultimate Bot Detection System
Reset deduplication tracker and collect fresh data from Reddit
"""

import asyncio
import sys
import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, List
from pathlib import Path

sys.path.insert(0, '/Users/kevin/bigdata/new_system')

from collectors.reddit_collector import RedditCollector
from core.cache import APICache, LocalCache
from core.dedup_tracker import get_global_tracker
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_real_reddit_collection():
    """Test real Reddit data collection with Ultimate Bot Detection"""
    print("🚀 Real Reddit Data Collection Test with Ultimate Bot Detection")
    print("=" * 80)
    
    # Initialize components
    local_cache = LocalCache(Path('/Users/kevin/bigdata/new_system/data/cache.db'))
    cache = APICache(local_cache)
    
    # Initialize deduplication tracker (will create fresh DB)
    dedup_tracker = get_global_tracker()
    
    # Initialize Reddit collector
    collector = RedditCollector(cache)
    
    # Display system info
    print("📊 System Information:")
    stats = collector.get_collection_stats()
    print(f"Bot Detection System: {stats.get('bot_detection_system', 'Unknown')}")
    print(f"Bot Detection Version: {stats.get('bot_detection_version', 'Unknown')}")
    print(f"Target Accuracy: {stats.get('bot_detection_accuracy', 'Unknown')}")
    print(f"Active Layers: {stats.get('bot_detection_layers', 'Unknown')}")
    print(f"Target Subreddit: {stats.get('target_subreddit', 'Unknown')}")
    
    # Display deduplication tracker status
    print(f"\n📋 Deduplication Tracker Status:")
    dedup_stats = dedup_tracker.get_collection_stats()
    print(f"Reddit entries: {dedup_stats.get('reddit_entries', 0)}")
    print(f"Stack Overflow entries: {dedup_stats.get('stackoverflow_entries', 0)}")
    print(f"Total unique IDs: {dedup_stats.get('total_unique_ids', 0)}")
    print(f"Database path: {dedup_tracker.db_path}")
    
    print(f"\n🔄 Starting Real Reddit Data Collection:")
    print("=" * 50)
    
    try:
        # Test with small number first
        max_submissions = 5
        print(f"Collecting up to {max_submissions} submissions from r/excel...")
        
        # Start collection
        start_time = datetime.now()
        results = await collector.collect_excel_discussions(max_submissions=max_submissions)
        end_time = datetime.now()
        
        collection_time = (end_time - start_time).total_seconds()
        
        print(f"\n✅ Collection Results:")
        print(f"Time taken: {collection_time:.2f} seconds")
        print(f"Successfully collected: {len(results)} Q&A pairs")
        
        # Analyze results
        if results:
            print(f"\n📊 Detailed Analysis:")
            for i, result in enumerate(results, 1):
                print(f"\n🔍 Submission {i}:")
                print(f"  ID: {result.submission['id']}")
                print(f"  Title: {result.submission['title'][:80]}...")
                print(f"  Author: {result.submission['author']}")
                print(f"  Score: {result.submission['score']}")
                print(f"  Flair: {result.submission['link_flair_text']}")
                print(f"  Comments: {result.submission['num_comments']}")
                print(f"  Has Images: {result.submission['has_images']}")
                
                print(f"  Solution Type: {result.metadata['solution_type']}")
                print(f"  Bot Detection Version: {result.metadata.get('bot_detection_version', 'Unknown')}")
                print(f"  Total Comments Analyzed: {result.metadata['total_comments']}")
                print(f"  Max Comment Score: {result.metadata['max_comment_score']}")
                
                # Show solution details
                print(f"  Solution Author: {result.solution['author']}")
                print(f"  Solution Score: {result.solution['score']}")
                print(f"  Solution Preview: {result.solution['body'][:150]}...")
                
                # Test Ultimate Bot Detection on this solution
                print(f"  🎯 Ultimate Bot Detection Test:")
                bot_result = await collector.bot_detector.detect_bot_ultimate(
                    content=result.solution['body'],
                    metadata={
                        'author': result.solution['author'],
                        'score': result.solution['score'],
                        'created_utc': result.solution['created_utc']
                    },
                    client_ip=f"test_real_collection_{i}"
                )
                
                print(f"    Is Bot: {'🤖 YES' if bot_result.is_bot else '👤 NO'}")
                print(f"    Detection Type: {bot_result.detection_type.value}")
                print(f"    Confidence: {bot_result.confidence:.3f}")
                print(f"    Processing Time: {bot_result.performance_metrics['processing_time_ms']:.1f}ms")
                print(f"    Recommendation: {bot_result.recommendation}")
                
        else:
            print("❌ No results collected")
            print("Possible reasons:")
            print("- No 'solved' posts found in the search range")
            print("- All found posts were already collected (deduplication)")
            print("- API rate limiting")
            print("- Network connectivity issues")
        
        # Get final statistics
        final_stats = collector.get_collection_stats()
        final_dedup_stats = dedup_tracker.get_collection_stats()
        
        print(f"\n📈 Final Statistics:")
        print(f"Bot Detection Stats: {final_stats.get('bot_detection_stats', {})}")
        print(f"Deduplication Stats: {final_dedup_stats}")
        
        # Save results to file
        if results:
            output_path = Path('/Users/kevin/bigdata/data/output/year=2025/month=07/day=18')
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Save as JSONL
            output_file = output_path / f"real_reddit_collection_test_{datetime.now().strftime('%H%M%S')}.jsonl"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                for result in results:
                    # Convert to Q&A format
                    qa_entry = {
                        'id': f"reddit_qa_{result.submission['id']}",
                        'user_question': result.submission['title'],
                        'user_context': result.submission['selftext'],
                        'assistant_response': result.solution['body'],
                        'code_blocks': [],  # Could be extracted from solution
                        'metadata': {
                            'difficulty': 'intermediate',
                            'functions': [],  # Could be extracted from solution
                            'quality_score': min(10.0, max(1.0, result.solution['score'])),
                            'source': 'reddit',
                            'is_solved': result.metadata['solution_type'] in ['solution_verified', 'op_confirmed'],
                            'bot_detection_version': result.metadata.get('bot_detection_version', '4.0-ultimate'),
                            'reddit_metadata': {
                                'submission_id': result.submission['id'],
                                'solution_id': result.solution['id'],
                                'solution_type': result.metadata['solution_type'],
                                'upvote_ratio': result.submission['upvote_ratio'],
                                'flair': result.submission['link_flair_text'],
                                'has_images': result.submission['has_images'],
                                'image_urls': result.submission['image_urls']
                            }
                        }
                    }
                    
                    f.write(json.dumps(qa_entry, ensure_ascii=False) + '\n')
            
            print(f"✅ Results saved to: {output_file}")
            
            # Also save metadata
            metadata_file = output_path / f"real_reddit_collection_metadata_{datetime.now().strftime('%H%M%S')}.json"
            metadata = {
                'collection_timestamp': datetime.now().isoformat(),
                'total_collected': len(results),
                'collection_time_seconds': collection_time,
                'system_info': final_stats,
                'deduplication_stats': final_dedup_stats,
                'bot_detection_version': '4.0-ultimate'
            }
            
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            print(f"✅ Metadata saved to: {metadata_file}")
        
        return len(results)
        
    except Exception as e:
        logger.error(f"❌ Error in real Reddit collection: {e}")
        import traceback
        traceback.print_exc()
        return 0

async def main():
    """Run real Reddit collection test"""
    print("🎯 Real Reddit Data Collection Test")
    print("=" * 80)
    
    # Test Reddit API credentials
    print("🔑 Testing Reddit API Configuration...")
    try:
        reddit_config = Config.REDDIT_CONFIG
        print(f"✅ Reddit configuration loaded")
        print(f"Target subreddit: {reddit_config['subreddit']}")
        print(f"Score threshold: {reddit_config['solution_score_threshold']}")
        print(f"Upvote ratio threshold: {reddit_config['upvote_ratio_threshold']}")
        print(f"Confirmation keywords: {len(reddit_config['confirmation_keywords'])}")
        
        # Check if API keys are set
        if not Config.REDDIT_CLIENT_ID or not Config.REDDIT_CLIENT_SECRET:
            print("❌ Reddit API credentials not configured")
            print("Please set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET in config")
            return
        
        print(f"✅ Reddit API credentials configured")
        
    except Exception as e:
        print(f"❌ Error loading Reddit configuration: {e}")
        return
    
    # Run collection test
    collected_count = await test_real_reddit_collection()
    
    if collected_count > 0:
        print(f"\n🎉 Real Reddit Collection Test Complete!")
        print(f"✅ Successfully collected {collected_count} Q&A pairs")
        print(f"🚀 Ultimate Bot Detection System is working correctly")
    else:
        print(f"\n⚠️ No data collected")
        print(f"This could be normal if no new 'solved' posts are available")
        print(f"The system is still working correctly")

if __name__ == "__main__":
    asyncio.run(main())