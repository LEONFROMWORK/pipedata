#!/usr/bin/env python3
"""
Integration test for the advanced bot detection system
"""

import sys
import os
sys.path.insert(0, '/Users/kevin/bigdata/new_system')

from bot_detection.advanced_bot_detector import AdvancedBotDetector
import json

def test_integration():
    """Test the integration with actual Reddit data"""
    print("🚀 Testing Integration with Real Reddit Data")
    
    # Read current Reddit data file
    reddit_file = '/Users/kevin/bigdata/data/output/year=2025/month=07/day=18/reddit_20250718.jsonl'
    
    try:
        with open(reddit_file, 'r', encoding='utf-8') as f:
            data = json.loads(f.read().strip())
            
        print(f"📊 Current Reddit Data Item:")
        print(f"ID: {data['id']}")
        print(f"Question: {data['user_question'][:100]}...")
        print(f"Response: {data['assistant_response'][:100]}...")
        print(f"Quality Score: {data['metadata']['quality_score']}")
        print(f"Source: {data['metadata']['source']}")
        
        # Test with advanced bot detector
        detector = AdvancedBotDetector()
        
        print("\n🔍 Testing Advanced Bot Detection:")
        
        # Test the response
        response_result = detector.detect_bot_comprehensive({
            'body': data['assistant_response'],
            'author': 'unknown_user',
            'score': 5,
            'created_utc': 1726666800
        })
        
        print(f"Response Analysis:")
        print(f"  Is Bot: {'🚨 YES' if response_result.is_bot else '✅ NO'}")
        print(f"  Confidence: {response_result.confidence:.2f}")
        print(f"  Bot Type: {response_result.bot_type.value}")
        print(f"  Indicators: {len(response_result.indicators)}")
        
        # Test the question
        question_result = detector.detect_bot_comprehensive({
            'body': data['user_context'],
            'author': 'unknown_user',
            'score': 5,
            'created_utc': 1726666800
        })
        
        print(f"\nQuestion Analysis:")
        print(f"  Is Bot: {'🚨 YES' if question_result.is_bot else '✅ NO'}")
        print(f"  Confidence: {question_result.confidence:.2f}")
        print(f"  Bot Type: {question_result.bot_type.value}")
        print(f"  Indicators: {len(question_result.indicators)}")
        
        # Overall assessment
        print(f"\n📈 Overall Assessment:")
        should_be_filtered = response_result.is_bot or question_result.is_bot
        print(f"Should be filtered: {'🚨 YES' if should_be_filtered else '✅ NO'}")
        print(f"Current quality score: {data['metadata']['quality_score']}")
        
        if should_be_filtered:
            print("⚠️  This item should be filtered out by the new system")
        else:
            print("✅ This item is legitimate and should pass through")
            
    except FileNotFoundError:
        print(f"❌ Reddit data file not found: {reddit_file}")
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing Reddit data file: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    test_integration()