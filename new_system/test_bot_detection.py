#!/usr/bin/env python3
"""
Test script for the advanced bot detection system
"""

import sys
import os
sys.path.insert(0, '/Users/kevin/bigdata/new_system')

from bot_detection.advanced_bot_detector import AdvancedBotDetector

def test_bot_detection():
    """Test the advanced bot detection system"""
    print("🚀 Testing Advanced Bot Detection System")
    
    # Initialize detector
    detector = AdvancedBotDetector()
    
    # Test cases
    test_cases = [
        {
            'body': "I would suggest that we need more of your r/VBA code, primarily how each of the variables, constants, range objects, and (variant?) arrays in the code in your opening post are defined (*Dim*ensioned), and at least the loop to show how your varArray2 array is created.",
            'author': 'helpful_user',
            'score': 3,
            'created_utc': 1726666800
        },
        {
            'body': "Your post was submitted successfully. Please contact the moderators if you have any questions.",
            'author': 'AutoModerator',
            'score': 1,
            'created_utc': 1726666800
        },
        {
            'body': "I am a bot, and this action was performed automatically. Please contact the moderators of this subreddit if you have any questions or concerns.",
            'author': 'AutoModerator',
            'score': 1,
            'created_utc': 1726666800
        },
        {
            'body': "To use VLOOKUP, you can try this formula: =VLOOKUP(A1, Sheet2!A:B, 2, FALSE). This will look up the value in A1 and return the corresponding value from column B.",
            'author': 'excel_expert',
            'score': 5,
            'created_utc': 1726666800
        }
    ]
    
    print("\n📊 Testing Bot Detection Results:")
    print("=" * 60)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n🔍 Test Case {i}:")
        print(f"Author: {test_case['author']}")
        print(f"Content: {test_case['body'][:100]}...")
        
        result = detector.detect_bot_comprehensive(test_case)
        
        print(f"Result: {'🚨 BOT' if result.is_bot else '✅ HUMAN'}")
        print(f"Confidence: {result.confidence:.2f}")
        print(f"Bot Type: {result.bot_type.value}")
        print(f"Indicators: {len(result.indicators)}")
        
        if result.indicators:
            for indicator in result.indicators[:3]:  # Show first 3 indicators
                print(f"  • {indicator}")
        
        print("-" * 40)
    
    print("\n📈 Detection System Stats:")
    stats = detector.get_detection_stats()
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    print("\n✅ Bot Detection Test Complete!")

if __name__ == "__main__":
    test_bot_detection()