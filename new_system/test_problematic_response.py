#!/usr/bin/env python3
"""
Test the problematic response that keeps appearing in the data
"""

import sys
import os
sys.path.insert(0, '/Users/kevin/bigdata/new_system')

from bot_detection.advanced_bot_detector import AdvancedBotDetector

def test_problematic_response():
    """Test the specific problematic response"""
    print("🚀 Testing Problematic Response Detection")
    
    # Initialize detector
    detector = AdvancedBotDetector()
    
    # The problematic response that keeps appearing
    problematic_response = """varArray = Range(myRange).Value 'This is what I use to pick up the data from the spreadsheet

    varArray2(row, col) = varArray(rowY, colX) 'I run some code to create the new array

    Range(pasteRange).Value = varArray2 'I put the values in the new array where I need on the sheet

> Any thoughts?

I would suggest that we need more of your r/VBA code, primarily how each of the variables, constants, range objects, and (variant?) arrays in the code in your opening post are defined (*Dim*ensioned), and at least the loop to show how your varArray2 array is created."""
    
    test_case = {
        'body': problematic_response,
        'author': 'unknown_user',
        'score': 5,
        'created_utc': 1726666800
    }
    
    print("\n📊 Testing Problematic Response:")
    print("=" * 80)
    print(f"Content: {problematic_response[:200]}...")
    
    result = detector.detect_bot_comprehensive(test_case)
    
    print(f"\nResult: {'🚨 BOT' if result.is_bot else '✅ HUMAN'}")
    print(f"Confidence: {result.confidence:.2f}")
    print(f"Bot Type: {result.bot_type.value}")
    print(f"Total Indicators: {len(result.indicators)}")
    
    if result.indicators:
        print("\nDetailed Indicators:")
        for indicator in result.indicators:
            print(f"  • {indicator}")
    
    # Test with the simple interface
    simple_result = detector.is_bot_response(problematic_response)
    print(f"\nSimple Interface Result: {'🚨 BOT' if simple_result else '✅ HUMAN'}")
    
    print("\n" + "=" * 80)
    print("🔍 Analysis Summary:")
    print(f"The problematic response should be detected as: {'BOT' if result.is_bot else 'HUMAN'}")
    print(f"This {'will' if result.is_bot else 'will NOT'} be filtered out by the new system.")

if __name__ == "__main__":
    test_problematic_response()