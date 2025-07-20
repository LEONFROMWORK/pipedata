#!/usr/bin/env python3
"""
Comprehensive test for bot detection with various scenarios
"""

import sys
import os
sys.path.insert(0, '/Users/kevin/bigdata/new_system')

from bot_detection.advanced_bot_detector import AdvancedBotDetector

def test_comprehensive_bot_detection():
    """Test various bot detection scenarios"""
    print("🚀 Comprehensive Bot Detection Testing")
    
    detector = AdvancedBotDetector()
    
    test_cases = [
        # Legitimate responses
        {
            'name': 'Legitimate VBA Response',
            'body': """You need to ensure your array is properly dimensioned as Variant. Try this:
            
Dim varArray As Variant
Dim varArray2 As Variant
varArray = Range(myRange).Value
ReDim varArray2(1 To UBound(varArray), 1 To 2)

For i = 1 To UBound(varArray)
    varArray2(i, 1) = varArray(i, 1)
    varArray2(i, 2) = varArray(i, 2)
Next i""",
            'author': 'excel_helper',
            'score': 5,
            'expected': False
        },
        
        # AutoModerator responses  
        {
            'name': 'AutoModerator Post Rules',
            'body': """Your post was submitted successfully. Please follow the submission rules:

* Include your Excel version
* Provide sample data
* Be specific about your issue

If you need help, please contact the moderators of this subreddit.""",
            'author': 'AutoModerator',
            'score': 1,
            'expected': True
        },
        
        # Template-like responses
        {
            'name': 'Template Response',
            'body': """Thank you for your submission to r/excel.

**Please note:**
- [x] Check rule 1
- [x] Check rule 2  
- [x] Check rule 3

This action was performed automatically. Please contact the moderators if you have questions.""",
            'author': 'ModeratorBot',
            'score': 1,
            'expected': True
        },
        
        # AI-generated patterns
        {
            'name': 'AI-Generated Response',
            'body': """I hope this helps! Let me know if you need further assistance. 

Here's what I would suggest:
1. Try this approach
2. Another option is to consider
3. You might want to look into

Please don't hesitate to ask if you have any questions. I'm here to help!""",
            'author': 'helpful_assistant',
            'score': 3,
            'expected': True  # Multiple AI patterns
        },
        
        # Spam-like content
        {
            'name': 'Spam Content',
            'body': """Check out this amazing Excel course! Limited time offer - 50% discount!
            
Click here to sign up now: [link]
            
Don't miss this exclusive deal! Order today and save money!""",
            'author': 'PromoBot123',
            'score': 0,
            'expected': True
        },
        
        # Suspicious username patterns
        {
            'name': 'Suspicious Username',
            'body': """This is a normal response about Excel formulas. You should use VLOOKUP for this.""",
            'author': 'User12345',
            'score': 2,
            'expected': True  # Suspicious username pattern
        }
    ]
    
    print("\n📊 Testing Multiple Bot Detection Scenarios:")
    print("=" * 80)
    
    correct_predictions = 0
    total_tests = len(test_cases)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n🔍 Test {i}: {test_case['name']}")
        print(f"Author: {test_case['author']}")
        print(f"Content: {test_case['body'][:100]}...")
        
        result = detector.detect_bot_comprehensive({
            'body': test_case['body'],
            'author': test_case['author'],
            'score': test_case['score'],
            'created_utc': 1726666800
        })
        
        is_correct = result.is_bot == test_case['expected']
        correct_predictions += is_correct
        
        status = "✅ CORRECT" if is_correct else "❌ INCORRECT"
        prediction = "🚨 BOT" if result.is_bot else "✅ HUMAN"
        expected = "🚨 BOT" if test_case['expected'] else "✅ HUMAN"
        
        print(f"Expected: {expected}")
        print(f"Predicted: {prediction} (confidence: {result.confidence:.2f})")
        print(f"Result: {status}")
        
        if result.indicators:
            print(f"Indicators ({len(result.indicators)}):")
            for indicator in result.indicators[:3]:  # Show first 3
                print(f"  • {indicator}")
        
        print("-" * 50)
    
    accuracy = (correct_predictions / total_tests) * 100
    print(f"\n📊 Overall Performance:")
    print(f"Correct Predictions: {correct_predictions}/{total_tests}")
    print(f"Accuracy: {accuracy:.1f}%")
    print(f"Performance: {'🎯 EXCELLENT' if accuracy >= 80 else '⚠️ NEEDS IMPROVEMENT'}")
    
    print(f"\n🎯 Bot Detection System Status:")
    print(f"✅ Layer 1 (Immediate Blocking): {'ACTIVE' if accuracy >= 70 else 'NEEDS TUNING'}")
    print(f"Status: Ready for {'production' if accuracy >= 80 else 'further development'}")

if __name__ == "__main__":
    test_comprehensive_bot_detection()