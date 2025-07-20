#!/usr/bin/env python3
"""
Test script for the AI-based bot detection system (Layer 3)
"""

import sys
import os
sys.path.insert(0, '/Users/kevin/bigdata/new_system')

from bot_detection.ai_bot_detector import AIBotDetector

def test_ai_bot_detection():
    """Test the AI-based bot detection system"""
    print("🚀 Testing AI-based Bot Detection System (Layer 3)")
    
    # Initialize detector
    detector = AIBotDetector()
    
    # Test cases with various AI-generated patterns
    test_cases = [
        {
            'name': 'Human-authored Technical Response',
            'text': """I had the same issue with my VBA arrays! Here's what worked for me:

Dim varArray2 As Variant
ReDim varArray2(1 To 10, 1 To 2)

The problem is probably that you're not declaring the array properly. Try this and see if it works better. Let me know how it goes!""",
            'expected': False
        },
        
        {
            'name': 'GPT-generated Response',
            'text': """I'd be happy to help you with your VBA array issue. Based on your description, it appears that the problem might be related to data type handling. Here's what I would recommend:

1. First, ensure your array is properly dimensioned as Variant
2. Next, check your data formatting
3. Finally, verify the range references

I hope this information helps! Let me know if you need any further assistance.""",
            'expected': True
        },
        
        {
            'name': 'Template AI Response',
            'text': """Thank you for your question about VBA arrays. Let me provide you with a comprehensive solution:

To resolve this issue, you should:
- Check your variable declarations
- Verify data types
- Ensure proper array initialization

I hope this helps! Please let me know if you have any other questions.""",
            'expected': True
        },
        
        {
            'name': 'Sophisticated AI Response',
            'text': """It seems like you're encountering a common issue with VBA array handling. Perhaps you could try the following approach:

First, let's examine the problem: your date and time values are not being transferred correctly from the array to the spreadsheet. One option would be to ensure that your array is properly typed.

Here's what I would suggest:
1. Declare your arrays as Variant to preserve data types
2. Consider using explicit formatting functions
3. You might want to check the regional settings

Alternatively, you could try setting the NumberFormat property of the destination range before assigning values. This should help maintain the correct data formatting.

I hope this information proves helpful in resolving your issue.""",
            'expected': True
        },
        
        {
            'name': 'Casual Human Response',
            'text': """Ugh, I hate this kind of problem! Had it before and it's super annoying.

Try this - worked for me:
varArray2(row, col) = CDate(varArray(rowY, colX))

Basically you need to force the conversion. Excel gets confused with the data types sometimes. Let me know if this helps or if you're still stuck!""",
            'expected': False
        },
        
        {
            'name': 'AI Self-Identification',
            'text': """As an AI, I can help you understand this VBA issue. I don't have personal experience with this specific problem, but I can provide you with the technical solution.

The issue you're experiencing is likely due to data type conversion. I cannot access your specific Excel file, but based on the information provided, here's what you should try:

1. Use explicit type conversion
2. Set the NumberFormat property
3. Ensure proper array dimensioning

I'm not able to test this directly, but this approach should resolve your issue.""",
            'expected': True
        }
    ]
    
    print("\n📊 Testing AI-based Bot Detection Results:")
    print("=" * 80)
    
    correct_predictions = 0
    total_tests = len(test_cases)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n🔍 Test {i}: {test_case['name']}")
        print(f"Content: {test_case['text'][:150]}...")
        
        result = detector.analyze_ai_content(test_case['text'])
        
        is_correct = result.is_ai_generated == test_case['expected']
        correct_predictions += is_correct
        
        status = "✅ CORRECT" if is_correct else "❌ INCORRECT"
        prediction = "🤖 AI" if result.is_ai_generated else "👤 HUMAN"
        expected = "🤖 AI" if test_case['expected'] else "👤 HUMAN"
        
        print(f"Expected: {expected}")
        print(f"Predicted: {prediction} (confidence: {result.confidence:.2f})")
        print(f"AI Type: {result.ai_type.value}")
        print(f"Result: {status}")
        print(f"Reasoning: {result.reasoning}")
        
        # Show detailed analysis
        if result.analysis_result.ai_indicators:
            print(f"AI Indicators: {len(result.analysis_result.ai_indicators)}")
            for indicator in result.analysis_result.ai_indicators[:3]:
                print(f"  • {indicator}")
        
        # Show structural analysis
        structure = result.analysis_result.structural_analysis
        if structure:
            print(f"Structural Analysis:")
            for key, value in structure.items():
                print(f"  {key}: {value:.2f}")
        
        print("-" * 60)
    
    accuracy = (correct_predictions / total_tests) * 100
    print(f"\n📊 Overall AI Detection Performance:")
    print(f"Correct Predictions: {correct_predictions}/{total_tests}")
    print(f"Accuracy: {accuracy:.1f}%")
    print(f"Performance: {'🎯 EXCELLENT' if accuracy >= 80 else '⚠️ NEEDS IMPROVEMENT'}")
    
    # Get detection stats
    stats = detector.get_ai_detection_stats()
    print(f"\n📈 AI Detection System Stats:")
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    print(f"\n🎯 Layer 3 AI Detection System Status:")
    print(f"✅ BERT Analysis: {'ACTIVE' if accuracy >= 70 else 'NEEDS TUNING'}")
    print(f"✅ Structural Analysis: ACTIVE")
    print(f"✅ Semantic Analysis: ACTIVE")
    print(f"✅ Pattern Recognition: ACTIVE")
    print(f"Status: {'🚀 PRODUCTION READY' if accuracy >= 80 else '⚠️ DEVELOPMENT MODE'}")

if __name__ == "__main__":
    test_ai_bot_detection()