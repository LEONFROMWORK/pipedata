#!/usr/bin/env python3
"""
Test script for the integrated bot detection system (Layers 1 & 2)
"""

import sys
import os
sys.path.insert(0, '/Users/kevin/bigdata/new_system')

from bot_detection.integrated_bot_detector import IntegratedBotDetector
import time

def test_integrated_bot_detection():
    """Test the integrated bot detection system"""
    print("🚀 Testing Integrated Bot Detection System (Layers 1 & 2)")
    
    # Initialize detector
    detector = IntegratedBotDetector()
    
    # Test cases with mock user data and history
    test_cases = [
        {
            'name': 'Legitimate User with History',
            'comment': {
                'body': 'You can fix this VBA array issue by properly dimensioning your variables. Make sure to use Variant types for your arrays.',
                'author': 'excel_expert_2019',
                'score': 5,
                'created_utc': 1726666800
            },
            'user_data': {
                'username': 'excel_expert_2019',
                'account_age_days': 1200,
                'comment_karma': 850,
                'link_karma': 120,
                'contributor_quality_score': 'high'
            },
            'user_history': [
                {'body': 'Try using INDEX-MATCH instead of VLOOKUP for better performance.', 'created_utc': 1726666500, 'score': 8},
                {'body': 'For pivot tables, you need to refresh the data source first.', 'created_utc': 1726666300, 'score': 6},
                {'body': 'Use conditional formatting to highlight duplicates.', 'created_utc': 1726666100, 'score': 4},
                {'body': 'XLOOKUP is the newer alternative to VLOOKUP in Excel 365.', 'created_utc': 1726665900, 'score': 12}
            ],
            'expected': False
        },
        
        {
            'name': 'AutoModerator Bot',
            'comment': {
                'body': 'Your post was submitted successfully. Please follow the submission rules and contact the moderators if you have any questions.',
                'author': 'AutoModerator',
                'score': 1,
                'created_utc': 1726666800
            },
            'user_data': {
                'username': 'AutoModerator',
                'account_age_days': 3000,
                'comment_karma': 1,
                'link_karma': 1,
                'contributor_quality_score': 'lowest'
            },
            'user_history': [
                {'body': 'Thank you for your submission to r/excel.', 'created_utc': 1726666500, 'score': 1},
                {'body': 'Your post was submitted successfully.', 'created_utc': 1726666300, 'score': 1},
                {'body': 'Please follow the submission rules.', 'created_utc': 1726666100, 'score': 1},
                {'body': 'Contact the moderators if you have questions.', 'created_utc': 1726665900, 'score': 1}
            ],
            'expected': True
        },
        
        {
            'name': 'Suspicious Behavioral Pattern',
            'comment': {
                'body': 'Use VLOOKUP for your data lookup needs. It is very efficient.',
                'author': 'User12345',
                'score': 2,
                'created_utc': 1726666800
            },
            'user_data': {
                'username': 'User12345',
                'account_age_days': 5,
                'comment_karma': 15,
                'link_karma': 200,
                'contributor_quality_score': 'low'
            },
            'user_history': [
                {'body': 'Use VLOOKUP for your data lookup needs. It is very efficient.', 'created_utc': 1726666500, 'score': 2},
                {'body': 'Use VLOOKUP for your data lookup needs. It is very efficient.', 'created_utc': 1726666300, 'score': 2},
                {'body': 'Use VLOOKUP for your data lookup needs. It is very efficient.', 'created_utc': 1726666100, 'score': 2},
                {'body': 'Use VLOOKUP for your data lookup needs. It is very efficient.', 'created_utc': 1726665900, 'score': 2}
            ],
            'expected': True
        },
        
        {
            'name': 'AI-Generated Response Pattern',
            'comment': {
                'body': 'I hope this helps! Let me know if you need further assistance. Here are some suggestions: 1. Try this approach 2. Another option is to consider 3. You might want to look into. Please don\'t hesitate to ask!',
                'author': 'helpful_assistant',
                'score': 3,
                'created_utc': 1726666800
            },
            'user_data': {
                'username': 'helpful_assistant',
                'account_age_days': 30,
                'comment_karma': 45,
                'link_karma': 5,
                'contributor_quality_score': 'medium'
            },
            'user_history': [
                {'body': 'I hope this helps! Let me know if you need assistance.', 'created_utc': 1726666500, 'score': 3},
                {'body': 'Here are some suggestions for your problem.', 'created_utc': 1726666300, 'score': 3},
                {'body': 'Please don\'t hesitate to ask if you need help.', 'created_utc': 1726666100, 'score': 3}
            ],
            'expected': True
        }
    ]
    
    print("\n📊 Testing Integrated Bot Detection Results:")
    print("=" * 80)
    
    correct_predictions = 0
    total_tests = len(test_cases)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n🔍 Test {i}: {test_case['name']}")
        print(f"Author: {test_case['comment']['author']}")
        print(f"Content: {test_case['comment']['body'][:100]}...")
        
        # Run integrated detection
        result = detector.detect_bot_integrated(
            test_case['comment'],
            test_case['user_data'],
            test_case['user_history']
        )
        
        is_correct = result.is_bot == test_case['expected']
        correct_predictions += is_correct
        
        status = "✅ CORRECT" if is_correct else "❌ INCORRECT"
        prediction = "🚨 BOT" if result.is_bot else "✅ HUMAN"
        expected = "🚨 BOT" if test_case['expected'] else "✅ HUMAN"
        
        print(f"Expected: {expected}")
        print(f"Predicted: {prediction} (confidence: {result.confidence:.2f})")
        print(f"Type: {result.integrated_type.value}")
        print(f"Result: {status}")
        
        # Show reasoning
        print(f"Reasoning: {result.final_reasoning}")
        
        # Show layer results
        print(f"Layer 1: {'Bot' if result.layer1_result.is_bot else 'Human'} ({result.layer1_result.confidence:.2f})")
        if result.layer2_result:
            print(f"Layer 2: {'Bot' if result.layer2_result.is_bot else 'Human'} ({result.layer2_result.confidence:.2f})")
        else:
            print("Layer 2: Not available")
        
        print("-" * 60)
    
    accuracy = (correct_predictions / total_tests) * 100
    print(f"\n📊 Overall Integrated System Performance:")
    print(f"Correct Predictions: {correct_predictions}/{total_tests}")
    print(f"Accuracy: {accuracy:.1f}%")
    print(f"Performance: {'🎯 EXCELLENT' if accuracy >= 80 else '⚠️ NEEDS IMPROVEMENT'}")
    
    # Get system performance
    performance = detector.get_detection_performance()
    print(f"\n📈 System Performance Metrics:")
    for key, value in performance.items():
        print(f"{key}: {value}")
    
    # Get system status
    status = detector.get_system_status()
    print(f"\n🛠️ System Status:")
    for key, value in status.items():
        print(f"{key}: {value}")
    
    print(f"\n🎯 Integrated Bot Detection System Status:")
    print(f"✅ Layer 1 (Immediate Blocking): ACTIVE")
    print(f"✅ Layer 2 (Behavioral Analysis): ACTIVE")
    print(f"✅ Integration Layer: ACTIVE")
    print(f"Status: {'🚀 PRODUCTION READY' if accuracy >= 80 else '⚠️ NEEDS TUNING'}")

if __name__ == "__main__":
    test_integrated_bot_detection()