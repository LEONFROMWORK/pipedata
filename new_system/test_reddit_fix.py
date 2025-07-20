#!/usr/bin/env python3
"""
Test script to verify Reddit text extraction fix
"""
import os
import sys
import json
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from output.dataset_generator import JSONLDatasetGenerator

def test_reddit_text_extraction():
    """Test Reddit text extraction with sample data"""
    
    # Create sample Reddit Q&A pair as it comes from main_pipeline
    sample_reddit_pair = {
        'source': 'reddit',
        'question': {
            'title': 'How to use VLOOKUP in Excel?',
            'body': 'I need help with VLOOKUP function. Can someone explain how to use it?',
            'text': 'How to use VLOOKUP in Excel?\nI need help with VLOOKUP function. Can someone explain how to use it?',
            'body_markdown': 'How to use VLOOKUP in Excel?\nI need help with VLOOKUP function. Can someone explain how to use it?',
            'reddit_id': 'test123',
            'permalink': '/r/excel/comments/test123',
            'tags': ['excel'],
            'score': 10
        },
        'answer': {
            'text': 'VLOOKUP syntax is =VLOOKUP(lookup_value, table_array, col_index_num, [range_lookup])',
            'body_markdown': 'VLOOKUP syntax is =VLOOKUP(lookup_value, table_array, col_index_num, [range_lookup])',
            'score': 15,
            'reddit_id': 'answer456',
            'author': 'ExcelExpert',
            'permalink': '/r/excel/comments/test123/answer456'
        },
        'reddit_metadata': {
            'op_confirmed': True,
            'solution_type': 'verified',
            'total_comments': 5
        },
        'quality_metrics': {
            'overall_score': 8.5,
            'raw_question_score': 7.0,
            'raw_answer_score': 9.0
        }
    }
    
    print("🧪 Testing Reddit text extraction fix...")
    print(f"Input question text: '{sample_reddit_pair['question']['text']}'")
    print(f"Input answer text: '{sample_reddit_pair['answer']['text']}'")
    
    # Initialize dataset generator
    generator = JSONLDatasetGenerator()
    
    # Test the content extraction methods directly
    question_content = generator._extract_question_content(sample_reddit_pair)
    answer_content = generator._extract_answer_content(sample_reddit_pair)
    
    print(f"\n✅ Extracted question text: '{question_content.text}'")
    print(f"✅ Extracted answer text: '{answer_content.text}'")
    
    # Check if extraction worked
    if question_content.text and answer_content.text:
        print("\n🎉 SUCCESS: Reddit text extraction is working correctly!")
        return True
    else:
        print("\n❌ FAILED: Reddit text extraction is still not working")
        return False

if __name__ == "__main__":
    success = test_reddit_text_extraction()
    sys.exit(0 if success else 1)