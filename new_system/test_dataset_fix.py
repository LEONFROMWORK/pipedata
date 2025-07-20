#!/usr/bin/env python3
"""
Direct test of dataset generator fix for Reddit text extraction
"""
import os
import sys
import json
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from output.dataset_generator import JSONLDatasetGenerator

def test_reddit_dataset_generation():
    """Test dataset generation with sample Reddit data as it comes from main pipeline"""
    
    # Create sample Reddit Q&A pair exactly as main_pipeline creates it
    sample_reddit_pair = {
        'source': 'reddit',
        'question': {
            'title': 'How to calculate percentage in Excel formula?',
            'body': 'I need to calculate what percentage 50 is of 200. What formula should I use?',
            'text': 'How to calculate percentage in Excel formula?\nI need to calculate what percentage 50 is of 200. What formula should I use?',
            'body_markdown': 'How to calculate percentage in Excel formula?\nI need to calculate what percentage 50 is of 200. What formula should I use?',
            'reddit_id': 'test123',
            'permalink': '/r/excel/comments/test123',
            'tags': ['excel'],
            'score': 15
        },
        'answer': {
            'text': 'You can use the formula =A1/B1*100 where A1 is 50 and B1 is 200. This will give you 25%.',
            'body_markdown': 'You can use the formula =A1/B1*100 where A1 is 50 and B1 is 200. This will give you 25%.',
            'score': 25,
            'reddit_id': 'answer456',
            'author': 'ExcelHelper',
            'permalink': '/r/excel/comments/test123/answer456'
        },
        'reddit_metadata': {
            'op_confirmed': True,
            'solution_type': 'verified',
            'total_comments': 8
        },
        'quality_metrics': {
            'overall_score': 9.2,
            'raw_question_score': 8.5,
            'raw_answer_score': 9.8
        }
    }
    
    print("🧪 Testing Reddit dataset generation with our fix...")
    print(f"✅ Input question text: '{sample_reddit_pair['question']['text']}'")
    print(f"✅ Input answer text: '{sample_reddit_pair['answer']['text']}'")
    
    # Initialize dataset generator
    generator = JSONLDatasetGenerator()
    
    try:
        # Test the dataset generation process
        test_output_dir = Path("/tmp/reddit_test_output")
        test_output_dir.mkdir(exist_ok=True)
        
        # Generate test dataset
        dataset_path = generator.generate_dataset(
            [sample_reddit_pair],
            metadata={'test': True},
            data_sources=['reddit']
        )
        
        print(f"\n📁 Generated dataset: {dataset_path}")
        
        # Read and verify the generated file
        with open(dataset_path, 'r', encoding='utf-8') as f:
            line = f.readline().strip()
            if line:
                data = json.loads(line)
                
                question_text = data['content']['question']['text']
                answer_text = data['content']['answer']['text']
                
                print(f"\n✅ Extracted question text: '{question_text}'")
                print(f"✅ Extracted answer text: '{answer_text}'")
                
                # Check if our fix worked
                if question_text and answer_text:
                    print("\n🎉 SUCCESS: Reddit text extraction fix is working!")
                    print(f"   Question length: {len(question_text)} chars")
                    print(f"   Answer length: {len(answer_text)} chars")
                    return True
                else:
                    print("\n❌ FAILED: Text fields are still empty")
                    print(f"   Question: '{question_text}'")
                    print(f"   Answer: '{answer_text}'")
                    return False
            else:
                print("\n❌ FAILED: No data in generated file")
                return False
                
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_reddit_dataset_generation()
    sys.exit(0 if success else 1)