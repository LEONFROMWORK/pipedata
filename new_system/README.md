# Excel Q&A Dataset Pipeline

TRD-compliant system for generating high-quality Excel Q&A datasets from Stack Overflow, optimized for local execution.

## Overview

This pipeline implements the specifications from PRD/TRD documents to create AI-ready datasets:

- **3-Tier Image Processing**: pytesseract OCR → img2table → OpenRouter AI enhancement
- **Quality Scoring**: TRD Section 4 weighted algorithm with batch normalization  
- **Semantic Deduplication**: sentence-transformers with 0.95 cosine similarity threshold
- **Local Optimization**: SQLite caching instead of Redis, local file storage
- **JSONL Output**: AI training-ready format with complete metadata

## Architecture

```
Pipeline Flow (TRD Section 6):
1. Data Collector → Stack Overflow API with custom filters
2. Triage → <img> tag detection for processing path decision  
3. Text/Image Processors → Parallel processing with fallback chains
4. Quality Scorer → Weighted scoring with normalization
5. Deduplicator → Semantic similarity-based removal
6. Dataset Generator → JSONL format with metadata
```

## Installation

1. **Install Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Install System Dependencies**:
   ```bash
   # For OCR (Ubuntu/Debian)
   sudo apt-get install tesseract-ocr
   
   # For OCR (macOS)
   brew install tesseract
   ```

3. **Configuration**:
   ```bash
   cp .env.example .env
   # Edit .env with your API keys
   ```

4. **Validate Setup**:
   ```bash
   python main.py validate
   ```

## Usage

### Command Line Interface

**Run Full Pipeline**:
```bash
python main.py pipeline --max-pages 50 --target-count 1000
```

**Incremental Collection** (last 24 hours):
```bash
python main.py pipeline --incremental --hours-back 24
```

**Web Dashboard**:
```bash
python main.py dashboard
# Open http://localhost:8000
```

### API Keys Required

- **Stack Overflow API Key**: [Register here](https://stackapps.com/)
- **OpenRouter API Key**: [Register here](https://openrouter.ai/) (for AI image enhancement)

## Configuration

Key settings in `config.py`:

```python
# Quality scoring weights (TRD Section 4)
QUALITY_SCORING = {
    'weights': {'w_q': 0.4, 'w_a': 0.5, 'w_c': 0.1},
    'threshold': 5.0
}

# Deduplication threshold (TRD Section 3.5)  
DEDUPLICATION = {
    'similarity_threshold': 0.95,
    'model': 'all-MiniLM-L6-v2'
}

# Image processing tiers (TRD Section 3.3b)
IMAGE_PROCESSING = {
    'tier2_model': 'anthropic/claude-3.5-sonnet',  # Tables
    'tier3_model': 'openai/gpt-4o'                 # Charts
}
```

## Output Format

Generated JSONL datasets follow TRD Section 5 schema:

```json
{
  "dataset_id": "excel-qa-12345",
  "source_info": {
    "question_id": 12345,
    "answer_id": 67890,
    "url": "https://stackoverflow.com/questions/12345",
    "license": "CC BY-SA 4.0"
  },
  "quality_metrics": {
    "score": 8.75,
    "raw_question_score": 5,
    "raw_answer_score": 12
  },
  "content": {
    "question": {
      "text": "How to create VLOOKUP formula?",
      "code_blocks": [{"language": "excel", "code": "=VLOOKUP(A1,B:C,2,FALSE)"}]
    },
    "answer": {
      "text": "Use this formula structure...",
      "code_blocks": [{"language": "vba", "code": "Sub Example()..."}]
    },
    "image_contexts": [
      {
        "source_image_url": "https://i.stack.imgur.com/abc.png",
        "processing_tier": "Tier 2 (claude-3.5-sonnet)",
        "extracted_content": "| Column A | Column B |\\n|----------|----------|"
      }
    ]
  }
}
```

## Monitoring

**Web Dashboard Features**:
- Real-time pipeline status
- Cache statistics and cleanup
- Recent dataset listing  
- Live log viewing
- One-click pipeline execution

**Directory Structure**:
```
data/
├── output/           # Generated JSONL datasets
│   └── year=2025/month=07/day=17/
├── failed/           # Dead-letter queue for errors
├── temp/             # Temporary files and caches
└── cache.db          # SQLite cache database

logs/
└── pipeline.log     # Detailed execution logs
```

## Error Handling

**Resilience Features** (TRD Section 7):
- Exponential backoff with jitter for API calls
- Dead-letter queue for failed items
- Fallback chains: AI → Table → OCR → Skip
- State checkpointing for recovery
- Circuit breaker patterns

**Failed items** saved to `data/failed/` for manual review:
- `api_error_*.json` - API failures
- `processing_error_*.json` - Processing failures  
- `image_error_*.json` - Image processing failures

## Performance

**Typical Performance** (local execution):
- Collection: ~100 Q&A pairs/minute
- Processing: ~50 items/minute (with images)
- Quality filtering: ~90% pass rate (threshold 5.0)
- Deduplication: ~15% removal rate
- Final output: ~500-800 items/hour

**Resource Usage**:
- Memory: ~2-4GB (sentence transformers)
- Storage: ~100MB/1000 items
- API costs: ~$0.05/1000 items (OpenRouter)

## Troubleshooting

**Common Issues**:

1. **Tesseract not found**: Install tesseract-ocr system package
2. **Rate limits**: Check API quotas and adjust batch sizes
3. **Memory errors**: Reduce deduplication batch size  
4. **Import errors**: Ensure all pip requirements installed

**Debug Mode**:
```bash
DEBUG=true python main.py pipeline
```

**Cache Issues**:
```bash
# Clear cache
rm data/cache.db
# Or use dashboard cleanup
```

## Development

**Adding New Processors**:
1. Implement processor interface in `processors/`
2. Add to pipeline in `pipeline/main_pipeline.py`
3. Update configuration in `config.py`

**Testing**:
```bash
pytest tests/  # Unit tests
python main.py validate  # Integration test
```

## License

Generated datasets inherit Stack Overflow's CC BY-SA 4.0 license. Pipeline code is available under MIT license.