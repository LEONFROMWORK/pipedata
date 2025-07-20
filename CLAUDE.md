# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is the Excel Q&A Dataset Pipeline - a sophisticated data collection and processing system that generates high-quality Q&A datasets from Stack Overflow, Reddit, and Oppadu for AI training purposes.

## Key Commands

### Backend (Python Pipeline)

```bash
# Run full pipeline
cd new_system && python main.py pipeline --max-pages 50 --target-count 1000

# Run incremental collection (last 24 hours)
cd new_system && python main.py pipeline --incremental --hours-back 24

# Validate environment setup
cd new_system && python main.py validate

# Run web dashboard
cd new_system && python main.py dashboard

# Run API server
cd new_system && python api_server.py

# Run tests
cd new_system && python -m pytest test_*.py -v

# Run a specific test
cd new_system && python test_<name>.py
```

### Frontend (Next.js Dashboard)

```bash
# Development
cd dashboard-ui && npm run dev

# Build for production
cd dashboard-ui && npm run build

# Run production build
cd dashboard-ui && npm run start

# Lint
cd dashboard-ui && npm run lint
```

## Architecture Overview

### Backend Pipeline Architecture

The system follows a modular pipeline architecture with these key components:

1. **Data Collectors** (`/new_system/collectors/`)
   - Stack Overflow collector with custom filters
   - Reddit collector via PRAW API
   - Oppadu forum collector
   - Each collector implements bot detection and rate limiting

2. **Processing Pipeline** (`/new_system/pipeline/`)
   - **Triage System**: Routes questions based on content type (text vs. images)
   - **Text Processor**: Direct text extraction and formatting
   - **Image Processor**: 3-tier OCR system:
     - Tier 1: pytesseract OCR
     - Tier 2: img2table for structured data
     - Tier 3: AI enhancement via OpenRouter

3. **Quality & Deduplication** (`/new_system/processors/`)
   - Quality scorer using weighted algorithm (w_q=0.4, w_a=0.5, w_c=0.1)
   - Semantic deduplication with sentence-transformers (0.95 similarity threshold)
   - Bot detection across all sources

4. **Storage** (`/new_system/storage/`)
   - SQLite for caching and metadata
   - File-based storage for processed data
   - JSONL output format for AI training

### Frontend Architecture

Next.js 15 app with:
- App Router pattern
- TanStack Query for data fetching
- Radix UI components with Tailwind CSS
- NextAuth.js for authentication

## Important Configuration

### Environment Variables

Backend (`.env`):
- `STACK_OVERFLOW_API_KEY` - Required for Stack Overflow API
- `OPENROUTER_API_KEY` - Required for AI image enhancement
- `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET` - For Reddit API
- `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` - Optional AI providers

Frontend (`dashboard-ui/.env.local`):
- `NEXTAUTH_SECRET` - Authentication secret
- `NEXTAUTH_URL` - Base URL for auth callbacks
- `API_BASE_URL` - Backend API endpoint

### Database Schema

The system uses SQLite with tables for:
- `questions` - Main Q&A storage
- `processing_cache` - Image processing cache
- `quality_scores` - Scoring metadata
- `deduplication_index` - Similarity tracking

## Development Guidelines

### Testing

- All test files are in `/new_system/test_*.py`
- Tests cover individual components and full pipeline integration
- Run tests before committing changes to ensure pipeline integrity

### API Endpoints

The Flask API server (`api_server.py`) provides:
- `/api/pipeline/status` - Pipeline run status
- `/api/pipeline/start` - Trigger pipeline run
- `/api/data/stats` - Collection statistics
- `/api/data/export` - Export processed data

### Deployment

The system is configured for Railway deployment:
- `railway.toml` - Deployment configuration
- `Procfile` - Process definitions
- Standalone Next.js build for optimized deployment

## Critical Implementation Details

1. **Bot Detection**: Multi-layer bot detection system checking usernames, posting patterns, and content similarity
2. **Rate Limiting**: Implemented across all data sources to respect API limits
3. **Error Handling**: Comprehensive error handling with fallback mechanisms for image processing
4. **Caching**: SQLite-based caching to avoid reprocessing images and API calls
5. **Quality Thresholds**: Minimum quality score of 5.0 for dataset inclusion