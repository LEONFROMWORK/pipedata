"""
Configuration management for Excel Q&A Dataset Pipeline
Based on PRD/TRD specifications with local optimization
"""
import os
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Main configuration class following TRD specifications"""
    
    # API Configuration
    STACKOVERFLOW_API_KEY = os.getenv('STACKOVERFLOW_API_KEY', '')
    OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY', '')
    
    # Reddit API Configuration
    REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID', '')
    REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET', '')
    REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'ExcelQA-Collector/1.0')
    
    # Local optimization: SQLite instead of Redis
    DATABASE_PATH = Path(os.getenv('DATABASE_PATH', './data/cache.db'))
    
    # Directory structure
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / 'data'
    OUTPUT_DIR = DATA_DIR / 'output'
    TEMP_DIR = DATA_DIR / 'temp'
    LOGS_DIR = BASE_DIR / 'logs'
    
    # Stack Overflow API Configuration (TRD Section 2)
    SO_API_CONFIG = {
        'base_url': 'https://api.stackexchange.com/2.3',
        'site': 'stackoverflow',
        'custom_filter': {
            'question_fields': [
                'question_id', 'is_answered', 'accepted_answer_id', 
                'score', 'view_count', 'favorite_count', 'tags',
                'owner.reputation', 'title', 'body_markdown'
            ],
            'answer_fields': [
                'answer_id', 'score', 'owner.reputation', 'body_markdown'
            ]
        },
        'tags': ['excel-formula'],  # Focus on Excel formula questions as requested
        'min_score': 3,
        'cache_ttl': 86400  # 24 hours as per TRD
    }
    
    # Image Processing Pipeline Configuration (TRD Section 3.3b)
    IMAGE_PROCESSING = {
        'download_timeout': 30,
        'max_image_size': 10 * 1024 * 1024,  # 10MB
        'supported_formats': ['.png', '.jpg', '.jpeg', '.gif', '.bmp'],
        'ocr_config': {
            'lang': 'eng',
            'config': '--psm 6'  # Uniform block of text
        },
        'openrouter_config': {
            'tier2_model': 'anthropic/claude-3.5-sonnet',  # Tables/dialogs
            'tier3_model': 'openai/gpt-4o',  # Charts/complex images
            'max_tokens': 1000,
            'temperature': 0.1
        }
    }
    
    @classmethod
    def get_current_model(cls) -> str:
        """Get currently selected model from dashboard settings"""
        try:
            from config.model_settings import get_current_model
            return get_current_model()
        except Exception:
            return 'anthropic/claude-3.5-sonnet'  # Default fallback
    
    @classmethod
    def get_openrouter_config(cls) -> dict:
        """Get OpenRouter configuration for current model"""
        try:
            from config.model_settings import get_openrouter_config
            return get_openrouter_config()
        except Exception:
            return {
                'model': 'anthropic/claude-3.5-sonnet',
                'temperature': 0.1,
                'max_tokens': 1000,
                'top_p': 0.9,
                'stream': False
            }
    
    # Quality Scoring Algorithm (TRD Section 4)
    QUALITY_SCORING = {
        'weights': {
            'w_q': 0.4,  # Question weight
            'w_a': 0.5,  # Answer weight  
            'w_c': 0.1   # Completion bonus weight
        },
        'completion_bonus': {
            'base': 1,
            'code_blocks': 2,
            'image_context': 3
        },
        'threshold': 5.0,  # Minimum quality score to keep
        'normalization': 'min_max'  # Within batch normalization
    }
    
    # Deduplication Configuration (TRD Section 3.5)
    DEDUPLICATION = {
        'model': 'all-MiniLM-L6-v2',  # sentence-transformers model
        'similarity_threshold': 0.95,  # Cosine similarity threshold
        'batch_size': 32
    }
    
    # Rate Limiting and Error Handling (TRD Section 7)
    RATE_LIMITING = {
        'max_requests_per_day': 10000,
        'requests_per_minute': 30,
        'backoff_config': {
            'max_tries': 5,
            'max_time': 300,  # 5 minutes
            'expo': True,
            'jitter': True
        }
    }
    
    # Reddit API Configuration (TRD)
    REDDIT_CONFIG = {
        'subreddit': 'excel',
        'target_flairs': ['solved'],  # Only collect solved posts as requested
        'question_keywords': ['?', 'how to', 'issue', 'error', 'help'],
        'upvote_ratio_threshold': 0.7,
        'solution_score_threshold': 1,  # Lowered from 5 to 1 for testing
        'confirmation_keywords': [
            'Solution Verified', 'Thanks', 'This worked', 'This works', 'Solved', 'That was it', 'Perfect',
            'Thank you', 'Awesome', 'Great', 'Exactly', 'Fixed it', 'Perfect!', 'Brilliant',
            'That did it', 'That worked', 'You saved me', 'Life saver', 'Genius', 'Spot on',
            'Exactly what I needed', 'This is it', 'Problem solved', 'Works perfectly'
        ],
        'cache_ttl': 86400  # 24 hours
    }
    
    # Reddit Quality Scoring (Different from Stack Overflow)
    REDDIT_QUALITY_SCORING = {
        'weights': {
            'w_s': 0.3,  # Submission weight
            'w_a': 0.4,  # Answer weight  
            'w_b': 0.3   # Bonus weight (higher than SO)
        },
        'bonus_scores': {
            'op_confirmed': 10,     # OP directly confirmed
            'solved_flair': 5,      # Flair changed to "Solved"
            'code_blocks': 3,       # Contains code
            'image_context': 2      # Image processing success
        },
        'threshold': 1.0,           # Temporarily lowered for Reddit text extraction testing
        'normalization': 'min_max'
    }

    # Final Dataset Configuration (TRD Section 5)
    DATASET_CONFIG = {
        'format': 'jsonl',
        'target_daily_count': 1000,
        'license': 'CC BY-SA 4.0',
        'partition_format': 'year={year}/month={month:02d}/day={day:02d}'
    }
    
    # Web Dashboard Configuration (Local optimization)
    WEB_CONFIG = {
        'host': '127.0.0.1',
        'port': 8000,
        'debug': os.getenv('DEBUG', 'false').lower() == 'true',
        'auto_reload': True
    }
    
    @classmethod
    def ensure_directories(cls) -> None:
        """Ensure all required directories exist"""
        directories = [
            cls.DATA_DIR,
            cls.OUTPUT_DIR, 
            cls.TEMP_DIR,
            cls.LOGS_DIR
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def validate_environment(cls) -> bool:
        """Validate required environment variables"""
        required_vars = [
            'STACKOVERFLOW_API_KEY',
            'OPENROUTER_API_KEY',
            'REDDIT_CLIENT_ID',
            'REDDIT_CLIENT_SECRET'
        ]
        
        missing = [var for var in required_vars if not os.getenv(var)]
        
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        return True
    
    @classmethod 
    def get_logging_config(cls) -> Dict[str, Any]:
        """Get logging configuration"""
        return {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'detailed': {
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                },
                'simple': {
                    'format': '%(levelname)s - %(message)s'
                }
            },
            'handlers': {
                'file': {
                    'class': 'logging.FileHandler',
                    'filename': cls.LOGS_DIR / 'pipeline.log',
                    'formatter': 'detailed',
                    'level': 'INFO'
                },
                'console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'simple',
                    'level': 'INFO'
                }
            },
            'loggers': {
                'pipeline': {
                    'handlers': ['file', 'console'],
                    'level': 'INFO',
                    'propagate': False
                }
            }
        }

# Initialize configuration on import
Config.ensure_directories()