"""
Main Entry Point for Excel Q&A Dataset Pipeline
TRD-compliant pipeline with local optimization
"""
import asyncio
import logging.config
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add the new_system directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from pipeline.main_pipeline import ExcelQAPipeline
from config import Config

def setup_logging():
    """Setup logging configuration"""
    logging_config = Config.get_logging_config()
    logging.config.dictConfig(logging_config)
    
    logger = logging.getLogger('pipeline.main')
    logger.info("Excel Q&A Dataset Pipeline starting")
    return logger

async def run_full_pipeline(args):
    """Run the complete pipeline"""
    logger = logging.getLogger('pipeline.main')
    
    # Determine from_date
    if args.incremental:
        from_date = datetime.now() - timedelta(hours=args.hours_back)
        logger.info(f"Running incremental collection from {from_date}")
    else:
        from_date = None
        logger.info("Running full collection")
    
    # Parse sources
    sources = [s.strip() for s in args.sources.split(',') if s.strip()]
    logger.info(f"Collection sources: {sources}")
    
    # Initialize and run pipeline
    pipeline = ExcelQAPipeline()
    
    try:
        result = await pipeline.run_full_pipeline(
            from_date=from_date,
            max_pages=args.max_pages,
            target_count=args.target_count,
            sources=sources
        )
        
        # Display results
        print("\\n" + "="*60)
        print("PIPELINE EXECUTION COMPLETE")
        print("="*60)
        
        print(f"Status: {result['execution_summary']['status']}")
        print(f"Execution Time: {result['execution_summary']['execution_time_seconds']:.1f} seconds")
        print(f"Current Stage: {result['execution_summary']['current_stage']}")
        
        print("\\nData Flow:")
        flow = result['data_flow']
        print(f"  Collected: {flow['collected']}")
        print(f"  Processed: {flow['processed']}")
        print(f"  Quality Filtered: {flow['quality_filtered']}")
        print(f"  Deduplicated: {flow['deduplicated']}")
        print(f"  Final Output: {flow['final_output']}")
        
        print("\\nEfficiency Metrics:")
        metrics = result['efficiency_metrics']
        print(f"  Collection to Final Ratio: {metrics['collection_to_final_ratio']:.1f}%")
        print(f"  Processing Success Rate: {metrics['processing_success_rate']:.1f}%")
        print(f"  Quality Pass Rate: {metrics['quality_pass_rate']:.1f}%")
        
        if 'dataset_path' in result:
            print(f"\\nDataset Generated: {result['dataset_path']}")
        
        if result['errors']:
            print(f"\\nErrors Encountered: {len(result['errors'])}")
            for error in result['errors'][:3]:  # Show first 3 errors
                print(f"  - {error}")
        
        return result['execution_summary']['status'] == 'completed'
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        print(f"\\nPipeline Failed: {e}")
        return False

async def run_web_dashboard():
    """Run the web dashboard"""
    logger = logging.getLogger('pipeline.main')
    logger.info("Starting web dashboard")
    
    try:
        from web.dashboard import create_app
        import uvicorn
        
        app = create_app()
        
        print("\\n" + "="*50)
        print("EXCEL Q&A PIPELINE DASHBOARD")
        print("="*50)
        print(f"Dashboard URL: http://{Config.WEB_CONFIG['host']}:{Config.WEB_CONFIG['port']}")
        print("Press Ctrl+C to stop")
        print("="*50)
        
        config = uvicorn.Config(
            app,
            host=Config.WEB_CONFIG['host'],
            port=Config.WEB_CONFIG['port'],
            log_level="info"
        )
        server = uvicorn.Server(config)
        await server.serve()
        
    except ImportError:
        print("Web dashboard dependencies not available. Install FastAPI and uvicorn:")
        print("pip install fastapi uvicorn jinja2")
        return False
    except Exception as e:
        logger.error(f"Dashboard failed: {e}")
        print(f"Dashboard Failed: {e}")
        return False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Excel Q&A Dataset Pipeline - TRD Compliant Implementation"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Pipeline command
    pipeline_parser = subparsers.add_parser('pipeline', help='Run the data collection pipeline')
    pipeline_parser.add_argument(
        '--incremental', 
        action='store_true',
        help='Run incremental collection (last N hours)'
    )
    pipeline_parser.add_argument(
        '--hours-back',
        type=int,
        default=24,
        help='Hours to look back for incremental collection (default: 24)'
    )
    pipeline_parser.add_argument(
        '--max-pages',
        type=int,
        default=50,
        help='Maximum pages to collect from API (default: 50)'
    )
    pipeline_parser.add_argument(
        '--target-count',
        type=int,
        default=1000,
        help='Target number of final items (default: 1000)'
    )
    pipeline_parser.add_argument(
        '--sources',
        type=str,
        default='stackoverflow,reddit',
        help='Comma-separated list of sources to collect from (default: stackoverflow,reddit)'
    )
    
    # Dashboard command
    dashboard_parser = subparsers.add_parser('dashboard', help='Run the web dashboard')
    
    # Validation command
    validate_parser = subparsers.add_parser('validate', help='Validate environment and configuration')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Setup logging
    logger = setup_logging()
    
    # Validate environment
    try:
        Config.validate_environment()
        logger.info("Environment validation passed")
    except ValueError as e:
        print(f"Environment validation failed: {e}")
        print("Please check your .env file and ensure all required API keys are set.")
        return
    
    # Execute command
    if args.command == 'pipeline':
        success = asyncio.run(run_full_pipeline(args))
        sys.exit(0 if success else 1)
        
    elif args.command == 'dashboard':
        asyncio.run(run_web_dashboard())
        
    elif args.command == 'validate':
        print("Environment Validation:")
        print("✓ Configuration loaded successfully")
        print("✓ Required API keys present")
        print("✓ Directory structure created")
        
        # Test cache
        try:
            from core.cache import LocalCache
            cache = LocalCache(Config.DATABASE_PATH)
            cache.set('test_key', 'test_value')
            assert cache.get('test_key') == 'test_value'
            print("✓ SQLite cache working")
        except Exception as e:
            print(f"✗ Cache test failed: {e}")
        
        print("\\nValidation complete!")

if __name__ == "__main__":
    main()