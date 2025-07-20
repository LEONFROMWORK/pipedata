#!/usr/bin/env python3
"""
Phase 1 403 Bypass Test Script
Tests cloudscraper-based image download success rates
"""
import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from processors.image_processor import ImageProcessor
from core.cache import APICache, LocalCache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def test_403_bypass():
    """Test Phase 1 403 bypass implementation"""
    
    # Initialize cache with proper parameters
    local_cache = LocalCache(db_path=Path("/tmp/test_cache.db"))
    cache = APICache(local_cache)
    
    # Initialize image processor
    processor = ImageProcessor(cache)
    
    # Test URLs that previously failed with 403
    test_urls = [
        # Stack Overflow images (i.sstatic.net)
        "https://i.sstatic.net/AJr6APk8.png",
        "https://i.sstatic.net/TpgieF6J.png", 
        "https://i.sstatic.net/82NP1kgT.png",
        "https://i.sstatic.net/UmaKMnME.jpg",
        
        # Reddit images (preview.redd.it)
        "https://preview.redd.it/76mukstfxhdf1.png"
    ]
    
    success_count = 0
    total_count = len(test_urls)
    
    logger.info(f"🧪 Testing Phase 1 403 bypass on {total_count} URLs")
    logger.info("=" * 60)
    
    for i, url in enumerate(test_urls, 1):
        logger.info(f"[{i}/{total_count}] Testing: {url}")
        
        try:
            # Test just the download method directly
            image_path = await processor._download_image(url)
            
            if image_path:
                success_count += 1
                logger.info(f"✅ SUCCESS: Downloaded to {image_path}")
                
                # Cleanup
                import os
                if os.path.exists(image_path):
                    os.unlink(image_path)
            else:
                logger.error(f"❌ FAILED: No image downloaded")
                
        except Exception as e:
            logger.error(f"❌ ERROR: {e}")
        
        logger.info("-" * 40)
    
    # Results summary
    success_rate = (success_count / total_count) * 100
    logger.info("=" * 60)
    logger.info(f"📊 PHASE 1 TEST RESULTS:")
    logger.info(f"   • Successful downloads: {success_count}/{total_count}")
    logger.info(f"   • Success rate: {success_rate:.1f}%")
    
    if success_rate >= 80:
        logger.info("🎉 Phase 1 cloudscraper solution is EFFECTIVE!")
        logger.info("   No need for Phase 2 (NoDriver) implementation")
    elif success_rate >= 50:
        logger.info("⚠️  Phase 1 partially effective - consider Phase 2")
    else:
        logger.error("💥 Phase 1 insufficient - Phase 2 (NoDriver) required")
    
    return success_rate

if __name__ == "__main__":
    asyncio.run(test_403_bypass())