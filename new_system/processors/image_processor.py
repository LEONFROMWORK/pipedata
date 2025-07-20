"""
Advanced Image Processing Pipeline for Excel Q&A Dataset
TRD Section 3.3b: Image-Included Path with 3-tier processing

Pipeline: httpx download → pytesseract OCR → img2table structure → OpenRouter AI enhancement
This is CRITICAL for dataset quality - failed image processing destroys learning value
"""
import asyncio
import logging
import re
import random
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import urlparse
import tempfile
import os

import httpx
import cloudscraper
import pytesseract
from PIL import Image, ImageEnhance
from img2table.document import Image as Img2TableImage
from img2table.ocr import TesseractOCR
import openai

from core.cache import APICache
from config import Config
from processors.reddit_image_bypasser import RedditImageBypasser

logger = logging.getLogger('pipeline.image_processor')

class ImageProcessingError(Exception):
    """Custom exception for image processing failures"""
    pass

class ImageProcessor:
    """
    Advanced image processor implementing TRD 3-tier pipeline:
    
    Tier 1: pytesseract OCR (fast, cost-free text extraction)
    Tier 2: img2table structure recognition (table detection/reconstruction) 
    Tier 3: OpenRouter AI enhancement (expensive, only when needed)
    """
    
    def __init__(self, cache: APICache):
        self.cache = cache
        self.config = Config.IMAGE_PROCESSING
        self.openrouter_config = self.config['openrouter_config']
        
        # Initialize cloudscraper for bypassing anti-bot protection
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        
        # Initialize Reddit image bypasser
        reddit_credentials = {
            'client_id': Config.REDDIT_CLIENT_ID,
            'client_secret': Config.REDDIT_CLIENT_SECRET,
            'username': 'ExcelQACollector'
        }
        self.reddit_bypasser = RedditImageBypasser(reddit_credentials)
        
        # Initialize OpenAI client for OpenRouter
        self.client = openai.OpenAI(
            api_key=Config.OPENROUTER_API_KEY,
            base_url="https://openrouter.ai/api/v1"
        )
        
        # Supported image formats
        self.supported_formats = set(self.config['supported_formats'])
        
        logger.info("ImageProcessor initialized with 3-tier processing pipeline")
    
    async def process_image_url(self, image_url: str, context_tags: List[str] = None) -> Dict[str, Any]:
        """
        Main image processing pipeline following TRD specifications
        
        Args:
            image_url: URL of the image to process
            context_tags: Question/answer tags for processing hints
            
        Returns:
            Dict containing processing results and metadata
        """
        context_tags = context_tags or []
        
        # Check cache first
        cached_result = self.cache.get_image_processing_result(image_url, 'full_pipeline')
        if cached_result:
            logger.info(f"Using cached image processing result for {image_url}")
            return cached_result
        
        processing_result = {
            'source_image_url': image_url,
            'processing_steps': [],
            'extracted_content_type': None,
            'extracted_content': '',
            'processing_tier': None,
            'success': False,
            'error': None
        }
        
        try:
            # Step 1: Download image using httpx
            image_path = await self._download_image(image_url)
            if not image_path:
                raise ImageProcessingError("Failed to download image")
            
            processing_result['processing_steps'].append('download_success')
            
            # Step 2: Quick OCR attempt with pytesseract
            ocr_result = await self._extract_text_with_ocr(image_path)
            processing_result['processing_steps'].append('ocr_attempted')
            
            # Step 3: Structure recognition with img2table
            table_result = await self._extract_tables_with_img2table(image_path)
            processing_result['processing_steps'].append('table_extraction_attempted')
            
            # Decision logic for AI enhancement (TRD Section 3.3b)
            needs_ai_enhancement = self._should_use_ai_enhancement(
                ocr_result, table_result, context_tags
            )
            
            if needs_ai_enhancement:
                # Step 4: OpenRouter AI enhancement
                ai_result = await self._enhance_with_openrouter(
                    image_path, image_url, ocr_result, table_result, context_tags
                )
                processing_result['processing_steps'].append('ai_enhancement_completed')
                
                # Use AI result as primary content
                processing_result.update(ai_result)
            else:
                # Use OCR/table results without AI enhancement
                if table_result['tables_found'] > 0:
                    processing_result['extracted_content_type'] = 'markdown_table'
                    processing_result['extracted_content'] = table_result['markdown_content']
                    processing_result['processing_tier'] = 'Tier 2 (img2table)'
                elif ocr_result['text_length'] > 10:
                    processing_result['extracted_content_type'] = 'plain_text'
                    processing_result['extracted_content'] = ocr_result['text']
                    processing_result['processing_tier'] = 'Tier 1 (pytesseract)'
                else:
                    processing_result['extracted_content_type'] = 'no_content'
                    processing_result['processing_tier'] = 'Failed extraction'
            
            processing_result['success'] = len(processing_result['extracted_content']) > 0
            
        except Exception as e:
            logger.error(f"Image processing failed for {image_url}: {e}")
            processing_result['error'] = str(e)
            processing_result['success'] = False
            
        finally:
            # Cleanup temporary file
            if 'image_path' in locals() and image_path and os.path.exists(image_path):
                os.unlink(image_path)
        
        # Cache the result (7 days TTL for expensive processing)
        self.cache.cache_image_processing_result(image_url, 'full_pipeline', processing_result)
        
        return processing_result
    
    async def _download_image(self, image_url: str) -> Optional[str]:
        """Download image using advanced bypass techniques for Stack Overflow and Reddit"""
        try:
            # Validate URL format
            parsed = urlparse(image_url)
            if not parsed.scheme or not parsed.netloc:
                raise ImageProcessingError(f"Invalid image URL: {image_url}")
            
            # Check file extension
            path_lower = parsed.path.lower()
            if not any(path_lower.endswith(ext) for ext in self.supported_formats):
                logger.warning(f"Unsupported image format for {image_url}")
            
            image_content = None
            download_method = "unknown"
            
            # Reddit 이미지인 경우 고급 우회 기법 사용
            if 'redd.it' in parsed.netloc or 'reddit' in parsed.netloc:
                logger.info(f"🎯 Reddit 이미지 감지 - 고급 우회 기법 적용: {image_url}")
                image_content, download_method = await self.reddit_bypasser.download_reddit_image_with_bypass(image_url)
                
            # Stack Overflow 이미지인 경우 기존 방법 사용 (이미 성공률 80%)
            elif 'sstatic.net' in parsed.netloc:
                logger.info(f"📚 Stack Overflow 이미지 - 기존 방법 사용: {image_url}")
                image_content = await self._download_stackoverflow_image(image_url)
                download_method = "stackoverflow_cloudscraper"
                
            # 기타 이미지는 기본 방법
            else:
                logger.info(f"🌐 일반 이미지 - 기본 다운로드: {image_url}")
                image_content = await self._download_generic_image(image_url)
                download_method = "generic_cloudscraper"
            
            if not image_content:
                logger.error(f"❌ 모든 다운로드 방법 실패: {image_url}")
                return None
            
            # Check content size
            content_length = len(image_content)
            if content_length > self.config['max_image_size']:
                raise ImageProcessingError(
                    f"Image too large: {content_length} bytes > {self.config['max_image_size']}"
                )
            
            # Save to temporary file
            suffix = Path(parsed.path).suffix or '.jpg'
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
                temp_file.write(image_content)
                temp_path = temp_file.name
            
            logger.info(f"✅ 이미지 다운로드 성공: {image_url} → {temp_path} ({content_length} bytes, 방법: {download_method})")
            return temp_path
            
        except Exception as e:
            logger.error(f"이미지 다운로드 실패: {image_url}: {e}")
            return None
    
    async def _download_stackoverflow_image(self, image_url: str) -> Optional[bytes]:
        """Stack Overflow 이미지 다운로드 (기존 검증된 방법)"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Referer': 'https://stackoverflow.com/',
                'Sec-Fetch-Dest': 'image',
                'Sec-Fetch-Mode': 'no-cors',
                'Sec-Fetch-Site': 'cross-site',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"'
            }
            
            # 지연 적용
            delay = random.expovariate(0.5)  # 평균 2초
            delay = min(delay, 8.0)  # 최대 8초
            delay = max(delay, 0.5)  # 최소 0.5초
            await asyncio.sleep(delay)
            
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.scraper.get(
                    image_url,
                    headers=headers,
                    timeout=self.config['download_timeout'],
                    allow_redirects=True
                )
            )
            
            response.raise_for_status()
            return response.content
            
        except Exception as e:
            logger.debug(f"Stack Overflow 이미지 다운로드 실패: {e}")
            return None
    
    async def _download_generic_image(self, image_url: str) -> Optional[bytes]:
        """일반 이미지 다운로드"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br'
            }
            
            await asyncio.sleep(random.uniform(0.5, 2.0))
            
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.scraper.get(
                    image_url,
                    headers=headers,
                    timeout=self.config['download_timeout']
                )
            )
            
            response.raise_for_status()
            return response.content
            
        except Exception as e:
            logger.debug(f"일반 이미지 다운로드 실패: {e}")
            return None
    
    async def _extract_text_with_ocr(self, image_path: str) -> Dict[str, Any]:
        """Extract text using pytesseract OCR (Tier 1)"""
        try:
            # Load and preprocess image
            with Image.open(image_path) as img:
                # Convert to RGB if necessary
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Enhance image for better OCR
                enhancer = ImageEnhance.Contrast(img)
                img = enhancer.enhance(1.5)
                
                # Apply OCR with configuration
                ocr_config = self.config['ocr_config']
                text = pytesseract.image_to_string(
                    img,
                    lang=ocr_config['lang'],
                    config=ocr_config['config']
                )
                
                # Clean up text
                cleaned_text = re.sub(r'\s+', ' ', text.strip())
                
                result = {
                    'text': cleaned_text,
                    'text_length': len(cleaned_text),
                    'word_count': len(cleaned_text.split()) if cleaned_text else 0,
                    'confidence': None  # pytesseract doesn't provide confidence easily
                }
                
                logger.info(f"OCR extracted {result['text_length']} characters, {result['word_count']} words")
                return result
                
        except Exception as e:
            logger.error(f"OCR failed for {image_path}: {e}")
            return {'text': '', 'text_length': 0, 'word_count': 0, 'confidence': 0}
    
    async def _extract_tables_with_img2table(self, image_path: str) -> Dict[str, Any]:
        """Extract table structures using img2table (Tier 2)"""
        try:
            # Initialize img2table with Tesseract OCR
            ocr = TesseractOCR(
                lang=self.config['ocr_config']['lang'],
                psm=6  # Uniform block of text
            )
            
            # Process image for table detection
            doc = Img2TableImage(src=image_path, detect_rotation=True)
            
            # Extract tables
            extracted_tables = doc.extract_tables(
                ocr=ocr,
                implicit_rows=True,
                implicit_columns=True,
                borderless_tables=True
            )
            
            markdown_tables = []
            
            if extracted_tables:
                for table in extracted_tables:
                    # Convert table to markdown format
                    markdown_table = self._table_to_markdown(table)
                    if markdown_table:
                        markdown_tables.append(markdown_table)
            
            result = {
                'tables_found': len(extracted_tables),
                'markdown_content': '\n\n'.join(markdown_tables),
                'raw_tables': extracted_tables
            }
            
            logger.info(f"Table extraction found {result['tables_found']} tables")
            return result
            
        except Exception as e:
            logger.error(f"Table extraction failed for {image_path}: {e}")
            return {'tables_found': 0, 'markdown_content': '', 'raw_tables': []}
    
    def _table_to_markdown(self, table) -> Optional[str]:
        """Convert img2table result to markdown format"""
        try:
            if not hasattr(table, 'content') or not table.content:
                return None
            
            # Get table content (list of lists)
            rows = table.content
            if not rows or len(rows) < 1:
                return None
            
            # Convert any slice objects or non-iterable objects to proper format
            processed_rows = []
            for row in rows:
                if isinstance(row, slice):
                    continue  # Skip slice objects
                
                # Ensure row is iterable and convert to list of strings
                try:
                    if hasattr(row, '__iter__') and not isinstance(row, (str, bytes)):
                        processed_row = []
                        for cell in row:
                            if isinstance(cell, slice):
                                continue  # Skip slice cells
                            processed_row.append(str(cell) if cell is not None else '')
                        if processed_row:  # Only add non-empty rows
                            processed_rows.append(processed_row)
                    else:
                        # Single cell row
                        processed_rows.append([str(row)])
                except Exception:
                    # Skip problematic rows
                    continue
            
            if not processed_rows or len(processed_rows) < 1:
                return None
            
            # Create markdown table
            markdown_lines = []
            
            # Header row
            header = '| ' + ' | '.join(str(cell).strip() for cell in processed_rows[0]) + ' |'
            markdown_lines.append(header)
            
            # Separator row
            separator = '|' + ''.join([':-------' for _ in processed_rows[0]]) + '|'
            markdown_lines.append(separator)
            
            # Data rows
            for row in processed_rows[1:]:
                data_row = '| ' + ' | '.join(str(cell).strip() for cell in row) + ' |'
                markdown_lines.append(data_row)
            
            return '\n'.join(markdown_lines)
            
        except Exception as e:
            logger.error(f"Table to markdown conversion failed: {e}")
            return None
    
    def _should_use_ai_enhancement(self, ocr_result: Dict, table_result: Dict, 
                                 context_tags: List[str]) -> bool:
        """
        Determine if AI enhancement is needed based on TRD criteria:
        - OCR/structure recognition insufficient
        - Chart/graph tags present
        """
        # Check if OCR/table extraction was successful
        has_good_ocr = ocr_result['text_length'] > 20 and ocr_result['word_count'] > 5
        has_tables = table_result['tables_found'] > 0
        
        # Check for chart/graph indicators in tags
        chart_indicators = {'chart', 'graph', 'plot', 'visualization', 'diagram'}
        has_chart_tags = any(tag.lower() in chart_indicators for tag in context_tags)
        
        # AI enhancement needed if:
        # 1. Poor OCR/table results AND not clearly just text
        # 2. Chart/graph tags present (complex visual content)
        needs_enhancement = (not has_good_ocr and not has_tables) or has_chart_tags
        
        logger.info(f"AI enhancement decision: {needs_enhancement} "
                   f"(OCR good: {has_good_ocr}, Tables: {has_tables}, Charts: {has_chart_tags})")
        
        return needs_enhancement
    
    async def _enhance_with_openrouter(self, image_path: str, image_url: str, 
                                     ocr_result: Dict, table_result: Dict,
                                     context_tags: List[str]) -> Dict[str, Any]:
        """
        Use OpenRouter AI models for complex image analysis (Tier 3)
        
        Model selection per TRD:
        - Tables/dialogs: claude-3.5-sonnet (Tier 2)
        - Charts/complex: gpt-4o (Tier 3)
        """
        # Determine appropriate model based on content type
        has_tables = table_result['tables_found'] > 0
        chart_indicators = {'chart', 'graph', 'plot', 'visualization', 'diagram'}
        has_charts = any(tag.lower() in chart_indicators for tag in context_tags)
        
        if has_tables or 'table' in ' '.join(context_tags).lower():
            model = self.openrouter_config['tier2_model']  # claude-3.5-sonnet
            prompt = self._get_table_analysis_prompt(ocr_result, table_result)
            processing_tier = "Tier 2 (claude-3.5-sonnet)"
        else:
            model = self.openrouter_config['tier3_model']  # gpt-4o
            prompt = self._get_chart_analysis_prompt(ocr_result, context_tags)
            processing_tier = "Tier 3 (gpt-4o)"
        
        try:
            # Check cache first
            cached_response = self.cache.get_openrouter_response(model, [prompt], image_url)
            if cached_response:
                logger.info(f"Using cached OpenRouter response for {image_url}")
                return cached_response
            
            # Read image as base64 for API call
            with open(image_path, 'rb') as img_file:
                import base64
                image_data = base64.b64encode(img_file.read()).decode()
                image_mime = 'image/jpeg'  # Default, could be improved
            
            # Call OpenRouter API
            messages = [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{image_mime};base64,{image_data}"
                            }
                        }
                    ]
                }
            ]
            
            response = await asyncio.to_thread(
                self.client.chat.completions.create,
                model=model,
                messages=messages,
                max_tokens=self.openrouter_config['max_tokens'],
                temperature=self.openrouter_config['temperature']
            )
            
            ai_content = response.choices[0].message.content.strip()
            
            # Determine content type from AI response
            if '|' in ai_content and '---' in ai_content:
                content_type = 'markdown_table'
            elif any(keyword in ai_content.lower() for keyword in ['chart', 'graph', 'shows', 'displays']):
                content_type = 'chart_description'
            else:
                content_type = 'enhanced_text'
            
            result = {
                'extracted_content_type': content_type,
                'extracted_content': ai_content,
                'processing_tier': processing_tier,
                'ai_model_used': model,
                'tokens_used': response.usage.total_tokens if response.usage else 0
            }
            
            # Cache the expensive AI result
            self.cache.cache_openrouter_response(model, [prompt], image_url, result)
            
            logger.info(f"AI enhancement completed with {model}, {result['tokens_used']} tokens")
            return result
            
        except Exception as e:
            logger.error(f"OpenRouter AI enhancement failed: {e}")
            # Fallback to best available result
            if table_result['tables_found'] > 0:
                return {
                    'extracted_content_type': 'markdown_table',
                    'extracted_content': table_result['markdown_content'],
                    'processing_tier': 'Tier 2 (img2table fallback)',
                    'ai_model_used': None,
                    'tokens_used': 0
                }
            else:
                return {
                    'extracted_content_type': 'plain_text',
                    'extracted_content': ocr_result['text'],
                    'processing_tier': 'Tier 1 (OCR fallback)',
                    'ai_model_used': None,
                    'tokens_used': 0
                }
    
    def _get_table_analysis_prompt(self, ocr_result: Dict, table_result: Dict) -> str:
        """Generate prompt for table analysis with claude-3.5-sonnet"""
        return f"""
You are analyzing an Excel-related screenshot that contains tabular data. Your task is to extract and reconstruct the table structure in clean markdown format.

Context from previous processing:
- OCR extracted text: {ocr_result['text'][:200]}...
- Tables detected: {table_result['tables_found']}

Please:
1. Identify all tables/data structures in the image
2. Extract the data accurately, preserving relationships
3. Format as clean markdown tables with proper headers
4. Include any formulas or cell references you can see
5. Note any Excel-specific elements (formulas, formatting, etc.)

Provide only the markdown table(s), no explanatory text.
"""
    
    def _get_chart_analysis_prompt(self, ocr_result: Dict, context_tags: List[str]) -> str:
        """Generate prompt for chart/complex image analysis with gpt-4o"""
        tags_context = ', '.join(context_tags) if context_tags else 'none'
        
        return f"""
You are analyzing an Excel-related screenshot that may contain charts, graphs, or complex visual elements.

Context:
- Question tags: {tags_context}
- OCR text found: {ocr_result['text'][:200]}...

Please analyze this image and provide:
1. Type of chart/visualization (if any)
2. Key data points, trends, or patterns shown
3. Any Excel formulas, functions, or settings visible
4. Step-by-step explanation of what the image demonstrates
5. How this relates to Excel functionality

Be specific about Excel features and provide actionable insights for learning.
"""