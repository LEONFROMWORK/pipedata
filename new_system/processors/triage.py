"""
Triage System for Text/Image Processing Path Decision
TRD Section 3.2: body_markdown 내 <img> 태그 존재 여부로 처리 경로 분기
"""
import re
import logging
from typing import Dict, Any, List, Tuple, Optional, Union
from dataclasses import dataclass
from urllib.parse import urlparse

logger = logging.getLogger('pipeline.triage')

@dataclass
class TriageResult:
    """Triage decision result"""
    processing_path: str  # 'text_only' or 'image_included'
    image_urls: List[str]  # Extracted image URLs
    text_content: str  # Cleaned text content
    has_code_blocks: bool  # Contains code examples
    estimated_complexity: str  # 'simple', 'moderate', 'complex'
    
class ContentTriageSystem:
    """
    Intelligent triage system for routing Q&A content to appropriate processors
    
    Key decisions per TRD:
    - Text-Only Path (3a): No images, pure text processing
    - Image-Included Path (3b): Contains images, needs full pipeline
    """
    
    def __init__(self):
        # Image URL patterns for comprehensive detection
        self.image_patterns = [
            r'<img[^>]*src=["\']([^"\']*)["\'][^>]*>',  # Standard img tags
            r'!\[.*?\]\(([^)]*\.(png|jpg|jpeg|gif|bmp|svg))\)',  # Markdown images
            r'https?://[^\s]*\.(png|jpg|jpeg|gif|bmp|svg)',  # Direct image URLs
        ]
        
        # Code block patterns
        self.code_patterns = [
            r'<pre[^>]*>.*?</pre>',  # HTML pre blocks
            r'<code[^>]*>.*?</code>',  # HTML code blocks  
            r'```[\s\S]*?```',  # Markdown code blocks
            r'`[^`]+`',  # Inline code
        ]
        
        # Excel-specific indicators for complexity assessment
        self.excel_complexity_indicators = {
            'simple': ['sum', 'average', 'count', 'basic', 'simple'],
            'moderate': ['vlookup', 'hlookup', 'pivot', 'chart', 'formula', 'macro'],
            'complex': ['vba', 'array formula', 'dynamic array', 'power query', 'power pivot', 'custom function']
        }
        
        logger.info("ContentTriageSystem initialized")
    
    def triage_content(self, question: Dict[str, Any], answer: Dict[str, Any] = None) -> TriageResult:
        """
        Main triage method to determine processing path
        
        Args:
            question: Question data from Stack Overflow API
            answer: Answer data from Stack Overflow API
            
        Returns:
            TriageResult with processing decision and metadata
        """
        # Combine question and answer content for analysis
        question_content = question.get('body_markdown', '') or question.get('body', '')
        answer_content = ''
        
        if answer:
            answer_content = answer.get('body_markdown', '') or answer.get('body', '')
        
        combined_content = f"{question_content}\n\n{answer_content}"
        
        # Extract image URLs using multiple patterns
        # First check if Reddit data already has extracted image URLs
        reddit_image_urls = question.get('image_urls', [])
        text_image_urls = self._extract_image_urls(combined_content)
        
        # Check for direct image arrays (Oppadu style)
        direct_image_urls = []
        if question.get('images'):
            direct_image_urls.extend(question['images'])
        if answer and answer.get('images'):
            direct_image_urls.extend(answer['images'])
        
        # Combine and deduplicate URLs, prioritizing direct images > Reddit API > text extraction
        all_image_urls = direct_image_urls + reddit_image_urls + text_image_urls
        image_urls = list(dict.fromkeys(all_image_urls))  # Remove duplicates while preserving order
        
        if direct_image_urls:
            logger.info(f"Using {len(direct_image_urls)} direct image URLs (Oppadu style)")
        elif reddit_image_urls:
            logger.info(f"Using {len(reddit_image_urls)} Reddit API extracted image URLs")
        
        # Clean text content (remove HTML/markdown artifacts)
        cleaned_text = self._clean_text_content(combined_content)
        
        # Detect code blocks
        has_code_blocks = self._detect_code_blocks(combined_content)
        
        # Assess complexity for processing optimization
        complexity = self._assess_complexity(cleaned_text, question.get('tags', []))
        
        # Make triage decision
        if image_urls:
            processing_path = 'image_included'
            logger.info(f"Triaged to IMAGE path: {len(image_urls)} images found")
        else:
            processing_path = 'text_only'
            logger.info("Triaged to TEXT-ONLY path: no images detected")
        
        return TriageResult(
            processing_path=processing_path,
            image_urls=image_urls,
            text_content=cleaned_text,
            has_code_blocks=has_code_blocks,
            estimated_complexity=complexity
        )
    
    def _extract_image_urls(self, content: str) -> List[str]:
        """Extract all image URLs from content using multiple patterns"""
        image_urls = []
        
        for pattern in self.image_patterns:
            matches = re.finditer(pattern, content, re.IGNORECASE | re.DOTALL)
            for match in matches:
                if pattern.startswith('<img'):
                    # Extract src attribute
                    url = match.group(1)
                elif pattern.startswith('!'):
                    # Markdown image
                    url = match.group(1)
                else:
                    # Direct URL
                    url = match.group(0)
                
                # Validate and clean URL
                cleaned_url = self._validate_image_url(url)
                if cleaned_url and cleaned_url not in image_urls:
                    image_urls.append(cleaned_url)
        
        logger.debug(f"Extracted {len(image_urls)} unique image URLs")
        return image_urls
    
    def _validate_image_url(self, url: str) -> Optional[str]:
        """Validate and clean image URL"""
        if not url or len(url) < 10:
            return None
        
        # Remove common prefixes/suffixes
        url = url.strip().strip('"\'')
        
        try:
            parsed = urlparse(url)
            if not parsed.scheme:
                # Relative URL, add https
                if url.startswith('//'):
                    url = f"https:{url}"
                elif url.startswith('/'):
                    # Skip relative paths without domain
                    return None
                else:
                    url = f"https://{url}"
            
            # Re-parse cleaned URL
            parsed = urlparse(url)
            if not parsed.netloc:
                return None
            
            # Check if it's actually an image
            path_lower = parsed.path.lower()
            image_extensions = {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg', '.webp'}
            
            if any(path_lower.endswith(ext) for ext in image_extensions):
                return url
            
            # Some Stack Overflow images don't have extensions
            if 'imgur.com' in parsed.netloc or 'stack.imgur.com' in parsed.netloc:
                return url
            
            return None
            
        except Exception as e:
            logger.debug(f"URL validation failed for {url}: {e}")
            return None
    
    def _clean_text_content(self, content: str) -> str:
        """Clean text content by removing HTML/markdown and normalizing"""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', content)
        
        # Remove markdown image syntax
        text = re.sub(r'!\[.*?\]\([^)]*\)', ' ', text)
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove URLs (but keep the context)
        text = re.sub(r'https?://[^\s]+', '[URL]', text)
        
        return text.strip()
    
    def _detect_code_blocks(self, content: str) -> bool:
        """Detect presence of code blocks using multiple patterns"""
        for pattern in self.code_patterns:
            if re.search(pattern, content, re.IGNORECASE | re.DOTALL):
                return True
        return False
    
    def _assess_complexity(self, text: str, tags: List[str]) -> str:
        """
        Assess content complexity for processing optimization
        
        Returns:
            'simple', 'moderate', or 'complex'
        """
        text_lower = text.lower()
        tags_lower = [tag.lower() for tag in tags]
        
        # Check for complexity indicators
        complex_score = 0
        moderate_score = 0
        simple_score = 0
        
        # Analyze tags first (strong indicators)
        for tag in tags_lower:
            if any(indicator in tag for indicator in self.excel_complexity_indicators['complex']):
                complex_score += 3
            elif any(indicator in tag for indicator in self.excel_complexity_indicators['moderate']):
                moderate_score += 2
            elif any(indicator in tag for indicator in self.excel_complexity_indicators['simple']):
                simple_score += 1
        
        # Analyze content
        for indicators, score_boost in [
            (self.excel_complexity_indicators['complex'], 2),
            (self.excel_complexity_indicators['moderate'], 1),
            (self.excel_complexity_indicators['simple'], 0.5)
        ]:
            for indicator in indicators:
                if indicator in text_lower:
                    if score_boost == 2:
                        complex_score += 1
                    elif score_boost == 1:
                        moderate_score += 1
                    else:
                        simple_score += 1
        
        # Length-based complexity (longer content often more complex)
        if len(text) > 2000:
            complex_score += 1
        elif len(text) > 500:
            moderate_score += 1
        
        # Make decision based on scores
        if complex_score >= 2:
            return 'complex'
        elif moderate_score >= 2 or complex_score >= 1:
            return 'moderate'
        else:
            return 'simple'
    
    def get_processing_recommendations(self, triage_result: TriageResult) -> Dict[str, Any]:
        """Generate processing recommendations based on triage result"""
        recommendations = {
            'priority': 'normal',
            'timeout_multiplier': 1.0,
            'image_processing_tier': 'standard',
            'parallel_processing': False
        }
        
        # Adjust based on complexity
        if triage_result.estimated_complexity == 'complex':
            recommendations['priority'] = 'high'
            recommendations['timeout_multiplier'] = 2.0
            recommendations['image_processing_tier'] = 'enhanced'
        elif triage_result.estimated_complexity == 'simple':
            recommendations['priority'] = 'low'
            recommendations['timeout_multiplier'] = 0.5
        
        # Image processing recommendations
        if triage_result.processing_path == 'image_included':
            if len(triage_result.image_urls) > 3:
                recommendations['parallel_processing'] = True
            if len(triage_result.image_urls) > 1:
                recommendations['timeout_multiplier'] *= 1.5
        
        return recommendations
    
    def get_triage_stats(self, results: List[TriageResult]) -> Dict[str, Any]:
        """Calculate statistics from multiple triage results"""
        if not results:
            return {}
        
        total = len(results)
        text_only = sum(1 for r in results if r.processing_path == 'text_only')
        image_included = total - text_only
        
        complexity_dist = {}
        for complexity in ['simple', 'moderate', 'complex']:
            complexity_dist[complexity] = sum(1 for r in results if r.estimated_complexity == complexity)
        
        total_images = sum(len(r.image_urls) for r in results)
        has_code = sum(1 for r in results if r.has_code_blocks)
        
        return {
            'total_items': total,
            'text_only_path': text_only,
            'image_included_path': image_included,
            'text_only_percentage': round((text_only / total) * 100, 1),
            'image_included_percentage': round((image_included / total) * 100, 1),
            'complexity_distribution': complexity_dist,
            'total_images_found': total_images,
            'average_images_per_item': round(total_images / total, 2) if total > 0 else 0,
            'items_with_code_blocks': has_code,
            'code_blocks_percentage': round((has_code / total) * 100, 1)
        }