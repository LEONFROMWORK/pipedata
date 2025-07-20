"""
Text Processor for Text-Only Path
TRD Section 3.3a: BeautifulSoup HTML 파싱, 텍스트와 코드 블록 분리
"""
import re
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import html

from bs4 import BeautifulSoup
import markdown

logger = logging.getLogger('pipeline.text_processor')

@dataclass 
class CodeBlock:
    """Structured code block representation"""
    language: str
    code: str
    block_type: str  # 'inline', 'block', 'pre'
    context: str  # Surrounding text context

@dataclass
class TextProcessingResult:
    """Result of text processing"""
    cleaned_text: str
    code_blocks: List[CodeBlock]
    has_formulas: bool
    has_vba: bool
    excel_functions: List[str]
    word_count: int
    processing_success: bool
    error: Optional[str] = None

class TextProcessor:
    """
    Text-only path processor implementing TRD Section 3.3a
    
    Capabilities:
    - HTML parsing with BeautifulSoup
    - Code block extraction and categorization
    - Excel formula detection and extraction
    - Text cleaning and normalization
    """
    
    def __init__(self):
        # Excel function patterns (comprehensive list)
        self.excel_functions = {
            'lookup': ['vlookup', 'hlookup', 'xlookup', 'lookup', 'index', 'match'],
            'math': ['sum', 'average', 'count', 'max', 'min', 'round', 'abs', 'sqrt'],
            'text': ['concatenate', 'left', 'right', 'mid', 'len', 'trim', 'upper', 'lower'],
            'date': ['today', 'now', 'date', 'year', 'month', 'day', 'weekday'],
            'logical': ['if', 'and', 'or', 'not', 'iferror', 'ifna', 'ifs'],
            'advanced': ['sumif', 'sumifs', 'countif', 'countifs', 'averageif', 'averageifs']
        }
        
        # VBA keywords for detection
        self.vba_keywords = [
            'sub', 'function', 'dim', 'as', 'integer', 'string', 'double', 'boolean',
            'if', 'then', 'else', 'elseif', 'end if', 'for', 'next', 'do', 'loop',
            'while', 'wend', 'select case', 'msgbox', 'inputbox', 'range', 'cells',
            'worksheets', 'workbooks', 'application'
        ]
        
        # Code block patterns
        self.code_patterns = {
            'pre_block': r'<pre[^>]*>(.*?)</pre>',
            'code_block': r'<code[^>]*>(.*?)</code>',
            'markdown_block': r'```(\w*)\n(.*?)\n```',
            'inline_code': r'`([^`]+)`'
        }
        
        logger.info("TextProcessor initialized with Excel-specific processing")
    
    def process_text_content(self, question_content: str, answer_content: str = "") -> TextProcessingResult:
        """
        Main text processing method following TRD specifications
        
        Args:
            question_content: Question body_markdown from Stack Overflow
            answer_content: Answer body_markdown from Stack Overflow
            
        Returns:
            TextProcessingResult with processed content and metadata
        """
        try:
            # Combine content for processing
            combined_content = f"{question_content}\n\n{answer_content}"
            
            # Step 1: Parse HTML with BeautifulSoup (TRD requirement)
            soup = BeautifulSoup(combined_content, 'html.parser')
            
            # Step 2: Extract code blocks before text cleaning
            # 오빠두 데이터의 경우 정리된 코드 블록 사용
            if hasattr(question_content, 'get') and question_content.get('excel_formulas'):
                code_blocks = question_content.get('excel_formulas', [])
                if hasattr(answer_content, 'get') and answer_content.get('excel_formulas'):
                    code_blocks.extend(answer_content.get('excel_formulas', []))
            else:
                code_blocks = self._extract_code_blocks(soup, combined_content)
            
            # Step 3: Clean text content
            cleaned_text = self._clean_html_content(soup)
            
            # Step 4: Excel-specific analysis
            has_formulas, excel_functions = self._detect_excel_formulas(cleaned_text, code_blocks)
            has_vba = self._detect_vba_code(code_blocks)
            
            # Step 5: Calculate metrics
            word_count = len(cleaned_text.split())
            
            result = TextProcessingResult(
                cleaned_text=cleaned_text,
                code_blocks=code_blocks,
                has_formulas=has_formulas,
                has_vba=has_vba,
                excel_functions=excel_functions,
                word_count=word_count,
                processing_success=True
            )
            
            logger.info(f"Text processing successful: {word_count} words, {len(code_blocks)} code blocks")
            return result
            
        except Exception as e:
            logger.error(f"Text processing failed: {e}")
            return TextProcessingResult(
                cleaned_text="",
                code_blocks=[],
                has_formulas=False,
                has_vba=False,
                excel_functions=[],
                word_count=0,
                processing_success=False,
                error=str(e)
            )
    
    def _extract_code_blocks(self, soup: BeautifulSoup, raw_content: str) -> List[CodeBlock]:
        """Extract and categorize code blocks from content"""
        code_blocks = []
        
        # Extract HTML pre/code blocks
        for tag_name, pattern in [('pre', 'pre_block'), ('code', 'code_block')]:
            for tag in soup.find_all(tag_name):
                code_text = tag.get_text().strip()
                if code_text:
                    # Determine language from class or content
                    language = self._detect_code_language(code_text, tag.get('class', []))
                    
                    # Get context (surrounding text)
                    context = self._get_code_context(tag)
                    
                    code_blocks.append(CodeBlock(
                        language=language,
                        code=code_text,
                        block_type='block' if tag_name == 'pre' else 'inline',
                        context=context
                    ))
        
        # Extract markdown code blocks from raw content
        markdown_pattern = self.code_patterns['markdown_block']
        for match in re.finditer(markdown_pattern, raw_content, re.DOTALL | re.IGNORECASE):
            language = match.group(1) or 'unknown'
            code_text = match.group(2).strip()
            
            if code_text:
                code_blocks.append(CodeBlock(
                    language=language,
                    code=code_text,
                    block_type='block',
                    context=""
                ))
        
        # Extract inline code 
        inline_pattern = self.code_patterns['inline_code']
        for match in re.finditer(inline_pattern, raw_content):
            code_text = match.group(1).strip()
            if len(code_text) > 3:  # Skip very short inline code
                language = self._detect_code_language(code_text)
                
                code_blocks.append(CodeBlock(
                    language=language,
                    code=code_text,
                    block_type='inline',
                    context=""
                ))
        
        # Deduplicate similar code blocks
        return self._deduplicate_code_blocks(code_blocks)
    
    def _detect_code_language(self, code: str, css_classes: List[str] = None) -> str:
        """Detect programming language from code content or CSS classes"""
        css_classes = css_classes or []
        
        # Check CSS classes first
        for cls in css_classes:
            if 'lang-' in cls:
                return cls.replace('lang-', '')
            if 'language-' in cls:
                return cls.replace('language-', '')
        
        code_lower = code.lower()
        
        # VBA detection
        vba_indicators = ['sub ', 'function ', 'dim ', 'as integer', 'as string', 'msgbox']
        if any(indicator in code_lower for indicator in vba_indicators):
            return 'vba'
        
        # Excel formula detection
        if code.startswith('=') or any(f'={func}(' in code_lower for func_list in self.excel_functions.values() for func in func_list):
            return 'excel'
        
        # SQL detection
        sql_keywords = ['select ', 'from ', 'where ', 'insert ', 'update ', 'delete ']
        if any(keyword in code_lower for keyword in sql_keywords):
            return 'sql'
        
        # Python detection
        python_keywords = ['def ', 'import ', 'from ', 'print(', 'if __name__']
        if any(keyword in code_lower for keyword in python_keywords):
            return 'python'
        
        # JavaScript detection
        js_keywords = ['function(', 'var ', 'let ', 'const ', 'console.log']
        if any(keyword in code_lower for keyword in js_keywords):
            return 'javascript'
        
        return 'unknown'
    
    def _get_code_context(self, tag) -> str:
        """Get surrounding text context for a code block"""
        try:
            # Get previous sibling text
            prev_text = ""
            if tag.previous_sibling:
                prev_text = str(tag.previous_sibling).strip()[-100:]  # Last 100 chars
            
            # Get next sibling text  
            next_text = ""
            if tag.next_sibling:
                next_text = str(tag.next_sibling).strip()[:100]  # First 100 chars
                
            return f"{prev_text} ... {next_text}".strip()
        except:
            return ""
    
    def _clean_html_content(self, soup: BeautifulSoup) -> str:
        """Clean HTML content and extract pure text"""
        # Remove code blocks (already extracted)
        for tag in soup.find_all(['pre', 'code']):
            tag.decompose()
        
        # Remove other unwanted elements
        for tag in soup.find_all(['script', 'style', 'img']):
            tag.decompose()
        
        # Get text and clean it
        text = soup.get_text()
        
        # Decode HTML entities
        text = html.unescape(text)
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove URLs
        text = re.sub(r'https?://[^\s]+', '[URL]', text)
        
        # Clean up line breaks and extra spaces
        text = re.sub(r'\n+', '\n', text)
        text = text.strip()
        
        return text
    
    def _detect_excel_formulas(self, text: str, code_blocks: List[CodeBlock]) -> tuple[bool, List[str]]:
        """Detect Excel formulas and functions in content"""
        found_functions = []
        has_formulas = False
        
        # Check text content for formula references
        text_lower = text.lower()
        
        # Check code blocks for formulas
        for block in code_blocks:
            if block.language in ['excel', 'unknown']:
                # Look for Excel formulas (starting with =)
                if '=' in block.code:
                    has_formulas = True
                    
                    # Extract function names
                    for category, functions in self.excel_functions.items():
                        for func in functions:
                            pattern = rf'={func}\s*\('
                            if re.search(pattern, block.code, re.IGNORECASE):
                                if func not in found_functions:
                                    found_functions.append(func)
        
        # Check text for function mentions
        for category, functions in self.excel_functions.items():
            for func in functions:
                if func in text_lower:
                    if func not in found_functions:
                        found_functions.append(func)
        
        return has_formulas, found_functions
    
    def _detect_vba_code(self, code_blocks: List[CodeBlock]) -> bool:
        """Detect VBA code in code blocks"""
        for block in code_blocks:
            if block.language == 'vba':
                return True
            
            # Check content for VBA keywords
            code_lower = block.code.lower()
            vba_score = sum(1 for keyword in self.vba_keywords if keyword in code_lower)
            
            if vba_score >= 2:  # Threshold for VBA detection
                return True
        
        return False
    
    def _deduplicate_code_blocks(self, code_blocks: List[CodeBlock]) -> List[CodeBlock]:
        """Remove duplicate or very similar code blocks"""
        if not code_blocks:
            return []
        
        deduplicated = []
        seen_codes = set()
        
        for block in code_blocks:
            # Create a normalized version for comparison
            normalized = re.sub(r'\s+', ' ', block.code.strip().lower())
            
            # Skip if we've seen this exact code
            if normalized in seen_codes:
                continue
            
            # Skip very short inline code
            if block.block_type == 'inline' and len(block.code) < 5:
                continue
                
            seen_codes.add(normalized)
            deduplicated.append(block)
        
        return deduplicated
    
    def get_processing_summary(self, result: TextProcessingResult) -> Dict[str, Any]:
        """Generate processing summary for logging/monitoring"""
        return {
            'processing_success': result.processing_success,
            'word_count': result.word_count,
            'code_blocks_found': len(result.code_blocks),
            'has_excel_formulas': result.has_formulas,
            'has_vba_code': result.has_vba,
            'excel_functions_detected': len(result.excel_functions),
            'code_languages': list(set(block.language for block in result.code_blocks)),
            'error': result.error
        }