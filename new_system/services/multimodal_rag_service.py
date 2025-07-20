"""
Multimodal RAG Service for Excel Q&A
Handles text + image processing for better context understanding
"""
import logging
import base64
import asyncio
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from pathlib import Path
import mimetypes
from dataclasses import dataclass
import json

from services.vector_db_service import VectorDBService
from services.excel_ai_service import ExcelAIService

logger = logging.getLogger('multimodal_rag_service')

@dataclass
class MultimodalContext:
    """Context combining text and image information"""
    text_context: str
    image_descriptions: List[str]
    excel_elements: List[str]
    formulas_detected: List[str]
    confidence_score: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "text_context": self.text_context,
            "image_descriptions": self.image_descriptions,
            "excel_elements": self.excel_elements,
            "formulas_detected": self.formulas_detected,
            "confidence_score": self.confidence_score
        }

class MultimodalRAGService:
    """Service for multimodal RAG processing"""
    
    def __init__(self, vector_db_service: VectorDBService, excel_ai_service: ExcelAIService):
        self.vector_db_service = vector_db_service
        self.excel_ai_service = excel_ai_service
        
        # Image processing configuration
        self.config = {
            "max_image_size": 5 * 1024 * 1024,  # 5MB
            "supported_formats": ["png", "jpg", "jpeg", "webp", "gif"],
            "max_images_per_request": 5,
            "image_analysis_model": "tier_2"  # Use Llama 4 Maverick for image analysis
        }
        
        # Statistics
        self.stats = {
            "total_multimodal_requests": 0,
            "successful_image_analyses": 0,
            "failed_image_analyses": 0,
            "average_confidence": 0.0,
            "excel_elements_detected": 0,
            "formulas_extracted": 0
        }
    
    async def process_multimodal_query(self, question: str, context: str, 
                                     images: List[str]) -> MultimodalContext:
        """Process multimodal query with text and images"""
        try:
            self.stats["total_multimodal_requests"] += 1
            
            # Validate images
            valid_images = await self._validate_images(images)
            
            if not valid_images:
                logger.warning("No valid images provided, falling back to text-only")
                text_context = await self._get_text_context(question)
                return MultimodalContext(
                    text_context=text_context,
                    image_descriptions=[],
                    excel_elements=[],
                    formulas_detected=[],
                    confidence_score=0.5
                )
            
            # Analyze images for Excel content
            image_analysis = await self._analyze_excel_images(valid_images, question)
            
            # Get text context based on question + image analysis
            enhanced_question = self._enhance_question_with_image_context(question, image_analysis)
            text_context = await self._get_text_context(enhanced_question)
            
            # Combine contexts
            multimodal_context = MultimodalContext(
                text_context=text_context,
                image_descriptions=image_analysis.get("descriptions", []),
                excel_elements=image_analysis.get("excel_elements", []),
                formulas_detected=image_analysis.get("formulas", []),
                confidence_score=image_analysis.get("confidence", 0.7)
            )
            
            # Update statistics
            self.stats["successful_image_analyses"] += 1
            self.stats["excel_elements_detected"] += len(image_analysis.get("excel_elements", []))
            self.stats["formulas_extracted"] += len(image_analysis.get("formulas", []))
            self._update_confidence_score(image_analysis.get("confidence", 0.7))
            
            logger.info(f"Processed multimodal query with {len(valid_images)} images")
            return multimodal_context
            
        except Exception as e:
            logger.error(f"Error processing multimodal query: {e}")
            self.stats["failed_image_analyses"] += 1
            
            # Fallback to text-only
            text_context = await self._get_text_context(question)
            return MultimodalContext(
                text_context=text_context,
                image_descriptions=[],
                excel_elements=[],
                formulas_detected=[],
                confidence_score=0.3
            )
    
    async def _validate_images(self, images: List[str]) -> List[str]:
        """Validate image inputs"""
        valid_images = []
        
        for image in images:
            if not image:
                continue
                
            # Check if it's a base64 encoded image
            if image.startswith('data:image/'):
                # Extract format and validate
                try:
                    header, data = image.split(',', 1)
                    mime_type = header.split(';')[0].split(':')[1]
                    format_ext = mime_type.split('/')[1].lower()
                    
                    if format_ext in self.config["supported_formats"]:
                        # Check size (approximate)
                        estimated_size = len(data) * 0.75  # Base64 overhead
                        if estimated_size <= self.config["max_image_size"]:
                            valid_images.append(image)
                        else:
                            logger.warning(f"Image too large: {estimated_size} bytes")
                    else:
                        logger.warning(f"Unsupported image format: {format_ext}")
                        
                except Exception as e:
                    logger.error(f"Error validating base64 image: {e}")
                    
            elif image.startswith('http'):
                # URL image
                valid_images.append(image)
                
            else:
                # File path
                try:
                    image_path = Path(image)
                    if image_path.exists():
                        # Check file size
                        if image_path.stat().st_size <= self.config["max_image_size"]:
                            # Check format
                            mime_type, _ = mimetypes.guess_type(str(image_path))
                            if mime_type and mime_type.startswith('image/'):
                                format_ext = mime_type.split('/')[1].lower()
                                if format_ext in self.config["supported_formats"]:
                                    valid_images.append(image)
                                else:
                                    logger.warning(f"Unsupported image format: {format_ext}")
                            else:
                                logger.warning(f"Not a valid image file: {image}")
                        else:
                            logger.warning(f"Image file too large: {image}")
                    else:
                        logger.warning(f"Image file not found: {image}")
                except Exception as e:
                    logger.error(f"Error validating image file: {e}")
        
        # Limit number of images
        if len(valid_images) > self.config["max_images_per_request"]:
            logger.warning(f"Too many images, using first {self.config['max_images_per_request']}")
            valid_images = valid_images[:self.config["max_images_per_request"]]
        
        return valid_images
    
    async def _analyze_excel_images(self, images: List[str], question: str) -> Dict[str, Any]:
        """Analyze images for Excel content using AI"""
        try:
            # Prepare prompt for image analysis
            analysis_prompt = f"""Analyze these Excel-related images and extract the following information:

1. Describe what you see in each image
2. Identify Excel elements (cells, formulas, charts, tables, etc.)
3. Extract any Excel formulas visible in the images
4. Determine how these images relate to the question: "{question}"

Please provide your analysis in the following JSON format:
{{
    "descriptions": ["Description of image 1", "Description of image 2"],
    "excel_elements": ["Cell A1", "Formula in B2", "Chart in C1:E5"],
    "formulas": ["=SUM(A1:A10)", "=VLOOKUP(...)"],
    "confidence": 0.85,
    "relevance_to_question": "How the images relate to the question"
}}

Focus on Excel-specific content and be as detailed as possible."""
            
            # Call the AI service with images
            from services.excel_ai_service import ModelTier
            model_config = self.excel_ai_service.models[ModelTier.TIER_2]  # Use Llama 4 Maverick
            
            response = await self.excel_ai_service._call_model(
                tier=ModelTier.TIER_2,
                prompt=analysis_prompt,
                images=images
            )
            
            if response["success"]:
                # Try to parse JSON response
                try:
                    analysis_result = self._parse_image_analysis(response["response"])
                    return analysis_result
                except Exception as e:
                    logger.error(f"Error parsing image analysis: {e}")
                    return self._fallback_image_analysis(response["response"])
            else:
                logger.error(f"Image analysis failed: {response.get('error', 'Unknown error')}")
                return self._create_empty_analysis()
                
        except Exception as e:
            logger.error(f"Error analyzing Excel images: {e}")
            return self._create_empty_analysis()
    
    def _parse_image_analysis(self, response: str) -> Dict[str, Any]:
        """Parse structured image analysis response"""
        try:
            # Try to extract JSON from response
            import re
            
            # Look for JSON in the response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(0)
                analysis_data = json.loads(json_str)
                
                return {
                    "descriptions": analysis_data.get("descriptions", []),
                    "excel_elements": analysis_data.get("excel_elements", []),
                    "formulas": analysis_data.get("formulas", []),
                    "confidence": analysis_data.get("confidence", 0.7),
                    "relevance": analysis_data.get("relevance_to_question", "")
                }
            else:
                # Fallback to text parsing
                return self._fallback_image_analysis(response)
                
        except Exception as e:
            logger.error(f"Error parsing image analysis JSON: {e}")
            return self._fallback_image_analysis(response)
    
    def _fallback_image_analysis(self, response: str) -> Dict[str, Any]:
        """Fallback analysis when structured parsing fails"""
        # Extract potential formulas
        import re
        
        # Look for Excel formulas
        formula_pattern = r'=\s*[A-Z]+\s*\([^)]*\)'
        formulas = re.findall(formula_pattern, response, re.IGNORECASE)
        
        # Look for cell references
        cell_pattern = r'[A-Z]+\d+(?::[A-Z]+\d+)?'
        cells = re.findall(cell_pattern, response)
        
        # Basic description extraction
        descriptions = [response[:200] + "..." if len(response) > 200 else response]
        
        return {
            "descriptions": descriptions,
            "excel_elements": cells[:10],  # Limit to first 10
            "formulas": formulas,
            "confidence": 0.5,
            "relevance": "Extracted from text analysis"
        }
    
    def _create_empty_analysis(self) -> Dict[str, Any]:
        """Create empty analysis result"""
        return {
            "descriptions": [],
            "excel_elements": [],
            "formulas": [],
            "confidence": 0.0,
            "relevance": "Analysis failed"
        }
    
    def _enhance_question_with_image_context(self, question: str, image_analysis: Dict[str, Any]) -> str:
        """Enhance question with image context for better vector search"""
        enhanced_parts = [question]
        
        # Add detected formulas
        formulas = image_analysis.get("formulas", [])
        if formulas:
            enhanced_parts.append(f"Formulas in images: {', '.join(formulas)}")
        
        # Add Excel elements
        excel_elements = image_analysis.get("excel_elements", [])
        if excel_elements:
            enhanced_parts.append(f"Excel elements: {', '.join(excel_elements[:5])}")
        
        # Add relevance context
        relevance = image_analysis.get("relevance", "")
        if relevance:
            enhanced_parts.append(f"Context: {relevance}")
        
        return " ".join(enhanced_parts)
    
    async def _get_text_context(self, question: str) -> str:
        """Get text context from vector database"""
        try:
            # Use hybrid search for better results
            similar_docs = await self.vector_db_service.hybrid_search(
                query=question,
                n_results=3
            )
            
            if not similar_docs:
                return ""
            
            # Build context
            context_parts = []
            for doc in similar_docs:
                context_parts.append(doc['document'][:300])
            
            return "\n---\n".join(context_parts)
            
        except Exception as e:
            logger.error(f"Error getting text context: {e}")
            return ""
    
    def _update_confidence_score(self, confidence: float):
        """Update average confidence score"""
        current_avg = self.stats["average_confidence"]
        successful_count = self.stats["successful_image_analyses"]
        
        if successful_count == 1:
            self.stats["average_confidence"] = confidence
        else:
            self.stats["average_confidence"] = (
                (current_avg * (successful_count - 1) + confidence) / successful_count
            )
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get multimodal RAG statistics"""
        return {
            "stats": self.stats.copy(),
            "config": self.config.copy(),
            "timestamp": datetime.now().isoformat()
        }
    
    def format_multimodal_context(self, context: MultimodalContext) -> str:
        """Format multimodal context for AI prompt"""
        if not context.image_descriptions and not context.excel_elements:
            return context.text_context
        
        formatted_parts = []
        
        # Add text context
        if context.text_context:
            formatted_parts.append(f"Related Knowledge:\n{context.text_context}")
        
        # Add image descriptions
        if context.image_descriptions:
            formatted_parts.append(f"\nImage Analysis:\n" + "\n".join([
                f"- {desc}" for desc in context.image_descriptions
            ]))
        
        # Add Excel elements
        if context.excel_elements:
            formatted_parts.append(f"\nExcel Elements Detected:\n" + "\n".join([
                f"- {element}" for element in context.excel_elements
            ]))
        
        # Add formulas
        if context.formulas_detected:
            formatted_parts.append(f"\nFormulas in Images:\n" + "\n".join([
                f"- {formula}" for formula in context.formulas_detected
            ]))
        
        return "\n".join(formatted_parts)

# Factory function
async def create_multimodal_rag_service(vector_db_service: VectorDBService, 
                                       excel_ai_service: ExcelAIService) -> MultimodalRAGService:
    """Create multimodal RAG service instance"""
    return MultimodalRAGService(vector_db_service, excel_ai_service)