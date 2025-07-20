"""
Excel Q&A Controller
Main controller that integrates all services for Excel problem solving
"""
import logging
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass, asdict

from services.excel_ai_service import get_excel_ai_service, ExcelAIService
from services.vector_db_service import get_vector_db_service, VectorDBService
from services.excel_validator_service import get_excel_validator_service, ExcelValidatorService
from services.multimodal_rag_service import create_multimodal_rag_service, MultimodalRAGService
from services.monitoring_service import get_monitoring_service, MonitoringService, monitor_performance

logger = logging.getLogger('excel_qa_controller')

@dataclass
class ExcelQARequest:
    """Request for Excel Q&A"""
    question: str
    context: str = ""
    images: List[str] = None
    user_id: str = ""
    session_id: str = ""
    
    def __post_init__(self):
        if self.images is None:
            self.images = []

@dataclass
class ExcelQAResponse:
    """Response from Excel Q&A"""
    success: bool
    solution: str = ""
    formulas: List[str] = None
    validation_results: Dict[str, Any] = None
    metadata: Dict[str, Any] = None
    error: str = ""
    
    def __post_init__(self):
        if self.formulas is None:
            self.formulas = []
        if self.validation_results is None:
            self.validation_results = {}
        if self.metadata is None:
            self.metadata = {}
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class ExcelQAController:
    """Main controller for Excel Q&A system"""
    
    def __init__(self):
        self.excel_ai_service: Optional[ExcelAIService] = None
        self.vector_db_service: Optional[VectorDBService] = None
        self.excel_validator_service: Optional[ExcelValidatorService] = None
        self.multimodal_rag_service: Optional[MultimodalRAGService] = None
        self.monitoring_service: Optional[MonitoringService] = None
        
        # System statistics
        self.stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_cost": 0.0,
            "average_response_time": 0.0,
            "last_request": None,
            "system_uptime": datetime.now().isoformat()
        }
        
        # Configuration
        self.config = {
            "max_vector_results": 5,
            "require_formula_validation": True,
            "min_similarity_threshold": 0.3,
            "max_response_time": 30.0
        }
    
    async def initialize(self) -> bool:
        """Initialize all services"""
        try:
            logger.info("🚀 Initializing Excel Q&A Controller...")
            
            # Initialize services
            self.excel_ai_service = await get_excel_ai_service()
            self.vector_db_service = await get_vector_db_service()
            self.excel_validator_service = await get_excel_validator_service()
            
            # Initialize multimodal RAG service
            self.multimodal_rag_service = await create_multimodal_rag_service(
                self.vector_db_service, 
                self.excel_ai_service
            )
            
            # Initialize monitoring service
            self.monitoring_service = await get_monitoring_service()
            
            # Test connections
            await self._test_services()
            
            logger.info("✅ Excel Q&A Controller initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Excel Q&A Controller: {e}")
            return False
    
    async def _test_services(self):
        """Test all services are working"""
        # Test vector DB
        vector_stats = await self.vector_db_service.get_statistics()
        logger.info(f"📊 Vector DB: {vector_stats['stats']['total_documents']} documents")
        
        # Test Excel AI service
        ai_stats = await self.excel_ai_service.get_usage_statistics()
        logger.info(f"🤖 AI Service: {len(ai_stats['model_configurations'])} models configured")
        
        # Test Excel validator
        validator_stats = await self.excel_validator_service.get_statistics()
        logger.info(f"🔍 Validator: {validator_stats['excel_functions_count']} functions supported")
    
    async def process_question(self, request: ExcelQARequest) -> ExcelQAResponse:
        """Process Excel Q&A request"""
        start_time = datetime.now()
        
        # Start monitoring
        async with monitor_performance("excel_qa_controller", "process_question"):
            try:
                self.stats["total_requests"] += 1
                logger.info(f"📝 Processing question: {request.question[:100]}...")
                
                # Step 1: Get multimodal context (text + images)
                if request.images:
                    multimodal_context = await self.multimodal_rag_service.process_multimodal_query(
                        request.question, request.context, request.images
                    )
                    vector_context = self.multimodal_rag_service.format_multimodal_context(multimodal_context)
                else:
                    vector_context = await self._search_vector_context(request.question)
                
                # Step 2: Generate solution using AI service
                ai_response = await self._generate_solution(request, vector_context)
                
                if not ai_response["success"]:
                    return self._create_error_response(ai_response["error"])
                
                # Step 3: Validate formulas in the response
                validation_results = await self._validate_solution(ai_response["solution"])
                
                # Step 4: Calculate metrics
                end_time = datetime.now()
                response_time = (end_time - start_time).total_seconds()
                
                # Update statistics
                self.stats["successful_requests"] += 1
                self.stats["total_cost"] += ai_response["metadata"]["cost"]
                self._update_response_time(response_time)
                self.stats["last_request"] = end_time.isoformat()
                
                # Create response
                response = ExcelQAResponse(
                    success=True,
                    solution=ai_response["solution"],
                    formulas=self._extract_formulas(ai_response["solution"]),
                    validation_results=validation_results,
                    metadata={
                        **ai_response["metadata"],
                        "response_time": response_time,
                        "vector_context_found": len(vector_context) > 0,
                        "formula_validation": validation_results.get("overall_valid", False),
                        "multimodal_processing": bool(request.images),
                        "images_processed": len(request.images) if request.images else 0,
                        "timestamp": end_time.isoformat()
                    }
                )
                
                # Record monitoring data
                await self._record_monitoring_data(request, response, response_time)
                
                logger.info(f"✅ Question processed successfully in {response_time:.2f}s")
                return response
                
            except Exception as e:
                logger.error(f"❌ Error processing question: {e}")
                self.stats["failed_requests"] += 1
                
                # Record error in monitoring
                if self.monitoring_service:
                    await self.monitoring_service.record_request({
                        "success": False,
                        "error": str(e),
                        "question": request.question[:100],
                        "response_time": (datetime.now() - start_time).total_seconds()
                    })
                
                return self._create_error_response(str(e))
    
    async def _search_vector_context(self, question: str, complexity_hint: str = None) -> str:
        """Search vector database for relevant context using hybrid search"""
        try:
            if not self.vector_db_service:
                return ""
            
            # Extract potential Excel functions from question
            excel_functions = self._extract_excel_functions(question)
            
            # Use hybrid search for better results
            similar_docs = await self.vector_db_service.hybrid_search(
                query=question,
                functions=excel_functions,
                n_results=self.config["max_vector_results"]
            )
            
            # Filter by similarity threshold
            relevant_docs = [
                doc for doc in similar_docs 
                if doc["similarity"] >= self.config["min_similarity_threshold"]
            ]
            
            if not relevant_docs:
                logger.info("📚 No relevant documents found in vector DB")
                return ""
            
            # Build enhanced context string
            context_parts = []
            for i, doc in enumerate(relevant_docs, 1):
                metadata = doc["metadata"]
                similarity = doc["similarity"]
                
                context_parts.append(f"""
Example {i} (Similarity: {similarity:.2f}):
- Functions Used: {metadata.get('functions', [])}
- Difficulty: {metadata.get('difficulty', 'unknown')}
- Problem Type: {metadata.get('source', 'unknown')}
- Solution: {doc['document'][:300]}...
""")
            
            vector_context = "\n".join(context_parts)
            logger.info(f"📚 Found {len(relevant_docs)} relevant documents using hybrid search")
            return vector_context
            
        except Exception as e:
            logger.error(f"Error searching vector context: {e}")
            return ""
    
    def _extract_excel_functions(self, text: str) -> List[str]:
        """Extract Excel function names from text"""
        import re
        
        # Common Excel functions
        excel_functions = [
            "SUM", "AVERAGE", "COUNT", "MAX", "MIN", "IF", "VLOOKUP", "HLOOKUP",
            "INDEX", "MATCH", "SUMIF", "SUMIFS", "COUNTIF", "COUNTIFS", "ROUND",
            "ABS", "AND", "OR", "NOT", "IFERROR", "XLOOKUP", "FILTER", "SORT",
            "UNIQUE", "TEXTJOIN", "CONCATENATE", "LEFT", "RIGHT", "MID", "LEN",
            "DATE", "TODAY", "NOW", "YEAR", "MONTH", "DAY", "WEEKDAY"
        ]
        
        found_functions = []
        text_upper = text.upper()
        
        for func in excel_functions:
            if func in text_upper:
                found_functions.append(func)
        
        return found_functions
    
    async def _record_monitoring_data(self, request: ExcelQARequest, response: ExcelQAResponse, response_time: float):
        """Record monitoring data for the request"""
        try:
            if not self.monitoring_service:
                return
            
            monitoring_data = {
                "success": response.success,
                "response_time": response_time,
                "cost": response.metadata.get("cost", 0.0),
                "quality_score": response.metadata.get("quality_score", 0.0),
                "tier_used": response.metadata.get("tier", "unknown"),
                "model_used": response.metadata.get("model_used", "unknown"),
                "escalated": response.metadata.get("escalated", False),
                "multimodal_processing": response.metadata.get("multimodal_processing", False),
                "images_processed": response.metadata.get("images_processed", 0),
                "vector_context_found": response.metadata.get("vector_context_found", False),
                "formula_validation": response.metadata.get("formula_validation", False),
                "formulas_count": len(response.formulas),
                "question_length": len(request.question),
                "context_length": len(request.context),
                "user_id": request.user_id,
                "session_id": request.session_id
            }
            
            await self.monitoring_service.record_request(monitoring_data)
            
        except Exception as e:
            logger.error(f"Error recording monitoring data: {e}")
    
    async def _generate_solution(self, request: ExcelQARequest, vector_context: str) -> Dict[str, Any]:
        """Generate solution using AI service"""
        try:
            if not self.excel_ai_service:
                raise Exception("Excel AI service not initialized")
            
            # Call AI service
            response = await self.excel_ai_service.solve_excel_problem(
                question=request.question,
                context=request.context,
                images=request.images,
                vector_context=vector_context
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Error generating solution: {e}")
            return {"success": False, "error": str(e)}
    
    async def _validate_solution(self, solution: str) -> Dict[str, Any]:
        """Validate formulas in the solution"""
        try:
            if not self.excel_validator_service or not self.config["require_formula_validation"]:
                return {"validation_skipped": True}
            
            # Validate formulas
            validation_results = await self.excel_validator_service.validate_ai_response(solution)
            
            if validation_results.get("has_formulas", False):
                logger.info(f"🔍 Validated {validation_results['formula_count']} formulas")
                if not validation_results.get("overall_valid", False):
                    logger.warning(f"⚠️ {validation_results['invalid_count']} formulas failed validation")
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Error validating solution: {e}")
            return {"validation_error": str(e)}
    
    def _extract_formulas(self, solution: str) -> List[str]:
        """Extract formulas from solution text"""
        import re
        formula_pattern = r'=\s*[A-Z]+\s*\([^)]*\)'
        formulas = re.findall(formula_pattern, solution, re.IGNORECASE)
        return [formula.strip() for formula in formulas]
    
    def _create_error_response(self, error: str) -> ExcelQAResponse:
        """Create error response"""
        return ExcelQAResponse(
            success=False,
            error=error,
            metadata={
                "timestamp": datetime.now().isoformat(),
                "error_type": "processing_error"
            }
        )
    
    def _update_response_time(self, response_time: float):
        """Update average response time"""
        current_avg = self.stats["average_response_time"]
        successful_requests = self.stats["successful_requests"]
        
        if successful_requests == 1:
            self.stats["average_response_time"] = response_time
        else:
            # Calculate running average
            self.stats["average_response_time"] = (
                (current_avg * (successful_requests - 1) + response_time) / successful_requests
            )
    
    async def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        try:
            # Get service statistics
            vector_stats = await self.vector_db_service.get_statistics() if self.vector_db_service else {}
            ai_stats = await self.excel_ai_service.get_usage_statistics() if self.excel_ai_service else {}
            validator_stats = await self.excel_validator_service.get_statistics() if self.excel_validator_service else {}
            
            return {
                "system_stats": self.stats,
                "configuration": self.config,
                "services": {
                    "vector_db": vector_stats,
                    "excel_ai": ai_stats,
                    "excel_validator": validator_stats
                },
                "health": {
                    "vector_db_ready": self.vector_db_service is not None,
                    "ai_service_ready": self.excel_ai_service is not None,
                    "validator_ready": self.excel_validator_service is not None
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def update_vector_database(self, qa_data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Update vector database with new Q&A data"""
        try:
            if not self.vector_db_service:
                return {"success": False, "error": "Vector DB service not initialized"}
            
            result = await self.vector_db_service.add_documents_batch(qa_data_list)
            
            if result["success"]:
                logger.info(f"📚 Added {result['added']} documents to vector DB")
            
            return result
            
        except Exception as e:
            logger.error(f"Error updating vector database: {e}")
            return {"success": False, "error": str(e)}
    
    async def cleanup(self):
        """Clean up resources"""
        try:
            if self.excel_ai_service:
                await self.excel_ai_service.close()
            
            if self.excel_validator_service:
                self.excel_validator_service.cleanup()
            
            logger.info("🧹 Excel Q&A Controller cleaned up")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Singleton instance
_excel_qa_controller = None

async def get_excel_qa_controller() -> ExcelQAController:
    """Get singleton Excel Q&A controller instance"""
    global _excel_qa_controller
    if _excel_qa_controller is None:
        _excel_qa_controller = ExcelQAController()
        await _excel_qa_controller.initialize()
    return _excel_qa_controller

async def cleanup_excel_qa_controller():
    """Clean up Excel Q&A controller"""
    global _excel_qa_controller
    if _excel_qa_controller:
        await _excel_qa_controller.cleanup()
        _excel_qa_controller = None