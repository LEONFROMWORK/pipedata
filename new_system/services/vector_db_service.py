"""
Vector Database Service for Excel Q&A RAG System
Integrates with ChromaDB for semantic search and retrieval
"""
import chromadb
from chromadb.config import Settings
from chromadb.utils import embedding_functions
import logging
import json
import uuid
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from pathlib import Path
import asyncio
import aiofiles
from dataclasses import dataclass, asdict
import hashlib

logger = logging.getLogger('vector_db_service')

@dataclass
class QADocument:
    """Document structure for Q&A pairs"""
    id: str
    question: str
    context: str
    answer: str
    code_blocks: List[str]
    metadata: Dict[str, Any]
    embedding_text: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

class VectorDBService:
    """Vector database service for Excel Q&A retrieval"""
    
    def __init__(self, db_path: str = None, collection_name: str = "excel_qa"):
        self.db_path = db_path or "/Users/kevin/bigdata/new_system/data/vector_db"
        self.collection_name = collection_name
        
        # Initialize ChromaDB
        self.client = chromadb.PersistentClient(
            path=self.db_path,
            settings=Settings(
                chroma_db_impl="duckdb+parquet",
                persist_directory=self.db_path
            )
        )
        
        # Use OpenAI embeddings for better performance
        self.embedding_function = embedding_functions.OpenAIEmbeddingFunction(
            api_key=self._get_openai_api_key(),
            model_name="text-embedding-3-small"
        )
        
        # Initialize collection
        self.collection = self._get_or_create_collection()
        
        # Statistics
        self.stats = {
            "total_documents": 0,
            "successful_queries": 0,
            "failed_queries": 0,
            "last_updated": None
        }
        
        self._update_stats()
    
    def _get_openai_api_key(self) -> str:
        """Get OpenAI API key from config"""
        try:
            from config import Config
            return Config.OPENAI_API_KEY
        except:
            # Fallback to environment variable
            import os
            return os.getenv("OPENAI_API_KEY", "")
    
    def _get_or_create_collection(self):
        """Get or create ChromaDB collection"""
        try:
            # Try to get existing collection
            collection = self.client.get_collection(
                name=self.collection_name,
                embedding_function=self.embedding_function
            )
            logger.info(f"Retrieved existing collection: {self.collection_name}")
            return collection
        except ValueError:
            # Collection doesn't exist, create new one
            collection = self.client.create_collection(
                name=self.collection_name,
                embedding_function=self.embedding_function,
                metadata={"hnsw:space": "cosine"}
            )
            logger.info(f"Created new collection: {self.collection_name}")
            return collection
    
    def _update_stats(self):
        """Update collection statistics"""
        try:
            count = self.collection.count()
            self.stats["total_documents"] = count
            self.stats["last_updated"] = datetime.now().isoformat()
            logger.info(f"Collection stats updated: {count} documents")
        except Exception as e:
            logger.error(f"Error updating stats: {e}")
    
    def _prepare_document(self, qa_data: Dict[str, Any]) -> QADocument:
        """Prepare document for vector storage"""
        # Generate unique ID
        doc_id = qa_data.get("id", str(uuid.uuid4()))
        
        # Extract components
        question = qa_data.get("user_question", "")
        context = qa_data.get("user_context", "")
        answer = qa_data.get("assistant_response", "")
        code_blocks = qa_data.get("code_blocks", [])
        metadata = qa_data.get("metadata", {})
        
        # Create embedding text (combination of question, context, and answer)
        embedding_text = f"Question: {question}\nContext: {context}\nAnswer: {answer}"
        
        # Add code blocks to embedding text
        if code_blocks:
            code_text = "\nCode: " + " ".join(code_blocks)
            embedding_text += code_text
        
        # Enhance metadata
        enhanced_metadata = {
            **metadata,
            "timestamp": datetime.now().isoformat(),
            "text_length": len(embedding_text),
            "has_code": len(code_blocks) > 0,
            "question_length": len(question),
            "difficulty": metadata.get("difficulty", "unknown"),
            "functions": metadata.get("functions", []),
            "source": metadata.get("source", "unknown")
        }
        
        return QADocument(
            id=doc_id,
            question=question,
            context=context,
            answer=answer,
            code_blocks=code_blocks,
            metadata=enhanced_metadata,
            embedding_text=embedding_text
        )
    
    async def add_document(self, qa_data: Dict[str, Any]) -> bool:
        """Add a single Q&A document to the vector database"""
        try:
            doc = self._prepare_document(qa_data)
            
            # Check if document already exists
            if await self.document_exists(doc.id):
                logger.info(f"Document {doc.id} already exists, skipping")
                return True
            
            # Add to collection
            self.collection.add(
                documents=[doc.embedding_text],
                metadatas=[doc.metadata],
                ids=[doc.id]
            )
            
            logger.info(f"Added document {doc.id} to vector database")
            self._update_stats()
            return True
            
        except Exception as e:
            logger.error(f"Error adding document: {e}")
            return False
    
    async def add_documents_batch(self, qa_data_list: List[Dict[str, Any]], batch_size: int = 100) -> Dict[str, Any]:
        """Add multiple Q&A documents in batches"""
        try:
            total_docs = len(qa_data_list)
            added_count = 0
            skipped_count = 0
            failed_count = 0
            
            logger.info(f"Adding {total_docs} documents in batches of {batch_size}")
            
            for i in range(0, total_docs, batch_size):
                batch = qa_data_list[i:i + batch_size]
                
                # Prepare batch
                documents = []
                metadatas = []
                ids = []
                
                for qa_data in batch:
                    try:
                        doc = self._prepare_document(qa_data)
                        
                        # Check if document already exists
                        if await self.document_exists(doc.id):
                            skipped_count += 1
                            continue
                        
                        documents.append(doc.embedding_text)
                        metadatas.append(doc.metadata)
                        ids.append(doc.id)
                        
                    except Exception as e:
                        logger.error(f"Error preparing document: {e}")
                        failed_count += 1
                
                # Add batch to collection
                if documents:
                    self.collection.add(
                        documents=documents,
                        metadatas=metadatas,
                        ids=ids
                    )
                    added_count += len(documents)
                    
                logger.info(f"Batch {i//batch_size + 1}: Added {len(documents)} documents")
            
            self._update_stats()
            
            return {
                "success": True,
                "total_processed": total_docs,
                "added": added_count,
                "skipped": skipped_count,
                "failed": failed_count,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error adding documents batch: {e}")
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def search_similar(self, query: str, n_results: int = 5, 
                           filter_metadata: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Search for similar documents"""
        try:
            logger.info(f"Searching for: {query[:50]}...")
            
            # Prepare query
            query_results = self.collection.query(
                query_texts=[query],
                n_results=n_results,
                where=filter_metadata,
                include=["documents", "metadatas", "distances"]
            )
            
            # Format results
            results = []
            if query_results["documents"] and query_results["documents"][0]:
                for i, doc in enumerate(query_results["documents"][0]):
                    result = {
                        "id": query_results["ids"][0][i],
                        "document": doc,
                        "metadata": query_results["metadatas"][0][i],
                        "distance": query_results["distances"][0][i],
                        "similarity": 1 - query_results["distances"][0][i]  # Convert distance to similarity
                    }
                    results.append(result)
            
            self.stats["successful_queries"] += 1
            logger.info(f"Found {len(results)} similar documents")
            return results
            
        except Exception as e:
            logger.error(f"Error searching similar documents: {e}")
            self.stats["failed_queries"] += 1
            return []
    
    async def search_by_keywords(self, keywords: List[str], n_results: int = 5) -> List[Dict[str, Any]]:
        """Search documents by keywords"""
        try:
            # Create search query from keywords
            query = " ".join(keywords)
            
            # Search with keyword-based metadata filter
            filter_metadata = {
                "$or": [
                    {"functions": {"$in": keywords}},
                    {"difficulty": {"$in": keywords}},
                    {"source": {"$in": keywords}}
                ]
            }
            
            return await self.search_similar(query, n_results, filter_metadata)
            
        except Exception as e:
            logger.error(f"Error searching by keywords: {e}")
            return []
    
    async def hybrid_search(self, query: str, keywords: List[str] = None, 
                           difficulty: str = None, functions: List[str] = None,
                           n_results: int = 5) -> List[Dict[str, Any]]:
        """Hybrid search combining semantic and keyword-based filtering"""
        try:
            # Build metadata filter
            filter_conditions = []
            
            if keywords:
                filter_conditions.extend([
                    {"functions": {"$in": keywords}},
                    {"difficulty": {"$in": keywords}}
                ])
            
            if difficulty:
                filter_conditions.append({"difficulty": difficulty})
            
            if functions:
                filter_conditions.append({"functions": {"$in": functions}})
            
            # Combine filters
            filter_metadata = None
            if filter_conditions:
                if len(filter_conditions) == 1:
                    filter_metadata = filter_conditions[0]
                else:
                    filter_metadata = {"$or": filter_conditions}
            
            # Perform semantic search with filters
            semantic_results = await self.search_similar(query, n_results, filter_metadata)
            
            # If we have fewer results than requested, try without filters
            if len(semantic_results) < n_results:
                additional_results = await self.search_similar(query, n_results - len(semantic_results))
                
                # Add unique results
                existing_ids = {result["id"] for result in semantic_results}
                for result in additional_results:
                    if result["id"] not in existing_ids:
                        semantic_results.append(result)
                        if len(semantic_results) >= n_results:
                            break
            
            # Sort by similarity
            semantic_results.sort(key=lambda x: x["similarity"], reverse=True)
            
            logger.info(f"Hybrid search found {len(semantic_results)} results")
            return semantic_results[:n_results]
            
        except Exception as e:
            logger.error(f"Error in hybrid search: {e}")
            return []
    
    async def search_by_complexity(self, query: str, complexity_level: str, n_results: int = 5) -> List[Dict[str, Any]]:
        """Search for documents by complexity level"""
        try:
            # Map complexity levels
            complexity_mapping = {
                "beginner": ["beginner", "basic", "simple"],
                "intermediate": ["intermediate", "medium", "advanced"],
                "advanced": ["advanced", "expert", "complex"]
            }
            
            difficulty_values = complexity_mapping.get(complexity_level.lower(), [complexity_level])
            
            return await self.hybrid_search(
                query=query,
                difficulty=difficulty_values[0],
                keywords=difficulty_values,
                n_results=n_results
            )
            
        except Exception as e:
            logger.error(f"Error searching by complexity: {e}")
            return []
    
    async def get_document_by_id(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Get document by ID"""
        try:
            result = self.collection.get(
                ids=[doc_id],
                include=["documents", "metadatas"]
            )
            
            if result["documents"]:
                return {
                    "id": doc_id,
                    "document": result["documents"][0],
                    "metadata": result["metadatas"][0]
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting document by ID: {e}")
            return None
    
    async def document_exists(self, doc_id: str) -> bool:
        """Check if document exists in the database"""
        try:
            result = self.collection.get(ids=[doc_id])
            return len(result["ids"]) > 0
        except Exception as e:
            logger.error(f"Error checking document existence: {e}")
            return False
    
    async def delete_document(self, doc_id: str) -> bool:
        """Delete document by ID"""
        try:
            self.collection.delete(ids=[doc_id])
            self._update_stats()
            logger.info(f"Deleted document {doc_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting document: {e}")
            return False
    
    async def clear_collection(self) -> bool:
        """Clear all documents from the collection"""
        try:
            # Delete the collection and recreate it
            self.client.delete_collection(name=self.collection_name)
            self.collection = self._get_or_create_collection()
            self._update_stats()
            logger.info("Collection cleared")
            return True
        except Exception as e:
            logger.error(f"Error clearing collection: {e}")
            return False
    
    async def load_jsonl_file(self, file_path: str) -> Dict[str, Any]:
        """Load Q&A data from JSONL file"""
        try:
            qa_data_list = []
            
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                async for line in f:
                    if line.strip():
                        qa_data = json.loads(line.strip())
                        qa_data_list.append(qa_data)
            
            logger.info(f"Loaded {len(qa_data_list)} Q&A pairs from {file_path}")
            
            # Add to vector database
            result = await self.add_documents_batch(qa_data_list)
            
            return {
                "success": True,
                "file_path": file_path,
                "loaded_count": len(qa_data_list),
                "add_result": result,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error loading JSONL file: {e}")
            return {
                "success": False,
                "error": str(e),
                "file_path": file_path,
                "timestamp": datetime.now().isoformat()
            }
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get vector database statistics"""
        self._update_stats()
        return {
            "collection_name": self.collection_name,
            "db_path": self.db_path,
            "stats": self.stats,
            "embedding_function": "OpenAI text-embedding-3-small",
            "timestamp": datetime.now().isoformat()
        }
    
    def __del__(self):
        """Cleanup on destruction"""
        try:
            if hasattr(self, 'client'):
                # ChromaDB client cleanup is handled automatically
                pass
        except:
            pass

# Singleton instance
_vector_db_service = None

async def get_vector_db_service() -> VectorDBService:
    """Get singleton vector database service instance"""
    global _vector_db_service
    if _vector_db_service is None:
        _vector_db_service = VectorDBService()
    return _vector_db_service

async def initialize_vector_db_from_oppadu_data():
    """Initialize vector database with existing Oppadu data"""
    try:
        vector_service = await get_vector_db_service()
        
        # Find recent Oppadu data files
        data_dir = Path("/Users/kevin/bigdata/data/output")
        jsonl_files = list(data_dir.glob("**/oppadu_collection_*.jsonl"))
        
        if not jsonl_files:
            logger.warning("No Oppadu data files found")
            return {"success": False, "error": "No data files found"}
        
        # Load the most recent file
        latest_file = max(jsonl_files, key=lambda f: f.stat().st_mtime)
        logger.info(f"Loading data from: {latest_file}")
        
        result = await vector_service.load_jsonl_file(str(latest_file))
        
        if result["success"]:
            logger.info(f"Successfully initialized vector DB with {result['loaded_count']} documents")
        else:
            logger.error(f"Failed to initialize vector DB: {result['error']}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error initializing vector DB: {e}")
        return {"success": False, "error": str(e)}