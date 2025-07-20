"""
Semantic Deduplication System using Sentence Transformers
TRD Section 3.5: 코사인 유사도 0.95 이상인 경우 의미적 중복으로 처리
"""
import logging
import numpy as np
from typing import Dict, Any, List, Tuple, Set
from dataclasses import dataclass
import pickle
from pathlib import Path

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

from config import Config

logger = logging.getLogger('pipeline.deduplicator')

@dataclass
class DuplicationResult:
    """Result of deduplication analysis"""
    duplicate_groups: List[List[int]]  # Groups of duplicate indices
    kept_indices: List[int]  # Indices of items to keep
    removed_indices: List[int]  # Indices of items to remove
    similarity_matrix: np.ndarray  # Full similarity matrix
    total_removed: int
    
class SemanticDeduplicator:
    """
    Advanced semantic deduplication using sentence transformers
    
    TRD Requirements:
    - Use sentence-transformers for question title embeddings
    - Cosine similarity threshold: 0.95
    - Keep highest quality score within duplicate groups
    - Batch processing with configurable size
    """
    
    def __init__(self, cache_dir: Path = None):
        self.config = Config.DEDUPLICATION
        self.model_name = self.config['model']  # all-MiniLM-L6-v2
        self.similarity_threshold = self.config['similarity_threshold']  # 0.95
        self.batch_size = self.config['batch_size']  # 32
        
        # Initialize model
        self.model = SentenceTransformer(self.model_name)
        
        # Embedding cache
        self.cache_dir = cache_dir or Config.TEMP_DIR / 'embeddings'
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"SemanticDeduplicator initialized with model {self.model_name}, threshold {self.similarity_threshold}")
    
    def deduplicate_qa_pairs(self, qa_pairs: List[Dict[str, Any]], 
                           quality_scores: List[float]) -> DuplicationResult:
        """
        Main deduplication method following TRD specifications
        
        Args:
            qa_pairs: List of Q&A pairs with question titles
            quality_scores: Corresponding quality scores for selection
            
        Returns:
            DuplicationResult with deduplication decisions
        """
        if len(qa_pairs) != len(quality_scores):
            raise ValueError("Mismatch between Q&A pairs and quality scores")
        
        logger.info(f"Starting deduplication for {len(qa_pairs)} Q&A pairs")
        
        # Step 1: Extract question titles for embedding
        question_titles = self._extract_question_titles(qa_pairs)
        
        # Step 2: Generate embeddings with caching
        embeddings = self._get_embeddings_with_cache(question_titles)
        
        # Step 3: Calculate similarity matrix
        similarity_matrix = self._calculate_similarity_matrix(embeddings)
        
        # Step 4: Find duplicate groups using threshold
        duplicate_groups = self._find_duplicate_groups(similarity_matrix)
        
        # Step 5: Select best item from each group based on quality
        kept_indices, removed_indices = self._select_best_from_groups(
            duplicate_groups, quality_scores
        )
        
        result = DuplicationResult(
            duplicate_groups=duplicate_groups,
            kept_indices=kept_indices,
            removed_indices=removed_indices,
            similarity_matrix=similarity_matrix,
            total_removed=len(removed_indices)
        )
        
        logger.info(f"Deduplication complete: removed {result.total_removed} duplicates, kept {len(kept_indices)} items")
        return result
    
    def _extract_question_titles(self, qa_pairs: List[Dict[str, Any]]) -> List[str]:
        """Extract and clean question titles for embedding"""
        titles = []
        
        for qa_pair in qa_pairs:
            question = qa_pair.get('question', {})
            title = question.get('title', '')
            
            # Fallback to first part of question body if no title
            if not title:
                body = question.get('body', '') or question.get('body_markdown', '')
                if body:
                    # Take first sentence as pseudo-title
                    sentences = body.split('.')
                    title = sentences[0][:100] if sentences else body[:100]
            
            # Clean title
            cleaned_title = self._clean_title_for_embedding(title)
            titles.append(cleaned_title)
        
        logger.debug(f"Extracted {len(titles)} question titles")
        return titles
    
    def _clean_title_for_embedding(self, title: str) -> str:
        """Clean and normalize title for better embedding quality"""
        import re
        
        if not title:
            return "no title"
        
        # Remove HTML tags
        title = re.sub(r'<[^>]+>', ' ', title)
        
        # Remove excessive punctuation
        title = re.sub(r'[!?]{2,}', '!', title)
        
        # Normalize whitespace
        title = re.sub(r'\s+', ' ', title)
        
        # Remove common prefixes that don't add semantic value
        prefixes_to_remove = [
            r'^(how to|how do i|how can i|help with|question about)\s*',
            r'^(excel|vba|spreadsheet):\s*',
            r'^\[.*?\]\s*'
        ]
        
        for prefix in prefixes_to_remove:
            title = re.sub(prefix, '', title, flags=re.IGNORECASE)
        
        return title.strip()
    
    def _get_embeddings_with_cache(self, texts: List[str]) -> np.ndarray:
        """Generate embeddings with file-based caching for efficiency"""
        # Create cache key from texts
        cache_key = self._create_cache_key(texts)
        cache_path = self.cache_dir / f"embeddings_{cache_key}.pkl"
        
        # Try to load from cache
        if cache_path.exists():
            try:
                with open(cache_path, 'rb') as f:
                    cached_data = pickle.load(f)
                    if len(cached_data['embeddings']) == len(texts):
                        logger.info(f"Loaded embeddings from cache: {cache_path}")
                        return cached_data['embeddings']
            except Exception as e:
                logger.warning(f"Failed to load embedding cache: {e}")
        
        # Generate new embeddings
        logger.info(f"Generating embeddings for {len(texts)} texts")
        embeddings = self.model.encode(
            texts,
            batch_size=self.batch_size,
            show_progress_bar=True,
            convert_to_numpy=True
        )
        
        # Cache embeddings
        try:
            cache_data = {
                'embeddings': embeddings,
                'model_name': self.model_name,
                'text_count': len(texts)
            }
            with open(cache_path, 'wb') as f:
                pickle.dump(cache_data, f)
            logger.debug(f"Cached embeddings to: {cache_path}")
        except Exception as e:
            logger.warning(f"Failed to cache embeddings: {e}")
        
        return embeddings
    
    def _create_cache_key(self, texts: List[str]) -> str:
        """Create a stable cache key from text list"""
        import hashlib
        
        # Create hash from concatenated texts (first 50 chars each to keep reasonable size)
        content = '|'.join(text[:50] for text in texts)
        hash_obj = hashlib.md5(content.encode())
        return hash_obj.hexdigest()[:16]
    
    def _calculate_similarity_matrix(self, embeddings: np.ndarray) -> np.ndarray:
        """Calculate cosine similarity matrix for all embeddings"""
        logger.debug(f"Calculating similarity matrix for {len(embeddings)} embeddings")
        
        # Use sklearn for efficient cosine similarity calculation
        similarity_matrix = cosine_similarity(embeddings)
        
        # Set diagonal to 0 to ignore self-similarity
        np.fill_diagonal(similarity_matrix, 0)
        
        return similarity_matrix
    
    def _find_duplicate_groups(self, similarity_matrix: np.ndarray) -> List[List[int]]:
        """
        Find groups of duplicates using TRD threshold (0.95)
        Uses connected components approach for transitive duplicates
        """
        n_items = len(similarity_matrix)
        visited = set()
        duplicate_groups = []
        
        for i in range(n_items):
            if i in visited:
                continue
            
            # Find all items similar to item i
            similar_items = [i]
            to_check = [i]
            visited.add(i)
            
            while to_check:
                current = to_check.pop(0)
                
                # Find items similar to current item
                similar_indices = np.where(similarity_matrix[current] >= self.similarity_threshold)[0]
                
                for similar_idx in similar_indices:
                    if similar_idx not in visited:
                        visited.add(similar_idx)
                        similar_items.append(similar_idx)
                        to_check.append(similar_idx)
            
            # Only consider groups with multiple items as duplicates
            if len(similar_items) > 1:
                duplicate_groups.append(sorted(similar_items))
        
        logger.info(f"Found {len(duplicate_groups)} duplicate groups")
        for i, group in enumerate(duplicate_groups):
            logger.debug(f"Duplicate group {i+1}: {len(group)} items - {group}")
        
        return duplicate_groups
    
    def _select_best_from_groups(self, duplicate_groups: List[List[int]], 
                               quality_scores: List[float]) -> Tuple[List[int], List[int]]:
        """
        Select best item from each duplicate group based on quality score
        TRD: 그룹 내에서는 quality_score가 가장 높은 항목만 유지
        """
        all_indices = set(range(len(quality_scores)))
        removed_indices = []
        kept_indices = []
        
        # Process each duplicate group
        for group in duplicate_groups:
            # Find item with highest quality score in group
            group_scores = [(idx, quality_scores[idx]) for idx in group]
            group_scores.sort(key=lambda x: x[1], reverse=True)  # Sort by score descending
            
            best_idx = group_scores[0][0]
            removed_idx_in_group = [idx for idx, _ in group_scores[1:]]
            
            kept_indices.append(best_idx)
            removed_indices.extend(removed_idx_in_group)
            
            logger.debug(f"Group {group}: kept item {best_idx} (score={quality_scores[best_idx]:.2f}), "
                        f"removed {len(removed_idx_in_group)} items")
        
        # Add all non-duplicate items to kept list
        duplicate_indices = set()
        for group in duplicate_groups:
            duplicate_indices.update(group)
        
        non_duplicate_indices = all_indices - duplicate_indices
        kept_indices.extend(list(non_duplicate_indices))
        
        return sorted(kept_indices), sorted(removed_indices)
    
    def apply_deduplication(self, qa_pairs: List[Dict[str, Any]], 
                          result: DuplicationResult) -> List[Dict[str, Any]]:
        """Apply deduplication result to filter Q&A pairs"""
        deduplicated_pairs = []
        
        for idx in result.kept_indices:
            qa_pair = qa_pairs[idx].copy()
            
            # Add deduplication metadata
            qa_pair['deduplication_info'] = {
                'was_duplicate': idx not in set(range(len(qa_pairs))) - set(result.removed_indices),
                'duplicate_group_size': self._get_group_size_for_index(idx, result.duplicate_groups),
                'removal_reason': None
            }
            
            deduplicated_pairs.append(qa_pair)
        
        logger.info(f"Applied deduplication: {len(deduplicated_pairs)} items remaining")
        return deduplicated_pairs
    
    def _get_group_size_for_index(self, idx: int, duplicate_groups: List[List[int]]) -> int:
        """Get the size of duplicate group containing the given index"""
        for group in duplicate_groups:
            if idx in group:
                return len(group)
        return 1  # Not in any duplicate group
    
    def get_deduplication_stats(self, result: DuplicationResult) -> Dict[str, Any]:
        """Calculate deduplication statistics"""
        total_items = len(result.kept_indices) + len(result.removed_indices)
        
        group_sizes = [len(group) for group in result.duplicate_groups]
        
        return {
            'total_input_items': total_items,
            'duplicate_groups_found': len(result.duplicate_groups),
            'total_duplicates_removed': result.total_removed,
            'items_kept': len(result.kept_indices),
            'deduplication_rate': (result.total_removed / total_items) * 100 if total_items > 0 else 0,
            'average_group_size': np.mean(group_sizes) if group_sizes else 0,
            'largest_duplicate_group': max(group_sizes) if group_sizes else 0,
            'similarity_threshold_used': self.similarity_threshold
        }
    
    def analyze_similarity_distribution(self, similarity_matrix: np.ndarray) -> Dict[str, Any]:
        """Analyze the distribution of similarity scores for insights"""
        # Get upper triangle (avoid double counting)
        upper_triangle = np.triu(similarity_matrix, k=1)
        similarities = upper_triangle[upper_triangle > 0]
        
        if len(similarities) == 0:
            return {'error': 'No similarities to analyze'}
        
        return {
            'total_comparisons': len(similarities),
            'mean_similarity': np.mean(similarities),
            'median_similarity': np.median(similarities),
            'std_similarity': np.std(similarities),
            'min_similarity': np.min(similarities),
            'max_similarity': np.max(similarities),
            'above_threshold': np.sum(similarities >= self.similarity_threshold),
            'threshold_percentage': (np.sum(similarities >= self.similarity_threshold) / len(similarities)) * 100
        }