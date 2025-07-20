"""
Main Pipeline Implementation - End-to-End Workflow
TRD Section 6: 상세 워크플로우 (End-to-End Process)

Pipeline Flow:
1. Data Collector → Stack Overflow API 호출
2. Triage → <img> 태그 검사로 Text/Image 경로 분기
3. Parallel Processing → Text Processor와 Image Processor 병렬 실행
4. Unifier & Scorer → 결과 통합 및 품질 점수 계산
5. Deduplicator → 중복 제거
6. Final Generator → JSONL 형식으로 최종 데이터셋 생성
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import json

from core.cache import LocalCache, APICache
from collectors.stackoverflow_collector import StackOverflowCollector
from collectors.reddit_collector import RedditCollector

# OppaduCrawler - optional (requires Selenium)
try:
    from collectors.oppadu_crawler import OppaduCrawler
    OPPADU_AVAILABLE = True
except ImportError:
    OPPADU_AVAILABLE = False
    logging.warning("OppaduCrawler not available (Selenium required)")
    
from processors.triage import ContentTriageSystem
from processors.text_processor import TextProcessor
from processors.image_processor import ImageProcessor
from quality.scorer import QualityScorer
from quality.simple_reddit_scorer import SimpleRedditScorer
from quality.korean_oppadu_scorer import KoreanOppaduScorer
from quality.deduplicator import SemanticDeduplicator
from output.dataset_generator import JSONLDatasetGenerator
from output.oppadu_dataset_generator import OppaduDatasetGenerator
from config import Config

logger = logging.getLogger('pipeline.main')

class PipelineState:
    """Pipeline execution state for checkpointing and recovery"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.current_stage = "initialized"
        self.collected_count = 0
        self.processed_count = 0
        self.quality_filtered_count = 0
        self.deduplicated_count = 0
        self.final_count = 0
        self.errors = []
        self.completed_stages = []
        self.data_sources = []
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'start_time': self.start_time.isoformat(),
            'current_stage': self.current_stage,
            'collected_count': self.collected_count,
            'processed_count': self.processed_count,
            'quality_filtered_count': self.quality_filtered_count,
            'deduplicated_count': self.deduplicated_count,
            'final_count': self.final_count,
            'errors': self.errors,
            'completed_stages': self.completed_stages,
            'data_sources': self.data_sources
        }

class ExcelQAPipeline:
    """
    Main pipeline implementing TRD End-to-End workflow
    
    Features:
    - Incremental collection with fromdate parameter
    - Parallel text/image processing
    - Quality-based filtering and deduplication
    - Error handling with dead-letter queue
    - State checkpointing for recovery
    """
    
    def __init__(self):
        # Initialize components
        self.cache = LocalCache(Config.DATABASE_PATH)
        self.api_cache = APICache(self.cache)
        
        # Data collectors
        self.so_collector = StackOverflowCollector(self.api_cache)
        self.reddit_collector = RedditCollector(self.api_cache)
        self.oppadu_crawler = OppaduCrawler(self.api_cache) if OPPADU_AVAILABLE else None
        
        # Processing components
        self.triage = ContentTriageSystem()
        self.text_processor = TextProcessor()
        self.image_processor = ImageProcessor(self.api_cache)
        
        # Quality assessment (source-specific)
        self.so_quality_scorer = QualityScorer()
        self.reddit_quality_scorer = SimpleRedditScorer()
        self.oppadu_quality_scorer = KoreanOppaduScorer()
        
        # Common components
        self.deduplicator = SemanticDeduplicator()
        self.dataset_generator = JSONLDatasetGenerator()
        self.oppadu_dataset_generator = OppaduDatasetGenerator()  # 오빠두 전용 생성기
        
        # Pipeline state
        self.state = PipelineState()
        
        # Dead letter queue for failed items
        self.dead_letter_dir = Config.DATA_DIR / 'failed'
        self.dead_letter_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("ExcelQAPipeline initialized with Stack Overflow + Reddit + Oppadu collectors")
    
    async def run_full_pipeline(self, from_date: Optional[datetime] = None, 
                              max_pages: int = 50,
                              target_count: int = 1000,
                              sources: List[str] = None) -> Dict[str, Any]:
        """
        Run the complete pipeline following TRD workflow
        
        Args:
            from_date: Start date for incremental collection
            max_pages: Maximum pages to collect from API
            target_count: Target number of final items (TRD: ~1000/day)
            sources: List of sources to collect from ['stackoverflow', 'reddit', 'oppadu']
            
        Returns:
            Pipeline execution results
        """
        if sources is None:
            sources = ['stackoverflow', 'reddit', 'oppadu']  # Default to all three
        
        # Store sources in state for later use
        self.state.data_sources = sources
        
        logger.info(f"Starting full pipeline: target={target_count}, sources={sources}, from_date={from_date}")
        
        try:
            # Stage 1: Data Collection (Multi-source)
            self.state.current_stage = "collection"
            raw_qa_pairs = await self._run_multi_source_collection(from_date, max_pages, sources)
            self.state.collected_count = len(raw_qa_pairs)
            self.state.completed_stages.append("collection")
            
            if not raw_qa_pairs:
                logger.warning("No data collected, pipeline terminating")
                return self._create_pipeline_result()
            
            # Stage 2: Triage and Processing
            self.state.current_stage = "processing"
            processed_qa_pairs = await self._run_processing_stage(raw_qa_pairs)
            self.state.processed_count = len(processed_qa_pairs)
            self.state.completed_stages.append("processing")
            
            # Stage 3: Quality Scoring and Filtering
            self.state.current_stage = "quality_filtering"
            quality_filtered_pairs = await self._run_quality_stage(processed_qa_pairs)
            self.state.quality_filtered_count = len(quality_filtered_pairs)
            self.state.completed_stages.append("quality_filtering")
            
            # Stage 4: Deduplication
            self.state.current_stage = "deduplication"
            deduplicated_pairs = await self._run_deduplication_stage(quality_filtered_pairs)
            self.state.deduplicated_count = len(deduplicated_pairs)
            self.state.completed_stages.append("deduplication")
            
            # Stage 5: Final Dataset Generation
            self.state.current_stage = "dataset_generation"
            dataset_path = await self._run_dataset_generation(deduplicated_pairs)
            self.state.final_count = len(deduplicated_pairs)
            self.state.completed_stages.append("dataset_generation")
            
            # Pipeline completed successfully
            self.state.current_stage = "completed"
            result = self._create_pipeline_result()
            result['dataset_path'] = dataset_path
            
            logger.info(f"Pipeline completed successfully: {self.state.final_count} final items")
            return result
            
        except Exception as e:
            logger.error(f"Pipeline failed at stage {self.state.current_stage}: {e}")
            self.state.errors.append(f"Stage {self.state.current_stage}: {str(e)}")
            return self._create_pipeline_result()
        
        finally:
            # Cleanup
            await self.so_collector.close()
            # Reddit collector doesn't need async cleanup
    
    async def _run_multi_source_collection(self, from_date: Optional[datetime], 
                                          max_pages: int, sources: List[str]) -> List[Dict[str, Any]]:
        """Stage 1: Multi-source Data Collection"""
        logger.info(f"Starting multi-source data collection: {sources}")
        
        all_qa_pairs = []
        collection_results = {}
        
        # Collect from Stack Overflow
        if 'stackoverflow' in sources:
            try:
                logger.info("Collecting from Stack Overflow...")
                so_pairs = await self.so_collector.collect_excel_questions(
                    from_date=from_date,
                    max_pages=max_pages
                )
                
                # Convert SO format to standard format
                converted_so_pairs = []
                for so_item in so_pairs:
                    # Transform Stack Overflow format to pipeline format
                    converted_pair = {
                        'source': 'stackoverflow',
                        'question': {
                            'title': so_item.get('title', ''),
                            'body': so_item.get('body_markdown', ''),
                            'text': f"{so_item.get('title', '')}\n{so_item.get('body_markdown', '')}".strip(),
                            'body_markdown': so_item.get('body_markdown', ''),
                            'id': str(so_item.get('question_id', '')),
                            'so_id': so_item.get('question_id'),
                            'tags': so_item.get('tags', []),
                            'score': so_item.get('score', 0),
                            'link': so_item.get('link', ''),
                            'view_count': so_item.get('view_count', 0)
                        },
                        'answer': {
                            'text': so_item.get('accepted_answer', {}).get('body', ''),
                            'body_markdown': so_item.get('accepted_answer', {}).get('body', ''),
                            'id': str(so_item.get('accepted_answer', {}).get('answer_id', '')),
                            'so_id': so_item.get('accepted_answer', {}).get('answer_id'),
                            'score': so_item.get('accepted_answer', {}).get('score', 0),
                            'author': so_item.get('accepted_answer', {}).get('owner', {}).get('display_name', '')
                        },
                        'so_metadata': {
                            'is_answered': so_item.get('is_answered', False),
                            'accepted_answer_id': so_item.get('accepted_answer_id'),
                            'answer_count': so_item.get('answer_count', 0),
                            'creation_date': so_item.get('creation_date'),
                            'last_activity_date': so_item.get('last_activity_date'),
                            'owner': so_item.get('owner', {})
                        }
                    }
                    converted_so_pairs.append(converted_pair)
                
                all_qa_pairs.extend(converted_so_pairs)
                collection_results['stackoverflow'] = len(converted_so_pairs)
                logger.info(f"Stack Overflow collection complete: {len(converted_so_pairs)} Q&A pairs")
                
            except Exception as e:
                logger.error(f"Stack Overflow collection failed: {e}")
                self._save_to_dead_letter('so_collection_error', {'error': str(e)})
                collection_results['stackoverflow'] = 0
        
        # Collect from Reddit
        if 'reddit' in sources:
            try:
                logger.info("Collecting from Reddit...")
                reddit_results = await self.reddit_collector.collect_excel_discussions(
                    from_date=from_date,
                    max_submissions=max_pages * 2  # Reddit typically has lower yield
                )
                
                # Convert Reddit results to standard format
                reddit_pairs = []
                for result in reddit_results:
                    # Debug: Check the type and content of Reddit objects
                    print(f"🔍 [DEBUG] Reddit result type: {type(result)}")
                    print(f"🔍 [DEBUG] result.submission type: {type(result.submission)}")
                    print(f"🔍 [DEBUG] result.solution type: {type(result.solution)}")
                    
                    # Extract actual text content from Reddit objects
                    # Reddit 컬렉터는 항상 딕셔너리를 반환함 (위의 디버깅에서 확인됨)
                    submission = result.submission
                    solution = result.solution
                    
                    print(f"🔍 [DEBUG] Submission keys: {list(submission.keys()) if isinstance(submission, dict) else 'Not dict'}")
                    print(f"🔍 [DEBUG] Solution keys: {list(solution.keys()) if isinstance(solution, dict) else 'Not dict'}")
                    
                    # Reddit 컬렉터가 딕셔너리를 반환하므로 딕셔너리 방식으로 처리
                    question_title = submission.get('title', '') if isinstance(submission, dict) else getattr(submission, 'title', '')
                    question_body = submission.get('selftext', '') if isinstance(submission, dict) else getattr(submission, 'selftext', '')
                    answer_text = solution.get('body', '') if isinstance(solution, dict) else getattr(solution, 'body', '')
                    
                    # Clean up text
                    question_title = question_title.strip() if question_title else ''
                    question_body = question_body.strip() if question_body else ''
                    answer_text = answer_text.strip() if answer_text else ''
                    
                    question_text = f"{question_title}\n{question_body}".strip() if question_body else question_title.strip()
                    
                    print(f"🔍 [DEBUG] Extracted - title: '{question_title[:50]}...', body: '{question_body[:50]}...', answer: '{answer_text[:50]}...'")
                    print(f"🔍 [DEBUG] Final question_text: '{question_text[:100]}...'")
                    print(f"🔍 [DEBUG] Text lengths - title: {len(question_title)}, body: {len(question_body)}, answer: {len(answer_text)}")
                    
                    # Debug: Check text content before filtering
                    print(f"🔍 [REDDIT DEBUG] Question text length: {len(question_text)}, Answer text length: {len(answer_text)}")
                    print(f"🔍 [REDDIT DEBUG] Question preview: '{question_text[:100]}...' ")
                    print(f"🔍 [REDDIT DEBUG] Answer preview: '{answer_text[:100]}...'")
                    
                    # TEMPORARY: Don't skip empty content for debugging
                    if not question_text or not answer_text:
                        print(f"⚠️ [REDDIT DEBUG] FOUND empty content but NOT SKIPPING: Q={bool(question_text)}, A={bool(answer_text)}")
                        print(f"🔍 [REDDIT DEBUG] Question title: '{question_title}', body: '{question_body[:100] if question_body else 'N/A'}...'")
                        print(f"🔍 [REDDIT DEBUG] Answer body: '{answer_text[:100] if answer_text else 'N/A'}...'")
                        # continue  # COMMENTED OUT FOR DEBUGGING
                    else:
                        print(f"✅ [REDDIT DEBUG] Found valid content: Q={len(question_text)}, A={len(answer_text)}")
                    
                    reddit_pair = {
                        'source': 'reddit',
                        'question': {
                            'title': question_title,
                            'body': question_body,
                            'text': question_text,
                            'body_markdown': question_text,  # For compatibility with text processor
                            'url': result.submission.get('url', ''),
                            'id': result.submission.get('id', ''),  # Use 'id' not 'reddit_id'
                            'reddit_id': result.submission.get('id', ''),
                            'permalink': result.submission.get('permalink', ''),
                            'tags': ['excel'],  # Default tag for Reddit excel posts
                            'score': result.submission.get('score', 0)
                        },
                        'answer': {
                            'text': answer_text,
                            'body_markdown': answer_text,  # For compatibility with text processor
                            'score': result.solution.get('score', 0),
                            'id': result.solution.get('id', ''),  # Use 'id' not 'reddit_id'
                            'reddit_id': result.solution.get('id', ''),
                            'author': result.solution.get('author', ''),
                            'permalink': result.solution.get('permalink', '')
                        },
                        'reddit_metadata': result.metadata
                    }
                    
                    print(f"🔍 [DEBUG] Created reddit_pair - Q title: '{question_title[:50]}...', A text: '{answer_text[:50]}...'")
                    reddit_pairs.append(reddit_pair)
                
                all_qa_pairs.extend(reddit_pairs)
                collection_results['reddit'] = len(reddit_pairs)
                logger.info(f"Reddit collection complete: {len(reddit_pairs)} Q&A pairs")
                
            except Exception as e:
                logger.error(f"Reddit collection failed: {e}")
                self._save_to_dead_letter('reddit_collection_error', {'error': str(e)})
                collection_results['reddit'] = 0
        
        # Collect from Oppadu (한국 커뮤니티)
        if 'oppadu' in sources:
            if self.oppadu_crawler:
                try:
                    logger.info("🇰🇷 Collecting from Oppadu (Korean community)...")
                    oppadu_results = await self.oppadu_crawler.collect_oppadu_questions(
                        max_pages=max_pages
                    )
                
                # Convert Oppadu results to standard format
                oppadu_pairs = []
                for oppadu_data in oppadu_results:
                    
                    # Extract question and answer data
                    question_data = oppadu_data.get('question', {})
                    answer_data = oppadu_data.get('answer', {})
                    
                    # Standard format conversion
                    oppadu_pair = {
                        'source': 'oppadu',
                        'question': {
                            'title': oppadu_data.get('title', ''),
                            'body': question_data.get('text', ''),
                            'text': f"{oppadu_data.get('title', '')}\n{question_data.get('text', '')}".strip(),
                            'body_markdown': question_data.get('text', ''),  # For compatibility
                            'url': oppadu_data.get('url', ''),
                            'id': oppadu_data.get('post_id', ''),
                            'oppadu_id': oppadu_data.get('post_id', ''),
                            'tags': ['excel', 'korean'],  # Korean Excel posts
                            'score': oppadu_data.get('quality_score', 0),
                            'images': question_data.get('images', []),
                            'has_code': question_data.get('has_code', False),
                            'excel_version': oppadu_data.get('metadata', {}).get('excel_version', ''),
                            'os_version': oppadu_data.get('metadata', {}).get('os_version', '')
                        },
                        'answer': {
                            'text': answer_data.get('text', ''),
                            'body_markdown': answer_data.get('text', ''),  # For compatibility
                            'id': f"{oppadu_data.get('post_id', '')}_answer",
                            'score': oppadu_data.get('quality_score', 0),
                            'author': 'oppadu_community',
                            'images': answer_data.get('images', []),
                            'has_code': answer_data.get('has_code', False)
                        },
                        'oppadu_metadata': {
                            'excel_version': oppadu_data.get('metadata', {}).get('excel_version', ''),
                            'os_version': oppadu_data.get('metadata', {}).get('os_version', ''),
                            'country': 'KR',
                            'language': 'ko',
                            'source_type': 'korean_community',
                            'cultural_context': 'korean_business',
                            'collection_date': oppadu_data.get('collected_at', ''),
                            'quality_score': oppadu_data.get('quality_score', 0)
                        }
                    }
                    
                    oppadu_pairs.append(oppadu_pair)
                    logger.debug(f"Converted Oppadu post: {oppadu_data.get('title', '')[:50]}...")
                
                all_qa_pairs.extend(oppadu_pairs)
                collection_results['oppadu'] = len(oppadu_pairs)
                logger.info(f"🇰🇷 Oppadu collection complete: {len(oppadu_pairs)} Korean Q&A pairs")
                
                except Exception as e:
                    logger.error(f"Oppadu collection failed: {e}")
                    self._save_to_dead_letter('oppadu_collection_error', {'error': str(e)})
                    collection_results['oppadu'] = 0
            else:
                logger.warning("Oppadu crawler not available (Selenium required)")
                collection_results['oppadu'] = 0
        
        logger.info(f"Multi-source collection complete: {collection_results}")
        return all_qa_pairs
    
    async def _run_processing_stage(self, raw_qa_pairs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Stage 2: Triage and Parallel Processing"""
        logger.info(f"Starting processing stage for {len(raw_qa_pairs)} items")
        
        processed_pairs = []
        
        for i, qa_pair in enumerate(raw_qa_pairs):
            try:
                # Triage: determine processing path
                triage_result = self.triage.triage_content(
                    qa_pair['question'],
                    qa_pair.get('answer', {})
                )
                
                # Always run text processing
                text_result = self.text_processor.process_text_content(
                    qa_pair['question'].get('body_markdown', ''),
                    qa_pair.get('answer', {}).get('body_markdown', '')
                )
                
                # Add triage and text processing results
                qa_pair['triage_result'] = triage_result.__dict__
                qa_pair['text_processing'] = text_result.__dict__
                
                # Run image processing if needed
                if triage_result.processing_path == 'image_included':
                    image_results = await self._process_images_parallel(
                        triage_result.image_urls,
                        qa_pair['question'].get('tags', [])
                    )
                    qa_pair['image_processing'] = image_results
                else:
                    qa_pair['image_processing'] = {'success': False, 'reason': 'no_images'}
                
                processed_pairs.append(qa_pair)
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(raw_qa_pairs)} items")
                    
            except Exception as e:
                logger.error(f"Processing failed for item {i}: {e}")
                self._save_to_dead_letter('processing_error', {
                    'item_index': i,
                    'qa_pair': qa_pair,
                    'error': str(e)
                })
                continue
        
        logger.info(f"Processing stage complete: {len(processed_pairs)} items processed")
        return processed_pairs
    
    async def _process_images_parallel(self, image_urls: List[str], 
                                     context_tags: List[str]) -> Dict[str, Any]:
        """Process multiple images in parallel with error handling"""
        if not image_urls:
            return {'success': False, 'reason': 'no_urls'}
        
        try:
            # Process first image (Stack Overflow posts typically have one main image)
            primary_url = image_urls[0]
            result = await self.image_processor.process_image_url(primary_url, context_tags)
            
            # Add information about additional images if any
            if len(image_urls) > 1:
                result['additional_images'] = len(image_urls) - 1
                result['all_image_urls'] = image_urls
            
            return result
            
        except Exception as e:
            logger.error(f"Image processing failed for URLs {image_urls}: {e}")
            return {
                'success': False,
                'error': str(e),
                'image_urls': image_urls
            }
    
    async def _run_quality_stage(self, processed_pairs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Stage 3: Quality Scoring and Filtering (Multi-source)"""
        logger.info(f"Starting quality assessment for {len(processed_pairs)} items")
        
        try:
            # Separate by source for appropriate scoring
            so_pairs = [pair for pair in processed_pairs if pair.get('source') == 'stackoverflow']
            reddit_pairs = [pair for pair in processed_pairs if pair.get('source') == 'reddit']
            oppadu_pairs = [pair for pair in processed_pairs if pair.get('source') == 'oppadu']
            
            quality_filtered = []
            
            # Process Stack Overflow pairs
            if so_pairs:
                logger.info(f"Processing {len(so_pairs)} Stack Overflow items")
                so_metrics = self.so_quality_scorer.score_batch(so_pairs)
                so_filtered = self.so_quality_scorer.filter_by_quality(so_pairs, so_metrics)
                quality_filtered.extend(so_filtered)
                
                so_stats = self.so_quality_scorer.get_batch_statistics(so_metrics)
                logger.info(f"Stack Overflow filtering: {len(so_filtered)}/{len(so_pairs)} passed "
                           f"(threshold: {Config.QUALITY_SCORING['threshold']}, "
                           f"avg score: {so_stats.get('average_score', 0):.2f})")
            
            # Process Reddit pairs with Simple Reddit scorer
            if reddit_pairs:
                logger.info(f"Processing {len(reddit_pairs)} Reddit items with Simple Scorer")
                
                # Score Reddit batch with simple scorer
                reddit_quality_results = self.reddit_quality_scorer.score_batch(reddit_pairs)
                
                # Filter Reddit pairs
                reddit_filtered = self.reddit_quality_scorer.filter_by_quality(
                    reddit_pairs, reddit_quality_results
                )
                
                quality_filtered.extend(reddit_filtered)
                
                # Get statistics
                reddit_stats = self.reddit_quality_scorer.get_batch_statistics(reddit_quality_results)
                logger.info(f"Simple Reddit scoring: {len(reddit_filtered)}/{len(reddit_pairs)} passed "
                           f"(pass rate: {reddit_stats.get('pass_rate', 0):.1f}%, "
                           f"avg score: {reddit_stats.get('average_score', 0):.2f})")
            
            # Process Oppadu pairs with Korean Oppadu scorer
            if oppadu_pairs:
                logger.info(f"🇰🇷 Processing {len(oppadu_pairs)} Oppadu items with Korean Scorer")
                
                # Score Oppadu batch with Korean scorer
                oppadu_quality_results = self.oppadu_quality_scorer.score_batch(oppadu_pairs)
                
                # Filter Oppadu pairs (낮은 threshold로 한국 특성 반영)
                oppadu_filtered = self.oppadu_quality_scorer.filter_by_quality(
                    oppadu_pairs, oppadu_quality_results, threshold=5.5  # 한국 데이터 특성상 낮은 임계값
                )
                
                quality_filtered.extend(oppadu_filtered)
                
                # Get statistics
                oppadu_stats = self.oppadu_quality_scorer.get_batch_statistics(oppadu_quality_results)
                logger.info(f"🇰🇷 Korean Oppadu scoring: {len(oppadu_filtered)}/{len(oppadu_pairs)} passed "
                           f"(Korean threshold: 5.5, "
                           f"avg score: {oppadu_stats.get('average_score', 0):.2f}, "
                           f"Korean business posts: {oppadu_stats.get('korean_business_posts', 0)})")
            
            logger.info(f"Quality filtering complete: {len(quality_filtered)} total items passed")
            return quality_filtered
            
        except Exception as e:
            logger.error(f"Quality assessment failed: {e}")
            self._save_to_dead_letter('quality_error', {
                'processed_pairs_count': len(processed_pairs),
                'error': str(e)
            })
            return []
    
    async def _run_deduplication_stage(self, quality_pairs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Stage 4: Semantic Deduplication"""
        logger.info(f"Starting deduplication for {len(quality_pairs)} items")
        
        try:
            # Extract quality scores for deduplication selection
            quality_scores = [
                pair.get('quality_metrics', {}).get('overall_score', 0.0)
                for pair in quality_pairs
            ]
            
            # Run deduplication
            dedup_result = self.deduplicator.deduplicate_qa_pairs(
                quality_pairs, quality_scores
            )
            
            # Apply deduplication
            deduplicated_pairs = self.deduplicator.apply_deduplication(
                quality_pairs, dedup_result
            )
            
            # Log statistics
            stats = self.deduplicator.get_deduplication_stats(dedup_result)
            logger.info(f"Deduplication complete: removed {stats['total_duplicates_removed']} duplicates, "
                       f"kept {len(deduplicated_pairs)} items "
                       f"({stats['deduplication_rate']:.1f}% deduplication rate)")
            
            return deduplicated_pairs
            
        except Exception as e:
            logger.error(f"Deduplication failed: {e}")
            self._save_to_dead_letter('deduplication_error', {
                'quality_pairs_count': len(quality_pairs),
                'error': str(e)
            })
            return quality_pairs  # Return original if deduplication fails
    
    async def _run_dataset_generation(self, final_pairs: List[Dict[str, Any]]) -> str:
        """Stage 5: Final JSONL Dataset Generation (Source-specific)"""
        logger.info(f"Starting dataset generation for {len(final_pairs)} items")
        
        try:
            # 소스별로 데이터 분리
            oppadu_pairs = [pair for pair in final_pairs if pair.get('source') == 'oppadu']
            other_pairs = [pair for pair in final_pairs if pair.get('source') != 'oppadu']
            
            generated_paths = []
            
            # 오빠두 데이터는 별도 파일로 생성 (한국 특화)
            if oppadu_pairs:
                logger.info(f"🇰🇷 Generating Korean Oppadu dataset: {len(oppadu_pairs)} items")
                oppadu_path = self.oppadu_dataset_generator.generate_oppadu_dataset(
                    oppadu_pairs,
                    metadata={
                        'pipeline_execution': self.state.to_dict(),
                        'generation_timestamp': datetime.now().isoformat()
                    }
                )
                if oppadu_path:
                    generated_paths.append(oppadu_path)
                    
                    # 오빠두 데이터셋 검증
                    oppadu_validation = self.oppadu_dataset_generator.validate_korean_dataset(oppadu_path)
                    logger.info(f"🇰🇷 Korean Oppadu validation: {oppadu_validation.get('korean_content_lines', 0)} Korean content lines")
            
            # 다른 소스들은 기존 방식으로 결합
            if other_pairs:
                logger.info(f"Generating combined dataset: {len(other_pairs)} items")
                combined_path = self.dataset_generator.generate_dataset(
                    other_pairs,
                    metadata={
                        'pipeline_execution': self.state.to_dict(),
                        'generation_timestamp': datetime.now().isoformat()
                    },
                    data_sources=[src for src in self.state.data_sources if src != 'oppadu']
                )
                if combined_path:
                    generated_paths.append(combined_path)
                    
                    # 결합 데이터셋 검증
                    validation_result = self.dataset_generator.validate_dataset(combined_path)
                    if validation_result['valid']:
                        logger.info(f"Combined dataset validation passed: {validation_result['valid_lines']} valid lines")
                    else:
                        logger.warning(f"Combined dataset validation issues: {validation_result.get('errors', [])}")
            
            # 주 경로 반환 (오빠두가 있으면 오빠두, 없으면 결합)
            main_path = generated_paths[0] if generated_paths else ""
            
            logger.info(f"Dataset generation complete. Generated files: {len(generated_paths)}")
            for path in generated_paths:
                logger.info(f"  📁 {path}")
            
            return main_path
            
        except Exception as e:
            logger.error(f"Dataset generation failed: {e}")
            self._save_to_dead_letter('dataset_generation_error', {
                'final_pairs_count': len(final_pairs),
                'error': str(e)
            })
            raise
    
    def _save_to_dead_letter(self, error_type: str, data: Dict[str, Any]) -> None:
        """Save failed items to dead letter queue for later analysis"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{error_type}_{timestamp}.json"
        file_path = self.dead_letter_dir / filename
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'error_type': error_type,
                    'timestamp': timestamp,
                    'pipeline_stage': self.state.current_stage,
                    'data': data
                }, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Saved error data to dead letter queue: {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to save to dead letter queue: {e}")
    
    def _create_pipeline_result(self) -> Dict[str, Any]:
        """Create comprehensive pipeline execution result"""
        execution_time = (datetime.now() - self.state.start_time).total_seconds()
        
        return {
            'execution_summary': {
                'status': 'completed' if self.state.current_stage == 'completed' else 'failed',
                'execution_time_seconds': execution_time,
                'current_stage': self.state.current_stage,
                'completed_stages': self.state.completed_stages
            },
            'data_flow': {
                'collected': self.state.collected_count,
                'processed': self.state.processed_count,
                'quality_filtered': self.state.quality_filtered_count,
                'deduplicated': self.state.deduplicated_count,
                'final_output': self.state.final_count
            },
            'efficiency_metrics': {
                'collection_to_final_ratio': (
                    self.state.final_count / self.state.collected_count * 100
                    if self.state.collected_count > 0 else 0
                ),
                'processing_success_rate': (
                    self.state.processed_count / self.state.collected_count * 100
                    if self.state.collected_count > 0 else 0
                ),
                'quality_pass_rate': (
                    self.state.quality_filtered_count / self.state.processed_count * 100
                    if self.state.processed_count > 0 else 0
                )
            },
            'errors': self.state.errors,
            'cache_stats': self.cache.get_stats(),
            'api_stats': {
                'stackoverflow': self.so_collector.get_collection_stats(),
                'reddit': self.reddit_collector.get_collection_stats()
            }
        }
    
    async def run_incremental_collection(self, hours_back: int = 24) -> Dict[str, Any]:
        """Run incremental collection for the last N hours"""
        from_date = datetime.now() - timedelta(hours=hours_back)
        
        logger.info(f"Running incremental collection from {hours_back} hours ago")
        
        return await self.run_full_pipeline(
            from_date=from_date,
            max_pages=10,  # Smaller for incremental
            target_count=100  # Smaller target for incremental
        )