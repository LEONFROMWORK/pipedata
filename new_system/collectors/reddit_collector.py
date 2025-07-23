"""
Reddit Data Collector with Thread Analysis
Reddit TRD: Q&A 쌍 식별이 핵심인 파이프라인
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import re

import praw
from prawcore.exceptions import PrawcoreException
import backoff

from core.cache import APICache
from core.dedup_tracker import get_global_tracker
from config import Config
from bot_detection.ultimate_bot_detector import UltimateBotDetector

logger = logging.getLogger('pipeline.reddit_collector')

class RedditAPIError(Exception):
    """Custom exception for Reddit API handling"""
    pass

def is_bot_response(response_text: str) -> bool:
    """
    Reddit 봇 응답 감지 함수 - 강화된 버전
    
    사용자 요청에 따른 봇 응답 필터링 로직
    """
    if not response_text:
        return True
    
    # 🚨 강화된 봇 감지 패턴
    bot_indicators = [
        "Your post was submitted successfully",
        "I am a bot",
        "contact the moderators",
        "/r/excel) if you have any questions",
        "This action was performed automatically",
        "reply to the **answer(s)** saying `Solution Verified`",
        "Follow the **[submission rules]",
        "Include your **[Excel version",
        "Failing to follow these steps may result",
        "Please [contact the moderators",
        "automatically. Please [contact",
        "I am a bot, and this action",
        "performed automatically",
        "moderators of this subreddit",
        "submission rules",
        "Excel version and all other",
        "relevant information",
        "post being removed without warning",
        "submission was submitted successfully",
        "close the thread"
    ]
    
    # 대소문자 구분 없이 검사
    response_lower = response_text.lower()
    for indicator in bot_indicators:
        if indicator.lower() in response_lower:
            logger.info(f"🚨 Bot response detected: '{indicator}' found in text")
            return True
    
    return False

class ThreadAnalysisResult:
    """Result of Reddit thread analysis"""
    def __init__(self, submission_data: Dict[str, Any], solution_comment: Dict[str, Any], 
                 analysis_metadata: Dict[str, Any]):
        self.submission = submission_data
        self.solution = solution_comment
        self.metadata = analysis_metadata

class RedditCollector:
    """
    Reddit data collector implementing Reddit TRD specifications:
    - PRAW-based r/excel subreddit collection
    - Thread analysis for Q&A pair identification
    - OP confirmation detection system
    - Local optimization with SQLite caching
    """
    
    def __init__(self, cache: APICache):
        self.cache = cache
        self.config = Config.REDDIT_CONFIG
        self.rate_config = Config.RATE_LIMITING
        self.dedup_tracker = get_global_tracker()  # 중복 방지 추적기
        
        # Initialize ultimate bot detector (4-layer system)
        self.bot_detector = UltimateBotDetector()
        
        # Initialize PRAW Reddit instance
        self.reddit = praw.Reddit(
            client_id=Config.REDDIT_CLIENT_ID,
            client_secret=Config.REDDIT_CLIENT_SECRET,
            user_agent=Config.REDDIT_USER_AGENT,
            # Read-only mode (no authentication required)
        )
        
        # Thread analysis configuration
        self.confirmation_keywords = self.config['confirmation_keywords']
        self.question_keywords = self.config['question_keywords']
        self.target_flairs = self.config['target_flairs']
        
        # Rate limiting tracking
        self.requests_today = 0
        self.last_request_time = 0
        
        logger.info("RedditCollector initialized with Ultimate Bot Detection System (4-layer)")
    
    async def collect_excel_discussions(self, from_date: Optional[datetime] = None,
                                      max_submissions: int = 100) -> List[ThreadAnalysisResult]:
        """
        Main collection method for Reddit discussions
        
        Args:
            from_date: Start date for collection
            max_submissions: Maximum submissions to analyze
            
        Returns:
            List of successfully analyzed Q&A pairs
        """
        if from_date is None:
            # Default to last 5 years for enhanced data collection (user request)
            from_date = datetime.now() - timedelta(days=365*5)
        
        logger.info(f"Starting Reddit collection from r/{self.config['subreddit']}")
        
        analyzed_pairs = []
        processed_count = 0
        
        # 통계 수집을 위한 변수 추가
        posts_explored = 0  # 탐색한 게시물 수
        collection_start_time = time.time()  # 수집 시작 시간
        quality_distribution = {"high": 0, "medium": 0, "low": 0}  # 품질 분포
        
        try:
            # Get submissions from target subreddit
            subreddit = self.reddit.subreddit(self.config['subreddit'])
            
            # Collect from multiple sorting methods to get both solved and unsolved posts
            submissions_to_process = []
            
            # 스크롤 방식으로 더 많은 게시글을 검색하여 'solved' 포스트 찾기
            search_limit = max_submissions * 20  # 훨씬 더 많이 검색
            checked_count = 0
            
            logger.info(f"🔍 'solved' 플레어 포스트를 찾기 위해 최대 {search_limit}개 게시글 검색 중...")
            
            # 여러 정렬 방법으로 검색 범위 확대
            for sort_method in ['new', 'hot', 'top']:
                if len(submissions_to_process) >= max_submissions:
                    break
                    
                try:
                    if sort_method == 'new':
                        submissions_iter = subreddit.new(limit=search_limit // 3)
                    elif sort_method == 'hot':
                        submissions_iter = subreddit.hot(limit=search_limit // 3)
                    else:  # top
                        submissions_iter = subreddit.top(time_filter='month', limit=search_limit // 3)
                    
                    logger.info(f"   📑 {sort_method} 정렬로 검색 중...")
                    
                    for submission in submissions_iter:
                        checked_count += 1
                        posts_explored += 1  # 탐색한 게시물 수 증가
                        
                        # 디버깅: 첫 20개 포스트의 플레어 출력
                        if checked_count <= 20:
                            flair = submission.link_flair_text or 'None'
                            logger.info(f"   🔍 [{checked_count}] 플레어: '{flair}' - {submission.title[:40]}...")
                        
                        # 주기적으로 진행 상황 로그
                        if checked_count % 100 == 0:
                            logger.info(f"   🔍 {checked_count}개 검사, {len(submissions_to_process)}개 'solved' 발견")
                        
                        # 'solved' 플레어 확인
                        if submission.link_flair_text and submission.link_flair_text.lower() == 'solved':
                            # 중복 체크: 이미 수집된 서브미션 제외
                            if not self.dedup_tracker.is_reddit_submission_collected(submission.id):
                                submissions_to_process.append(submission)
                                logger.info(f"   ✅ 'solved' 포스트 발견: {submission.title[:50]}... (ID: {submission.id})")
                            else:
                                logger.info(f"   🔄 이미 수집된 'solved' 포스트 건너뜀: {submission.id}")
                        
                        # 충분한 수집이면 중단
                        if len(submissions_to_process) >= max_submissions:
                            break
                        
                        # 너무 많이 검색하면 중단 (API 제한 방지)
                        if checked_count >= search_limit:
                            break
                    
                except Exception as e:
                    logger.warning(f"   ⚠️ {sort_method} 정렬 검색 중 오류: {e}")
                    continue
            
            logger.info(f"Found {len(submissions_to_process)} new 'solved' submissions to process")
            
            # Process submissions with priority to solved posts
            for submission in submissions_to_process:
                try:
                    # Convert timestamp and check date filter
                    submission_date = datetime.fromtimestamp(submission.created_utc)
                    if submission_date < from_date:
                        continue
                    
                    # Apply submission filter
                    if not self._passes_submission_filter(submission):
                        continue
                    
                    # Perform thread analysis
                    analysis_result = await self._analyze_thread(submission)
                    
                    if analysis_result:
                        analyzed_pairs.append(analysis_result)
                        
                        # 품질 점수에 따른 분류
                        quality_score = analysis_result.metadata.get('overall_score', 0.0)
                        if quality_score >= 8.0:
                            quality_distribution["high"] += 1
                        elif quality_score >= 5.0:
                            quality_distribution["medium"] += 1
                        else:
                            quality_distribution["low"] += 1
                        
                        # 수집된 서브미션을 중복 추적기에 등록
                        self.dedup_tracker.mark_reddit_collected(
                            submission.id, 
                            submission.title,
                            quality_score=quality_score,
                            metadata={
                                'flair': submission.link_flair_text,
                                'collection_date': datetime.now().isoformat(),
                                'score': submission.score,
                                'upvote_ratio': submission.upvote_ratio
                            }
                        )
                        
                        logger.info(f"Successfully analyzed thread: {submission.id} (Quality: {quality_score:.1f})")
                    
                    processed_count += 1
                    
                    if len(analyzed_pairs) >= max_submissions:
                        break
                    
                    # Rate limiting
                    await self._rate_limit_delay()
                    
                except Exception as e:
                    logger.error(f"Error processing submission {submission.id}: {e}")
                    continue
        
        except PrawcoreException as e:
            logger.error(f"Reddit API error: {e}")
            raise RedditAPIError(f"Reddit API failed: {e}")
        
        # 수집 시간 계산
        collection_time = time.time() - collection_start_time
        
        # 상세 통계 로깅
        logger.info(f"Reddit collection complete:")
        logger.info(f"  - 탐색한 게시물: {posts_explored}개")
        logger.info(f"  - 처리한 게시물: {processed_count}개")
        logger.info(f"  - 수집된 Q&A: {len(analyzed_pairs)}개")
        logger.info(f"  - 품질 분포: 상={quality_distribution['high']}, 중={quality_distribution['medium']}, 하={quality_distribution['low']}")
        logger.info(f"  - 수집 시간: {collection_time:.1f}초")
        
        # 데이터베이스에 수집 통계 저장
        collection_metadata = {
            'posts_explored': posts_explored,
            'posts_processed': processed_count,
            'quality_distribution': quality_distribution,
            'collection_time_seconds': collection_time,
            'timestamp': datetime.now().isoformat()
        }
        
        # 스킵된 항목 수 계산 (탐색했지만 처리하지 않은 게시물)
        items_skipped = posts_explored - processed_count
        
        self.dedup_tracker.record_collection_stats(
            source='reddit',
            items_collected=len(analyzed_pairs),
            items_skipped=items_skipped,
            metadata=collection_metadata
        )
        
        # 통계를 메타데이터로 추가 (각 결과에 전체 통계 포함)
        for result in analyzed_pairs:
            result.metadata['collection_stats'] = {
                'posts_explored': posts_explored,
                'items_collected': len(analyzed_pairs),
                'quality_distribution': quality_distribution,
                'collection_time_seconds': collection_time
            }
        
        return analyzed_pairs
    
    def _passes_submission_filter(self, submission) -> bool:
        """
        Apply TRD submission filtering criteria
        
        Filters (AND logic):
        1. Flair check: "Question", "Unsolved", "Discussion"
        2. Title keywords: '?', 'how to', 'issue', 'error', 'help'
        3. Upvote ratio: >= 0.7 (non-controversial)
        """
        try:
            # Check flair
            flair_text = getattr(submission, 'link_flair_text', None)
            if flair_text and flair_text in self.target_flairs:
                flair_match = True
            else:
                flair_match = False
            
            # Check title for question indicators
            title_lower = submission.title.lower()
            title_match = any(keyword in title_lower for keyword in self.question_keywords)
            
            # Check upvote ratio (avoid controversial posts)
            upvote_ratio = getattr(submission, 'upvote_ratio', 0)
            ratio_match = upvote_ratio >= self.config['upvote_ratio_threshold']
            
            # Must pass at least 2 out of 3 criteria (flexible filtering)
            criteria_passed = sum([flair_match, title_match, ratio_match])
            
            if criteria_passed >= 2:
                logger.debug(f"Submission {submission.id} passed filter: "
                           f"flair={flair_match}, title={title_match}, ratio={ratio_match}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error in submission filter for {submission.id}: {e}")
            return False
    
    async def _analyze_thread(self, submission) -> Optional[ThreadAnalysisResult]:
        """
        Core thread analysis following Reddit TRD specifications
        
        Process:
        1. Traverse comment tree
        2. Identify solution candidates (Top-voted + OP-confirmed)
        3. Apply priority logic for final selection
        
        Returns ThreadAnalysisResult or None if no valid Q&A pair found
        """
        # Check cache first
        cache_key = f"reddit_thread_{submission.id}"
        cached_result = self.cache.get_stackoverflow_response('reddit_thread', {'id': submission.id})
        
        if cached_result:
            logger.info(f"Using cached thread analysis for {submission.id}")
            return self._deserialize_analysis_result(cached_result)
        
        try:
            # Force comment loading
            submission.comments.replace_more(limit=0)  # Remove "load more" comments
            
            # Extract submission data
            submission_data = self._extract_submission_data(submission)
            
            # Find solution candidates with multi-tier evaluation
            solution_verified_candidate = None  # 1st priority: "Solution Verified" 
            op_confirmed_candidate = None       # 2nd priority: OP confirmation
            quality_candidate = None           # 3rd priority: High quality answer
            top_voted_candidate = None         # 4th priority: Top voted
            
            max_score = -1
            
            # Traverse all comments
            for comment in submission.comments.list():
                try:
                    # Skip deleted/removed comments
                    if not hasattr(comment, 'body') or comment.body in ['[deleted]', '[removed]']:
                        continue
                    
                    # Top-voted candidate (highest score top-level comment)
                    if comment.score > max_score and comment.is_root:
                        max_score = comment.score
                        top_voted_candidate = comment
                    
                    # Check for "Solution Verified" (highest priority)
                    if self._is_solution_verified(comment, submission.author):
                        parent_comment = self._get_parent_comment(comment)
                        if parent_comment and parent_comment.score > 0:
                            solution_verified_candidate = parent_comment
                            logger.debug(f"Found 'Solution Verified' for comment {parent_comment.id}")
                            break  # Solution Verified is highest priority
                    
                    # OP-confirmed candidate detection (2nd priority)
                    elif self._is_op_confirmation(comment, submission.author):
                        parent_comment = self._get_parent_comment(comment)
                        if parent_comment and parent_comment.score > 0:
                            op_confirmed_candidate = parent_comment
                            logger.debug(f"Found OP confirmation for comment {parent_comment.id}")
                    
                    # Quality candidate detection (3rd priority)
                    elif self._is_quality_answer(comment):
                        if not quality_candidate or comment.score > quality_candidate.score:
                            quality_candidate = comment
                            logger.debug(f"Found quality answer candidate {comment.id}")
                
                except Exception as e:
                    logger.debug(f"Error processing comment: {e}")
                    continue
            
            # Apply multi-tier solution selection priority with advanced bot filtering
            final_solution = None
            solution_type = None
            
            # 🚨 봇 응답 완전 차단: 모든 후보를 Ultimate Bot Detection System으로 재검증
            if solution_verified_candidate:
                bot_result = await self.bot_detector.detect_bot_ultimate(
                    content=solution_verified_candidate.body,
                    metadata={
                        'author': str(solution_verified_candidate.author),
                        'score': solution_verified_candidate.score,
                        'created_utc': solution_verified_candidate.created_utc
                    },
                    client_ip=f"reddit_collector_{submission.id}"
                )
                if not bot_result.is_bot:
                    final_solution = solution_verified_candidate
                    solution_type = "solution_verified"
                    logger.info(f"✅ Selected 'Solution Verified' answer for {submission.id} (Ultimate confidence: {bot_result.confidence:.3f})")
                else:
                    logger.warning(f"🚨 Solution verified candidate is bot response ({bot_result.detection_type.value}, confidence: {bot_result.confidence:.3f}), filtered out: {submission.id}")
            
            elif op_confirmed_candidate:
                bot_result = await self.bot_detector.detect_bot_ultimate(
                    content=op_confirmed_candidate.body,
                    metadata={
                        'author': str(op_confirmed_candidate.author),
                        'score': op_confirmed_candidate.score,
                        'created_utc': op_confirmed_candidate.created_utc
                    },
                    client_ip=f"reddit_collector_{submission.id}"
                )
                if not bot_result.is_bot:
                    final_solution = op_confirmed_candidate
                    solution_type = "op_confirmed"
                    logger.info(f"✅ Selected OP-confirmed solution for {submission.id} (Ultimate confidence: {bot_result.confidence:.3f})")
                else:
                    logger.warning(f"🚨 OP confirmed candidate is bot response ({bot_result.detection_type.value}, confidence: {bot_result.confidence:.3f}), filtered out: {submission.id}")
            
            elif (quality_candidate and 
                  quality_candidate.score >= self.config['solution_score_threshold']):
                bot_result = await self.bot_detector.detect_bot_ultimate(
                    content=quality_candidate.body,
                    metadata={
                        'author': str(quality_candidate.author),
                        'score': quality_candidate.score,
                        'created_utc': quality_candidate.created_utc
                    },
                    client_ip=f"reddit_collector_{submission.id}"
                )
                if not bot_result.is_bot:
                    final_solution = quality_candidate
                    solution_type = "quality_answer"
                    logger.info(f"✅ Selected quality answer for {submission.id} (Ultimate confidence: {bot_result.confidence:.3f})")
                else:
                    logger.warning(f"🚨 Quality candidate is bot response ({bot_result.detection_type.value}, confidence: {bot_result.confidence:.3f}), filtered out: {submission.id}")
            
            elif (top_voted_candidate and 
                  top_voted_candidate.score >= self.config['solution_score_threshold']):
                bot_result = await self.bot_detector.detect_bot_ultimate(
                    content=top_voted_candidate.body,
                    metadata={
                        'author': str(top_voted_candidate.author),
                        'score': top_voted_candidate.score,
                        'created_utc': top_voted_candidate.created_utc
                    },
                    client_ip=f"reddit_collector_{submission.id}"
                )
                if not bot_result.is_bot:
                    final_solution = top_voted_candidate
                    solution_type = "top_voted"
                    logger.info(f"✅ Selected top-voted solution for {submission.id} (Ultimate confidence: {bot_result.confidence:.3f})")
                else:
                    logger.warning(f"🚨 Top voted candidate is bot response ({bot_result.detection_type.value}, confidence: {bot_result.confidence:.3f}), filtered out: {submission.id}")
            
            if not final_solution:
                logger.warning(f"🚨 No valid non-bot solution found for submission {submission.id}")
                return None
            
            # Extract solution data
            solution_data = self._extract_comment_data(final_solution)
            
            # Create analysis metadata with bot detection stats
            analysis_metadata = {
                'solution_type': solution_type,
                'total_comments': len(submission.comments.list()),
                'max_comment_score': max_score,
                'op_confirmed': solution_type == "op_confirmed",
                'analysis_timestamp': datetime.now().isoformat(),
                'bot_detection_version': '4.0-ultimate',
                'bot_detection_layers': ['layer1', 'layer2', 'layer3', 'layer4']
            }
            
            result = ThreadAnalysisResult(submission_data, solution_data, analysis_metadata)
            
            # Cache the result
            self.cache.cache_stackoverflow_response(
                'reddit_thread',
                {'id': submission.id},
                self._serialize_analysis_result(result)
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Thread analysis failed for {submission.id}: {e}")
            return None
    
    def _is_solution_verified(self, comment, submission_author) -> bool:
        """
        Detect if comment contains "Solution Verified" (highest priority)
        
        Criteria:
        1. Comment author is submission author (is_submitter equivalent)
        2. Comment contains "Solution Verified" keyword
        """
        try:
            # Check if comment author is the original poster
            if comment.author != submission_author:
                return False
            
            # Check specifically for "Solution Verified"
            comment_text = comment.body.lower()
            
            if 'solution verified' in comment_text:
                logger.debug(f"Found 'Solution Verified' in comment {comment.id}")
                return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Error in solution verified check: {e}")
            return False
    
    def _is_op_confirmation(self, comment, submission_author) -> bool:
        """
        Detect if comment is OP confirmation using TRD criteria
        
        Criteria:
        1. Comment author is submission author (is_submitter equivalent)
        2. Comment contains positive feedback keywords
        """
        try:
            # Check if comment author is the original poster
            if comment.author != submission_author:
                return False
            
            # Check for confirmation keywords (excluding "Solution Verified")
            comment_text = comment.body.lower()
            
            for keyword in self.confirmation_keywords:
                if keyword.lower() != 'solution verified' and keyword.lower() in comment_text:
                    logger.debug(f"Found confirmation keyword '{keyword}' in comment {comment.id}")
                    return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Error in OP confirmation check: {e}")
            return False
    
    def _is_quality_answer(self, comment) -> bool:
        """
        Detect if comment is a quality answer based on content analysis
        
        Criteria:
        1. NOT a bot response (사용자 요청에 따른 필터링)
        2. Contains code blocks or formulas
        3. Has detailed explanation (> 100 characters)
        4. Has structured content (multiple lines)
        5. Contains Excel-specific terms
        """
        try:
            if not hasattr(comment, 'body') or not comment.body:
                return False
            
            comment_text = comment.body
            
            # 🚨 첫 번째 체크: Ultimate Bot Detection System 필터링 (사용자 요청에 따른 추가)
            # Note: We need to use the simpler layer 1 detection here for performance in quality checking
            # The full Ultimate detection is used later for final candidate verification
            from bot_detection.advanced_bot_detector import AdvancedBotDetector
            simple_bot_detector = AdvancedBotDetector()
            bot_result = simple_bot_detector.detect_bot_comprehensive({
                'body': comment_text,
                'author': str(comment.author) if comment.author else '[deleted]',
                'score': comment.score,
                'created_utc': comment.created_utc
            })
            if bot_result.is_bot:
                logger.debug(f"Filtering out bot response in comment {comment.id} (confidence: {bot_result.confidence:.2f})")
                return False
            
            # Check for code blocks or formulas
            has_code = ('```' in comment_text or 
                       '=' in comment_text and len([line for line in comment_text.split('\n') if line.strip().startswith('=')]) > 0 or
                       'VLOOKUP' in comment_text.upper() or 
                       'INDEX' in comment_text.upper() or
                       'MATCH' in comment_text.upper())
            
            # Check for detailed explanation
            has_detail = len(comment_text.strip()) > 100
            
            # Check for structured content
            has_structure = len(comment_text.split('\n')) > 2
            
            # Check for Excel-specific terms
            excel_terms = ['formula', 'cell', 'column', 'row', 'sheet', 'pivot', 'vlookup', 'index', 'match']
            has_excel_terms = any(term in comment_text.lower() for term in excel_terms)
            
            # Must have at least 2 out of 4 criteria
            quality_score = sum([has_code, has_detail, has_structure, has_excel_terms])
            
            if quality_score >= 2:
                logger.debug(f"Quality answer detected in comment {comment.id}: "
                           f"code={has_code}, detail={has_detail}, structure={has_structure}, excel={has_excel_terms}")
                return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Error in quality answer check: {e}")
            return False
    
    def _get_parent_comment(self, comment):
        """Get parent comment from comment object"""
        try:
            if hasattr(comment, 'parent') and comment.parent:
                parent = comment.parent()
                # Make sure it's actually a comment, not the submission
                if hasattr(parent, 'body'):
                    return parent
            return None
        except:
            return None
    
    def _extract_image_urls_from_submission(self, submission) -> List[str]:
        """
        Extract all possible image URLs from Reddit submission
        Priority: i.redd.it > external links > preview.redd.it (last resort)
        """
        image_urls = []
        
        try:
            # Method 1: Check if submission URL is direct image
            if submission.url and self._is_image_url(submission.url):
                # Prefer i.redd.it over preview.redd.it
                if 'i.redd.it' in submission.url:
                    image_urls.append(submission.url)
                elif 'preview.redd.it' not in submission.url:
                    # External image (imgur, etc.) - usually accessible
                    image_urls.append(submission.url)
            
            # Method 2: Check submission gallery/media
            if hasattr(submission, 'is_gallery') and submission.is_gallery:
                if hasattr(submission, 'media_metadata'):
                    for item_id, media_item in submission.media_metadata.items():
                        if 's' in media_item and 'u' in media_item['s']:
                            # Get the highest resolution image
                            original_url = media_item['s']['u'].replace('&amp;', '&')
                            if 'i.redd.it' in original_url:
                                image_urls.append(original_url)
            
            # Method 3: Check post media (single image posts)
            if hasattr(submission, 'post_hint') and submission.post_hint == 'image':
                if hasattr(submission, 'preview') and 'images' in submission.preview:
                    for image in submission.preview['images']:
                        if 'source' in image:
                            source_url = image['source']['url'].replace('&amp;', '&')
                            # Convert preview.redd.it to i.redd.it if possible
                            if 'preview.redd.it' in source_url:
                                original_url = source_url.replace('preview.redd.it', 'i.redd.it')
                                # Remove size parameters to get original
                                if '?' in original_url:
                                    original_url = original_url.split('?')[0]
                                image_urls.append(original_url)
                            else:
                                image_urls.append(source_url)
            
            # Method 4: Parse selftext for image URLs
            if submission.selftext:
                selftext_images = self._extract_images_from_text(submission.selftext)
                image_urls.extend(selftext_images)
            
            # Method 5: Last resort - use preview.redd.it but mark as low priority
            if not image_urls and submission.url and 'preview.redd.it' in submission.url:
                image_urls.append(submission.url)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_urls = []
            for url in image_urls:
                if url not in seen:
                    seen.add(url)
                    unique_urls.append(url)
            
            logger.debug(f"Extracted {len(unique_urls)} image URLs from submission {submission.id}")
            return unique_urls
            
        except Exception as e:
            logger.warning(f"Error extracting images from submission {submission.id}: {e}")
            return []
    
    def _is_image_url(self, url: str) -> bool:
        """Check if URL points to an image"""
        if not url:
            return False
        
        image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'}
        url_lower = url.lower()
        
        # Direct extension check
        if any(url_lower.endswith(ext) for ext in image_extensions):
            return True
        
        # Reddit image domains
        image_domains = {'i.redd.it', 'preview.redd.it', 'i.imgur.com', 'imgur.com'}
        for domain in image_domains:
            if domain in url_lower:
                return True
        
        return False
    
    def _extract_images_from_text(self, text: str) -> List[str]:
        """Extract image URLs from markdown text"""
        import re
        
        image_urls = []
        
        # Markdown image pattern: ![alt](url)
        markdown_pattern = r'!\[.*?\]\((https?://[^\s\)]+)\)'
        matches = re.findall(markdown_pattern, text)
        for url in matches:
            if self._is_image_url(url):
                image_urls.append(url)
        
        # Direct URL pattern
        url_pattern = r'https?://(?:i\.redd\.it|preview\.redd\.it|i\.imgur\.com|imgur\.com)/[^\s]+'
        matches = re.findall(url_pattern, text)
        for url in matches:
            if self._is_image_url(url):
                image_urls.append(url)
        
        return image_urls
    
    def _extract_submission_data(self, submission) -> Dict[str, Any]:
        """Extract structured data from Reddit submission"""
        # Extract all possible image URLs
        image_urls = self._extract_image_urls_from_submission(submission)
        
        return {
            'id': submission.id,
            'title': submission.title,
            'selftext': submission.selftext,
            'score': submission.score,
            'upvote_ratio': submission.upvote_ratio,
            'num_comments': submission.num_comments,
            'author': str(submission.author) if submission.author else '[deleted]',
            'link_flair_text': submission.link_flair_text,
            'created_utc': submission.created_utc,
            'url': submission.url,
            'permalink': submission.permalink,
            'image_urls': image_urls,
            'has_images': len(image_urls) > 0
        }
    
    def _extract_comment_data(self, comment) -> Dict[str, Any]:
        """Extract structured data from Reddit comment"""
        return {
            'id': comment.id,
            'body': comment.body,
            'score': comment.score,
            'author': str(comment.author) if comment.author else '[deleted]',
            'created_utc': comment.created_utc,
            'permalink': comment.permalink,
            'parent_id': comment.parent_id,
            'is_root': comment.is_root
        }
    
    def _serialize_analysis_result(self, result: ThreadAnalysisResult) -> Dict[str, Any]:
        """Serialize ThreadAnalysisResult for caching"""
        return {
            'submission': result.submission,
            'solution': result.solution,
            'metadata': result.metadata
        }
    
    def _deserialize_analysis_result(self, data: Dict[str, Any]) -> ThreadAnalysisResult:
        """Deserialize cached data to ThreadAnalysisResult"""
        return ThreadAnalysisResult(
            data['submission'],
            data['solution'],
            data['metadata']
        )
    
    async def _rate_limit_delay(self) -> None:
        """Simple rate limiting to be respectful to Reddit API"""
        min_delay = 1.0  # 1 second between requests
        elapsed = time.time() - self.last_request_time
        
        if elapsed < min_delay:
            delay = min_delay - elapsed
            logger.debug(f"Rate limiting delay: {delay:.2f}s")
            await asyncio.sleep(delay)
        
        self.last_request_time = time.time()
        self.requests_today += 1
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get collection statistics including Ultimate Bot Detection metrics"""
        bot_stats = self.bot_detector.get_system_status()
        
        return {
            'requests_today': self.requests_today,
            'last_request': datetime.fromtimestamp(self.last_request_time) if self.last_request_time else None,
            'target_subreddit': self.config['subreddit'],
            'confirmation_keywords': len(self.confirmation_keywords),
            'quality_threshold': self.config['solution_score_threshold'],
            'bot_detection_system': bot_stats['system_name'],
            'bot_detection_version': bot_stats['version'],
            'bot_detection_accuracy': bot_stats['target_accuracy'],
            'bot_detection_layers': len(bot_stats['layers_active']),
            'bot_detection_stats': bot_stats['detection_stats']
        }