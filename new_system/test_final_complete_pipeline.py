#!/usr/bin/env python3
"""
완전한 Stack Overflow + Reddit 403 우회 파이프라인 최종 테스트
실제 이미지 다운로드 및 AI 처리까지 포함한 완전한 데이터셋 생성
"""
import asyncio
import json
import logging
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from collectors.stackoverflow_collector import StackOverflowCollector
from collectors.reddit_collector import RedditCollector
from processors.image_processor import ImageProcessor
from core.cache import APICache, LocalCache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def extract_images_from_html(html_content: str) -> List[str]:
    """HTML 컨텐츠에서 이미지 URL 추출"""
    image_urls = []
    
    # <img src="..."> 패턴
    img_pattern = r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>'
    img_matches = re.findall(img_pattern, html_content, re.IGNORECASE)
    image_urls.extend(img_matches)
    
    # <a href="..."> 패턴 (이미지 링크)
    link_pattern = r'<a[^>]+href=["\']([^"\']+\.(?:png|jpg|jpeg|gif|webp|svg)(?:\?[^"\']*)?)["\'][^>]*>'
    link_matches = re.findall(link_pattern, html_content, re.IGNORECASE)
    image_urls.extend(link_matches)
    
    # 마크다운 스타일 이미지 링크 ![alt](url)
    markdown_pattern = r'!\[.*?\]\(([^)]+)\)'
    markdown_matches = re.findall(markdown_pattern, html_content)
    image_urls.extend(markdown_matches)
    
    # 직접 URL 패턴 (http://...image.png)
    direct_pattern = r'https?://[^\s<>"\']+\.(?:png|jpg|jpeg|gif|webp|svg)(?:\?[^\s<>"\']*)?'
    direct_matches = re.findall(direct_pattern, html_content, re.IGNORECASE)
    image_urls.extend(direct_matches)
    
    # 중복 제거 및 정리
    unique_urls = list(set(image_urls))
    
    # 빈 URL이나 프로필 이미지 제외
    filtered_urls = []
    for url in unique_urls:
        if url and 'gravatar.com' not in url and len(url.strip()) > 10:
            # 상대 URL을 절대 URL로 변환
            if url.startswith('//'):
                url = 'https:' + url
            elif url.startswith('/'):
                url = 'https://i.sstatic.net' + url
                
            filtered_urls.append(url.strip())
    
    return filtered_urls

def extract_images_from_reddit_data(reddit_data: List) -> List[str]:
    """Reddit 데이터에서 이미지 URL 추출"""
    all_images = []
    
    for item in reddit_data:
        # submission의 이미지들
        if hasattr(item, 'submission'):
            submission = item.submission
            
            # selftext에서 이미지 추출
            if isinstance(submission, dict) and 'selftext' in submission:
                images = extract_images_from_html(submission['selftext'])
                all_images.extend(images)
            
            # solution에서 이미지 추출
            if hasattr(item, 'solution') and isinstance(item.solution, dict):
                solution_body = item.solution.get('body', '')
                images = extract_images_from_html(solution_body)
                all_images.extend(images)
    
    return list(set(all_images))  # 중복 제거

async def test_complete_pipeline():
    """완전한 파이프라인 테스트 실행"""
    
    logger.info("🚀 Complete Pipeline Test - Stack Overflow + Reddit + 403 Bypass")
    logger.info("=" * 80)
    
    # Cache 초기화
    local_cache = LocalCache(db_path=Path("/tmp/complete_pipeline_test.db"))
    cache = APICache(local_cache)
    
    final_dataset = {
        "dataset_info": {
            "name": "Excel Q&A Dataset with 403 Bypass Complete",
            "version": "4.0-complete-pipeline",
            "description": "Complete Excel Q&A dataset with Stack Overflow + Reddit + 403 bypass + AI processing",
            "generated_at": datetime.now().isoformat()
        },
        "collection_results": {
            "stackoverflow": {"data": [], "images": []},
            "reddit": {"data": [], "images": []}
        },
        "image_processing": {
            "total_images": 0,
            "successful_downloads": 0,
            "successful_processing": 0,
            "results": []
        },
        "bypass_effectiveness": {
            "stackoverflow": {"tested": 0, "successful": 0, "rate": 0},
            "reddit": {"tested": 0, "successful": 0, "rate": 0},
            "overall": {"tested": 0, "successful": 0, "rate": 0}
        }
    }
    
    # Phase 1: Stack Overflow 수집
    logger.info("\n📚 Phase 1: Stack Overflow Collection")
    logger.info("-" * 60)
    
    try:
        so_collector = StackOverflowCollector(cache)
        so_data = await so_collector.collect_excel_questions(max_pages=2)  # 더 많은 데이터
        
        final_dataset["collection_results"]["stackoverflow"]["data"] = so_data
        logger.info(f"✅ Stack Overflow: {len(so_data)}개 수집")
        
        # 이미지 추출
        for item in so_data:
            question_body = item.get('body_markdown', '') + ' ' + item.get('body', '')
            question_images = extract_images_from_html(question_body)
            
            if 'accepted_answer' in item:
                answer_body = item['accepted_answer'].get('body', '')
                answer_images = extract_images_from_html(answer_body)
                question_images.extend(answer_images)
            
            final_dataset["collection_results"]["stackoverflow"]["images"].extend(question_images)
        
        # 중복 제거
        final_dataset["collection_results"]["stackoverflow"]["images"] = list(set(
            final_dataset["collection_results"]["stackoverflow"]["images"]
        ))
        
        logger.info(f"   📸 Stack Overflow 이미지: {len(final_dataset['collection_results']['stackoverflow']['images'])}개")
        
    except Exception as e:
        logger.error(f"❌ Stack Overflow 수집 실패: {e}")
    
    # Phase 2: Reddit 수집
    logger.info("\n🟠 Phase 2: Reddit Collection")
    logger.info("-" * 60)
    
    try:
        reddit_collector = RedditCollector(cache)
        reddit_data = await reddit_collector.collect_excel_discussions(max_submissions=10)  # 더 많은 데이터
        
        final_dataset["collection_results"]["reddit"]["data"] = [
            {
                "submission": item.submission,
                "solution": item.solution,
                "metadata": item.metadata
            } for item in reddit_data
        ]
        
        logger.info(f"✅ Reddit: {len(reddit_data)}개 수집")
        
        # Reddit 이미지 추출
        reddit_images = extract_images_from_reddit_data(reddit_data)
        final_dataset["collection_results"]["reddit"]["images"] = reddit_images
        
        logger.info(f"   📸 Reddit 이미지: {len(reddit_images)}개")
        
    except Exception as e:
        logger.error(f"❌ Reddit 수집 실패: {e}")
    
    # Phase 3: 이미지 403 우회 및 AI 처리
    logger.info("\n🖼️  Phase 3: Image Processing with 403 Bypass")
    logger.info("-" * 60)
    
    all_images = (final_dataset["collection_results"]["stackoverflow"]["images"] + 
                 final_dataset["collection_results"]["reddit"]["images"])
    
    # 테스트용 Reddit 이미지 추가
    test_reddit_images = [
        'https://preview.redd.it/76mukstfxhdf1.png'
    ]
    all_images.extend(test_reddit_images)
    
    # 중복 제거
    unique_images = list(set(all_images))
    
    final_dataset["image_processing"]["total_images"] = len(unique_images)
    
    logger.info(f"   🎯 총 처리할 이미지: {len(unique_images)}개")
    
    if unique_images:
        processor = ImageProcessor(cache)
        
        for i, img_url in enumerate(unique_images, 1):
            logger.info(f"   [{i}/{len(unique_images)}] 처리 중: {img_url[:60]}...")
            
            try:
                # 403 우회 + AI 처리
                result = await processor.process_image_url(img_url, ['excel'])
                
                # 소스 판별
                is_reddit = 'redd.it' in img_url or 'reddit' in img_url
                is_stackoverflow = 'sstatic.net' in img_url
                source = 'reddit' if is_reddit else 'stackoverflow'
                
                if result and result.get('success'):
                    final_dataset["image_processing"]["successful_downloads"] += 1
                    
                    if result.get('extracted_content'):
                        final_dataset["image_processing"]["successful_processing"] += 1
                    
                    final_dataset["image_processing"]["results"].append({
                        'url': img_url,
                        'source': source,
                        'success': True,
                        'processing_tier': result.get('processing_tier', ''),
                        'content_length': len(result.get('extracted_content', '')),
                        'extracted_content': result.get('extracted_content', '')[:200] + '...' if len(result.get('extracted_content', '')) > 200 else result.get('extracted_content', '')
                    })
                    
                    logger.info(f"      ✅ 성공! {result.get('processing_tier', 'Unknown')}")
                else:
                    final_dataset["image_processing"]["results"].append({
                        'url': img_url,
                        'source': source,
                        'success': False,
                        'error': result.get('error', 'Unknown') if result else 'No result'
                    })
                    logger.error(f"      ❌ 실패")
                
                # 소스별 통계 업데이트
                if is_stackoverflow:
                    final_dataset["bypass_effectiveness"]["stackoverflow"]["tested"] += 1
                    if result and result.get('success'):
                        final_dataset["bypass_effectiveness"]["stackoverflow"]["successful"] += 1
                elif is_reddit:
                    final_dataset["bypass_effectiveness"]["reddit"]["tested"] += 1
                    if result and result.get('success'):
                        final_dataset["bypass_effectiveness"]["reddit"]["successful"] += 1
                
            except Exception as e:
                logger.error(f"      ❌ 예외: {e}")
                final_dataset["image_processing"]["results"].append({
                    'url': img_url,
                    'source': source,
                    'success': False,
                    'error': str(e)
                })
    
    # Phase 4: 최종 통계 계산
    logger.info("\n📊 Phase 4: Final Statistics")
    logger.info("-" * 60)
    
    # 소스별 성공률 계산
    so_stats = final_dataset["bypass_effectiveness"]["stackoverflow"]
    if so_stats["tested"] > 0:
        so_stats["rate"] = (so_stats["successful"] / so_stats["tested"]) * 100
    
    reddit_stats = final_dataset["bypass_effectiveness"]["reddit"]
    if reddit_stats["tested"] > 0:
        reddit_stats["rate"] = (reddit_stats["successful"] / reddit_stats["tested"]) * 100
    
    # 전체 성공률
    total_tested = so_stats["tested"] + reddit_stats["tested"]
    total_successful = so_stats["successful"] + reddit_stats["successful"]
    
    final_dataset["bypass_effectiveness"]["overall"] = {
        "tested": total_tested,
        "successful": total_successful,
        "rate": (total_successful / total_tested * 100) if total_tested > 0 else 0
    }
    
    # 최종 업데이트
    final_dataset["dataset_info"].update({
        "total_qa_pairs": len(final_dataset["collection_results"]["stackoverflow"]["data"]) + len(final_dataset["collection_results"]["reddit"]["data"]),
        "total_images_found": final_dataset["image_processing"]["total_images"],
        "image_processing_success_rate": (final_dataset["image_processing"]["successful_processing"] / final_dataset["image_processing"]["total_images"] * 100) if final_dataset["image_processing"]["total_images"] > 0 else 0
    })
    
    # 결과 저장
    output_path = "/Users/kevin/bigdata/data/output/complete_pipeline_dataset.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_dataset, f, indent=2, ensure_ascii=False)
    
    # 최종 보고서
    logger.info("\n" + "🎉" * 30)
    logger.info("📋 COMPLETE PIPELINE RESULTS")
    logger.info("=" * 80)
    logger.info(f"💎 데이터셋 생성 완료: {output_path}")
    logger.info("")
    logger.info("📊 수집 통계:")
    logger.info(f"   • 총 Q&A 쌍: {final_dataset['dataset_info']['total_qa_pairs']}개")
    logger.info(f"   • Stack Overflow: {len(final_dataset['collection_results']['stackoverflow']['data'])}개")
    logger.info(f"   • Reddit: {len(final_dataset['collection_results']['reddit']['data'])}개")
    logger.info("")
    logger.info("🖼️  이미지 처리 통계:")
    logger.info(f"   • 총 이미지 발견: {final_dataset['image_processing']['total_images']}개")
    logger.info(f"   • 다운로드 성공: {final_dataset['image_processing']['successful_downloads']}개")
    logger.info(f"   • AI 처리 성공: {final_dataset['image_processing']['successful_processing']}개")
    logger.info(f"   • 전체 처리 성공률: {final_dataset['dataset_info']['image_processing_success_rate']:.1f}%")
    logger.info("")
    logger.info("🎯 403 우회 성과:")
    logger.info(f"   • 전체: {final_dataset['bypass_effectiveness']['overall']['rate']:.1f}% ({final_dataset['bypass_effectiveness']['overall']['successful']}/{final_dataset['bypass_effectiveness']['overall']['tested']})")
    logger.info(f"   • Stack Overflow: {final_dataset['bypass_effectiveness']['stackoverflow']['rate']:.1f}% ({final_dataset['bypass_effectiveness']['stackoverflow']['successful']}/{final_dataset['bypass_effectiveness']['stackoverflow']['tested']})")
    logger.info(f"   • Reddit: {final_dataset['bypass_effectiveness']['reddit']['rate']:.1f}% ({final_dataset['bypass_effectiveness']['reddit']['successful']}/{final_dataset['bypass_effectiveness']['reddit']['tested']})")
    
    if final_dataset['bypass_effectiveness']['overall']['rate'] >= 80:
        logger.info("🏆 403 우회 목표 달성!")
    elif final_dataset['bypass_effectiveness']['overall']['rate'] >= 60:
        logger.info("✅ 403 우회 양호한 성과!")
    else:
        logger.info("⚠️  403 우회 추가 개선 권장")
    
    return final_dataset

if __name__ == "__main__":
    result = asyncio.run(test_complete_pipeline())