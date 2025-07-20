#!/usr/bin/env python3
"""
403 우회 성능 테스트만 - 다운로드 성공률 측정
"""
import asyncio
import json
import logging
import re
from pathlib import Path
from typing import List

# Add project root to path
import sys
sys.path.append(str(Path(__file__).parent))

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

async def test_download_only():
    """403 우회 다운로드 성공률만 테스트"""
    
    logger.info("🚀 403 우회 다운로드 성공률 테스트")
    logger.info("=" * 70)
    
    # 이전 수집 결과 로드
    result_path = "/Users/kevin/bigdata/data/output/bypass_test_results.json"
    
    try:
        with open(result_path, 'r', encoding='utf-8') as f:
            test_data = json.load(f)
    except FileNotFoundError:
        logger.error("❌ 이전 수집 결과를 찾을 수 없습니다.")
        return
    
    # Stack Overflow 데이터에서 이미지 추출
    so_data = test_data.get('detailed_results', {}).get('stackoverflow', {}).get('data', [])
    all_images = []
    
    logger.info(f"📚 Stack Overflow 데이터에서 이미지 추출...")
    
    for item in so_data:
        # 질문 본문에서 이미지 추출
        question_body = item.get('body_markdown', '') + ' ' + item.get('body', '')
        question_images = extract_images_from_html(question_body)
        
        # 답변에서 이미지 추출
        answer_images = []
        if 'accepted_answer' in item:
            answer_body = item['accepted_answer'].get('body', '')
            answer_images = extract_images_from_html(answer_body)
        
        total_images = question_images + answer_images
        all_images.extend(total_images)
    
    # 중복 제거
    unique_images = list(set(all_images))
    
    # Reddit 테스트 이미지 추가
    reddit_test_urls = [
        'https://preview.redd.it/76mukstfxhdf1.png',  # 알려진 작동 URL
    ]
    
    all_test_images = unique_images + reddit_test_urls
    
    logger.info(f"🎯 테스트할 이미지: {len(all_test_images)}개")
    logger.info(f"   • Stack Overflow: {len(unique_images)}개")
    logger.info(f"   • Reddit: {len(reddit_test_urls)}개")
    
    # Cache 및 processor 초기화
    local_cache = LocalCache(db_path=Path("/tmp/bypass_test.db"))
    cache = APICache(local_cache)
    processor = ImageProcessor(cache)
    
    download_results = []
    stackoverflow_success = 0
    reddit_success = 0
    
    for i, img_url in enumerate(all_test_images, 1):
        logger.info(f"[{i}/{len(all_test_images)}] 다운로드 테스트: {img_url}")
        
        try:
            # 다운로드만 테스트 (처리는 건너뛰기)
            downloaded_path = await processor._download_image(img_url)
            
            if downloaded_path and Path(downloaded_path).exists():
                file_size = Path(downloaded_path).stat().st_size
                
                # 소스 판별
                is_reddit = 'redd.it' in img_url or 'reddit' in img_url
                is_stackoverflow = 'sstatic.net' in img_url
                
                if is_stackoverflow:
                    stackoverflow_success += 1
                elif is_reddit:
                    reddit_success += 1
                
                download_results.append({
                    'url': img_url,
                    'success': True,
                    'file_size': file_size,
                    'source': 'reddit' if is_reddit else 'stackoverflow'
                })
                
                logger.info(f"   ✅ 성공! 파일 크기: {file_size:,} bytes")
                
                # 임시 파일 정리
                if Path(downloaded_path).exists():
                    Path(downloaded_path).unlink()
            else:
                download_results.append({
                    'url': img_url,
                    'success': False,
                    'error': 'Download failed',
                    'source': 'reddit' if 'redd.it' in img_url else 'stackoverflow'
                })
                logger.error(f"   ❌ 다운로드 실패")
                
        except Exception as e:
            download_results.append({
                'url': img_url,
                'success': False,
                'error': str(e),
                'source': 'reddit' if 'redd.it' in img_url else 'stackoverflow'
            })
            logger.error(f"   ❌ 예외: {e}")
        
        logger.info("-" * 40)
    
    # 통계 계산
    total_success = sum(1 for r in download_results if r['success'])
    total_tested = len(download_results)
    overall_success_rate = (total_success / total_tested * 100) if total_tested > 0 else 0
    
    so_total = len(unique_images)
    so_rate = (stackoverflow_success / so_total * 100) if so_total > 0 else 0
    
    reddit_total = len(reddit_test_urls)
    reddit_rate = (reddit_success / reddit_total * 100) if reddit_total > 0 else 0
    
    # 결과 정리
    final_report = {
        "bypass_test_summary": {
            "total_images_tested": total_tested,
            "total_downloads_successful": total_success,
            "overall_success_rate": overall_success_rate,
            "stackoverflow_results": {
                "images_tested": so_total,
                "images_successful": stackoverflow_success,
                "success_rate": so_rate
            },
            "reddit_results": {
                "images_tested": reddit_total,
                "images_successful": reddit_success,
                "success_rate": reddit_rate
            }
        },
        "download_details": download_results
    }
    
    # 결과 저장
    output_path = "/Users/kevin/bigdata/data/output/bypass_effectiveness_test.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_report, f, indent=2, ensure_ascii=False)
    
    # 최종 보고서
    logger.info("=" * 70)
    logger.info("📊 403 우회 다운로드 성공률 최종 결과:")
    logger.info(f"   • 총 테스트: {final_report['bypass_test_summary']['total_images_tested']}개")
    logger.info(f"   • 성공한 다운로드: {final_report['bypass_test_summary']['total_downloads_successful']}개")
    logger.info(f"   • 전체 성공률: {final_report['bypass_test_summary']['overall_success_rate']:.1f}%")
    
    logger.info(f"\n🎯 소스별 403 우회 성과:")
    logger.info(f"   📚 Stack Overflow: {final_report['bypass_test_summary']['stackoverflow_results']['success_rate']:.1f}% ({final_report['bypass_test_summary']['stackoverflow_results']['images_successful']}/{final_report['bypass_test_summary']['stackoverflow_results']['images_tested']})")
    logger.info(f"   🟠 Reddit: {final_report['bypass_test_summary']['reddit_results']['success_rate']:.1f}% ({final_report['bypass_test_summary']['reddit_results']['images_successful']}/{final_report['bypass_test_summary']['reddit_results']['images_tested']})")
    
    logger.info(f"\n💾 상세 결과 저장: {output_path}")
    
    if overall_success_rate >= 80:
        logger.info("🎉 403 우회 성공! 목표 달성!")
    elif overall_success_rate >= 60:
        logger.info("✅ 403 우회 양호한 성과!")
    elif overall_success_rate >= 40:
        logger.warning("⚠️  403 우회 부분적 성공")
    else:
        logger.error("❌ 403 우회 개선 필요")
    
    return final_report

if __name__ == "__main__":
    result = asyncio.run(test_download_only())