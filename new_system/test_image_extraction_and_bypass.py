#!/usr/bin/env python3
"""
이미지 추출 및 403 우회 실제 테스트
Stack Overflow 수집 데이터에서 이미지를 추출하고 403 우회 적용 테스트
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

async def test_image_extraction_and_bypass():
    """실제 데이터에서 이미지 추출 및 403 우회 테스트"""
    
    logger.info("🚀 이미지 추출 및 403 우회 테스트")
    logger.info("=" * 70)
    
    # 이전 수집 결과 로드
    result_path = "/Users/kevin/bigdata/data/output/bypass_test_results.json"
    
    try:
        with open(result_path, 'r', encoding='utf-8') as f:
            test_data = json.load(f)
    except FileNotFoundError:
        logger.error("❌ 이전 수집 결과를 찾을 수 없습니다. 먼저 simple_bypass_test.py를 실행하세요.")
        return
    
    # Stack Overflow 데이터에서 이미지 추출
    so_data = test_data.get('detailed_results', {}).get('stackoverflow', {}).get('data', [])
    all_images = []
    
    logger.info(f"📚 Stack Overflow 데이터 {len(so_data)}개에서 이미지 추출 중...")
    
    for i, item in enumerate(so_data, 1):
        logger.info(f"[{i}/{len(so_data)}] 질문 분석: {item.get('title', 'Unknown')[:60]}...")
        
        # 질문 본문에서 이미지 추출
        question_body = item.get('body_markdown', '') + ' ' + item.get('body', '')
        question_images = extract_images_from_html(question_body)
        
        # 답변에서 이미지 추출
        answer_images = []
        if 'accepted_answer' in item:
            answer_body = item['accepted_answer'].get('body', '')
            answer_images = extract_images_from_html(answer_body)
        
        total_images = question_images + answer_images
        
        if total_images:
            logger.info(f"   📸 발견된 이미지: {len(total_images)}개")
            for img_url in total_images:
                logger.info(f"      • {img_url}")
                all_images.append({
                    'url': img_url,
                    'source': 'stackoverflow',
                    'question_id': item.get('question_id'),
                    'title': item.get('title', '')
                })
        else:
            logger.info("   📸 이미지 없음")
    
    logger.info(f"\n🎯 총 추출된 이미지: {len(all_images)}개")
    
    if not all_images:
        logger.warning("⚠️  테스트할 이미지가 없습니다.")
        return
    
    # 이미지 처리 및 403 우회 테스트
    logger.info("\n🖼️  403 우회 테스트 시작...")
    logger.info("-" * 70)
    
    # Cache 및 processor 초기화
    local_cache = LocalCache(db_path=Path("/tmp/image_extraction_test.db"))
    cache = APICache(local_cache)
    processor = ImageProcessor(cache)
    
    success_count = 0
    results = []
    
    for i, img_info in enumerate(all_images, 1):
        img_url = img_info['url']
        logger.info(f"[{i}/{len(all_images)}] 이미지 테스트: {img_url}")
        
        try:
            # 이미지 다운로드 및 처리
            result = await processor.process_image_url(img_url, ['excel'])
            
            if result and result.get('success'):
                success_count += 1
                results.append({
                    'url': img_url,
                    'success': True,
                    'processing_tier': result.get('processing_tier', ''),
                    'content_length': len(result.get('extracted_content', '')),
                    'source_info': img_info
                })
                
                logger.info(f"   ✅ 성공! 처리 방법: {result.get('processing_tier', 'Unknown')}")
                if result.get('extracted_content'):
                    preview = result['extracted_content'][:80].replace('\n', ' ')
                    logger.info(f"   📝 추출 내용: {preview}...")
            else:
                results.append({
                    'url': img_url,
                    'success': False,
                    'error': result.get('error', 'Unknown'),
                    'source_info': img_info
                })
                logger.error(f"   ❌ 실패: {result.get('error', 'Unknown')}")
                
        except Exception as e:
            logger.error(f"   ❌ 예외: {e}")
            results.append({
                'url': img_url,
                'success': False,
                'error': str(e),
                'source_info': img_info
            })
        
        logger.info("-" * 40)
    
    # 결과 정리
    success_rate = (success_count / len(all_images) * 100) if all_images else 0
    
    # Stack Overflow 이미지별 성공률
    so_success = sum(1 for r in results if r.get('success', False))
    so_total = len(results)
    
    final_report = {
        "image_extraction_results": {
            "total_images_found": len(all_images),
            "total_tested": so_total,
            "successful_downloads": so_success,
            "success_rate": success_rate,
            "stackoverflow_effectiveness": {
                "images_tested": so_total,
                "images_successful": so_success,
                "success_rate": (so_success / so_total * 100) if so_total > 0 else 0
            }
        },
        "detailed_results": results,
        "image_sources": all_images
    }
    
    # 결과 저장
    output_path = "/Users/kevin/bigdata/data/output/image_bypass_test_results.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_report, f, indent=2, ensure_ascii=False)
    
    # 최종 보고서
    logger.info("=" * 70)
    logger.info("📊 이미지 추출 및 403 우회 최종 결과:")
    logger.info(f"   • 총 이미지 발견: {final_report['image_extraction_results']['total_images_found']}개")
    logger.info(f"   • 테스트된 이미지: {final_report['image_extraction_results']['total_tested']}개")
    logger.info(f"   • 성공한 다운로드: {final_report['image_extraction_results']['successful_downloads']}개")
    logger.info(f"   • 전체 성공률: {final_report['image_extraction_results']['success_rate']:.1f}%")
    
    logger.info(f"\n🎯 Stack Overflow 403 우회 성과:")
    logger.info(f"   📚 성공률: {final_report['image_extraction_results']['stackoverflow_effectiveness']['success_rate']:.1f}%")
    logger.info(f"   📚 성공/전체: {final_report['image_extraction_results']['stackoverflow_effectiveness']['images_successful']}/{final_report['image_extraction_results']['stackoverflow_effectiveness']['images_tested']}")
    
    logger.info(f"\n💾 상세 결과 저장: {output_path}")
    
    if success_rate >= 80:
        logger.info("🎉 403 우회 성공! 목표 달성!")
    elif success_rate >= 60:
        logger.info("✅ 403 우회 양호한 성과!")
    elif success_rate >= 40:
        logger.warning("⚠️  403 우회 부분적 성공")
    else:
        logger.error("❌ 403 우회 개선 필요")
    
    return final_report

if __name__ == "__main__":
    result = asyncio.run(test_image_extraction_and_bypass())