#!/usr/bin/env python3
"""
오빠두 URL 문제 분석 및 수정
실제 게시글 페이지가 아닌 홈페이지로 리디렉션되는 문제 해결
"""

import cloudscraper
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_oppadu_url_issue():
    """오빠두 URL 리디렉션 문제 분석"""
    
    base_url = "https://www.oppadu.com"
    list_url = f"{base_url}/community/question/"
    
    # CloudScraper 설정
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Referer': base_url
    }
    scraper.headers.update(headers)
    
    print("🔍 오빠두 URL 문제 분석")
    print("="*80)
    
    try:
        # 1. 목록 페이지에서 실제 링크 추출
        print(f"📋 목록 페이지 분석: {list_url}")
        response = scraper.get(list_url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 2. 모든 링크 분석
        all_links = soup.find_all('a', href=True)
        print(f"총 {len(all_links)}개의 링크 발견")
        
        # 게시글 관련 링크 필터링
        post_links = []
        for link in all_links:
            href = link['href']
            link_text = link.get_text(strip=True)
            
            # 게시글 패턴 확인
            if any(pattern in href for pattern in ['uid=', 'view', 'board_id']):
                post_links.append({
                    'href': href,
                    'text': link_text,
                    'full_url': urljoin(base_url, href)
                })
        
        print(f"게시글 관련 링크: {len(post_links)}개")
        
        # 3. 링크 패턴 분석
        print(f"\n🔗 링크 패턴 분석:")
        patterns = {}
        for link in post_links[:10]:  # 첫 10개만 분석
            href = link['href']
            print(f"  원본 href: {href}")
            print(f"  전체 URL: {link['full_url']}")
            print(f"  링크 텍스트: {link['text'][:50]}...")
            
            # URL 패턴 분류
            if href.startswith('?'):
                patterns['query_only'] = patterns.get('query_only', 0) + 1
            elif href.startswith('/'):
                patterns['absolute_path'] = patterns.get('absolute_path', 0) + 1
            elif href.startswith('http'):
                patterns['full_url'] = patterns.get('full_url', 0) + 1
            else:
                patterns['relative'] = patterns.get('relative', 0) + 1
            print("-" * 40)
        
        print(f"\nURL 패턴 통계: {patterns}")
        
        # 4. 다양한 URL 형식 테스트
        print(f"\n🧪 URL 형식 테스트:")
        
        if post_links:
            test_link = post_links[0]
            original_href = test_link['href']
            
            # 여러 URL 형식 시도
            url_variants = []
            
            # 현재 방식 (문제가 있는 방식)
            current_url = urljoin(base_url, original_href)
            url_variants.append(('Current (urljoin)', current_url))
            
            # 직접 결합
            if original_href.startswith('?'):
                direct_url = base_url + original_href
                url_variants.append(('Direct concat', direct_url))
                
                # 커뮤니티 경로 포함
                community_url = f"{base_url}/community/question/{original_href}"
                url_variants.append(('With community path', community_url))
            
            # 각 URL 시도
            for variant_name, test_url in url_variants:
                print(f"\n  🔍 {variant_name}: {test_url}")
                
                try:
                    test_response = scraper.get(test_url, timeout=15, allow_redirects=False)
                    print(f"    상태 코드: {test_response.status_code}")
                    
                    if test_response.status_code in [301, 302, 303, 307, 308]:
                        print(f"    리디렉션: {test_response.headers.get('Location', 'N/A')}")
                    
                    if test_response.status_code == 200:
                        # 콘텐츠 확인
                        test_soup = BeautifulSoup(test_response.text, 'html.parser')
                        title = test_soup.find('title')
                        page_title = title.get_text() if title else "제목 없음"
                        print(f"    페이지 제목: {page_title}")
                        
                        # 게시글 컨텐츠 여부 확인
                        indicators = {
                            'post_content': test_soup.find(class_='post-content'),
                            'board_content': test_soup.find(class_='board-content'),
                            'article_content': test_soup.find(class_='article-content'),
                            'question_title': test_soup.find('h1'),
                            'home_indicators': test_soup.find_all(class_='slider-contents')
                        }
                        
                        for indicator_name, element in indicators.items():
                            if element:
                                if indicator_name == 'home_indicators':
                                    print(f"    ❌ 홈페이지 콘텐츠 감지: {len(element)}개 슬라이더")
                                else:
                                    text = element.get_text(strip=True) if hasattr(element, 'get_text') else str(element)
                                    print(f"    ✅ {indicator_name}: {text[:50]}...")
                
                except Exception as e:
                    print(f"    ❌ 오류: {e}")
        
        # 5. 올바른 URL 형식 제안
        print(f"\n💡 수정 제안:")
        print("  1. urljoin() 사용 시 base_url에 슬래시 확인")
        print("  2. 쿼리 파라미터가 있는 경우 올바른 경로 결합")
        print("  3. 세션/쿠키 요구사항 확인")
        print("  4. User-Agent 및 Referer 헤더 확인")
        
    except Exception as e:
        print(f"❌ 분석 중 오류: {e}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    analyze_oppadu_url_issue()