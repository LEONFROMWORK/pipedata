#!/usr/bin/env python3
"""
오빠두 URL 구성 방법 테스트
올바른 게시글 URL 형식 찾기
"""

import cloudscraper
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_oppadu_url_construction():
    """다양한 URL 구성 방법 테스트"""
    
    base_url = "https://www.oppadu.com"
    community_url = f"{base_url}/community/question/"
    
    # 테스트할 샘플 href (목록에서 추출한 값)
    sample_href = "?board_id=1&action=view&uid=79638&pg=1"
    
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
        'Referer': community_url
    }
    scraper.headers.update(headers)
    
    print("🔍 오빠두 URL 구성 방법 테스트")
    print("="*80)
    print(f"샘플 href: {sample_href}")
    print()
    
    # 다양한 URL 구성 방법 테스트
    url_variants = [
        ("1. urljoin(base_url, href)", urljoin(base_url, sample_href)),
        ("2. urljoin(community_url, href)", urljoin(community_url, sample_href)),
        ("3. base_url + href", base_url + sample_href),
        ("4. community_url + href", community_url + sample_href),
        ("5. base_url + /community/question/ + href", f"{base_url}/community/question/{sample_href}"),
        ("6. 절대 경로", f"{base_url}/community/question{sample_href}"),
    ]
    
    for method_name, test_url in url_variants:
        print(f"\n🧪 {method_name}")
        print(f"   URL: {test_url}")
        
        try:
            # 요청 보내기 (리디렉션 허용 안함으로 상태 확인)
            response = scraper.get(test_url, timeout=10, allow_redirects=False)
            print(f"   상태 코드: {response.status_code}")
            
            if response.status_code in [301, 302, 303, 307, 308]:
                redirect_location = response.headers.get('Location', 'N/A')
                print(f"   🔄 리디렉션: {redirect_location}")
                
                # 리디렉션된 페이지도 확인
                if redirect_location and redirect_location != '/':
                    final_response = scraper.get(redirect_location, timeout=10)
                    final_soup = BeautifulSoup(final_response.text, 'html.parser')
                    final_title = final_soup.find('title')
                    if final_title:
                        print(f"   🎯 최종 페이지 제목: {final_title.get_text()}")
            
            elif response.status_code == 200:
                # 성공적인 응답의 경우 내용 분석
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 페이지 제목
                title = soup.find('title')
                page_title = title.get_text() if title else "제목 없음"
                print(f"   📄 페이지 제목: {page_title}")
                
                # 게시글 콘텐츠 vs 홈페이지 판단
                is_homepage = False
                is_post = False
                
                # 홈페이지 표시 요소들
                home_indicators = [
                    soup.find_all(class_='slider-contents'),
                    soup.find_all(class_='main-page-list'),
                    soup.find(string=lambda text: text and "엑셀강의 대표채널" in text)
                ]
                
                if any(home_indicators):
                    is_homepage = True
                    print(f"   ❌ 홈페이지 콘텐츠 감지")
                
                # 게시글 콘텐츠 요소들
                post_indicators = [
                    ('post-content', soup.find(class_='post-content')),
                    ('article-content', soup.find(class_='article-content')),
                    ('board-content', soup.find(class_='board-content')),
                    ('xe-content', soup.find(class_='xe-content')),
                    ('view-content', soup.find(class_='view-content'))
                ]
                
                for indicator_name, element in post_indicators:
                    if element:
                        content_text = element.get_text(strip=True)
                        if len(content_text) > 30:
                            is_post = True
                            print(f"   ✅ {indicator_name} 발견: {content_text[:80]}...")
                            break
                
                if not is_homepage and not is_post:
                    print(f"   ❓ 콘텐츠 유형 불명확")
                
                # 결론
                if is_post and not is_homepage:
                    print(f"   🎯 결론: 올바른 게시글 페이지!")
                elif is_homepage:
                    print(f"   ❌ 결론: 홈페이지로 리디렉션됨")
                else:
                    print(f"   ❓ 결론: 판단 불가")
            
            else:
                print(f"   ❌ HTTP 오류: {response.status_code}")
        
        except Exception as e:
            print(f"   ❌ 요청 실패: {e}")
    
    print(f"\n" + "="*80)
    print("📊 결론 및 권장사항")
    print("="*80)

if __name__ == "__main__":
    test_oppadu_url_construction()