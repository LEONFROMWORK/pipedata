#!/usr/bin/env python3
"""
오빠두 올바른 게시글 링크 찾기
post-item-modern 내부의 실제 게시글 링크 추출
"""

import cloudscraper
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def find_correct_oppadu_links():
    """오빠두의 올바른 게시글 링크 찾기"""
    
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
    
    print("🔍 오빠두 올바른 게시글 링크 찾기")
    print("="*80)
    
    try:
        response = scraper.get(list_url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 1. post-list-modern 컨테이너 찾기
        post_list = soup.find('div', class_='post-list-modern')
        if not post_list:
            print("❌ post-list-modern 컨테이너를 찾을 수 없음")
            return
        
        print("✅ post-list-modern 컨테이너 발견")
        
        # 2. post-item-modern 항목들 분석
        post_items = post_list.find_all('div', class_='post-item-modern')
        print(f"📝 총 {len(post_items)}개의 post-item-modern 발견")
        
        # 3. 각 게시글 항목의 링크 구조 상세 분석
        for i, item in enumerate(post_items[:5]):  # 첫 5개만 분석
            print(f"\n--- 게시글 {i+1} 링크 구조 분석 ---")
            
            # item 내의 모든 링크 찾기
            all_links_in_item = item.find_all('a', href=True)
            print(f"  이 항목 내 총 링크 수: {len(all_links_in_item)}")
            
            for j, link in enumerate(all_links_in_item):
                href = link['href']
                link_text = link.get_text(strip=True)
                classes = link.get('class', [])
                
                print(f"    링크 {j+1}:")
                print(f"      href: {href}")
                print(f"      text: {link_text[:50]}...")
                print(f"      classes: {classes}")
                print(f"      full_url: {urljoin(base_url, href)}")
                
                # 게시글 제목 링크인지 확인
                if any(cls in classes for cls in ['post-title', 'post-title-modern']):
                    print(f"      🎯 게시글 제목 링크로 추정")
                
                # 사용자 링크 제외
                if 'user-info' in href:
                    print(f"      ❌ 사용자 정보 링크 (제외)")
                elif 'board_id' in href and 'action=view' in href:
                    print(f"      ✅ 게시글 링크로 추정")
                
                print()
            
            # 답변 완료 배지 확인
            answer_badge = item.find(class_='answer-complete-badge')
            print(f"  답변 완료 배지: {'✅ 있음' if answer_badge else '❌ 없음'}")
            
            # 추천 링크 추출 로직
            title_link = item.find('a', class_='post-title-modern')
            if title_link:
                recommended_url = urljoin(base_url, title_link['href'])
                print(f"  🎯 추천 URL: {recommended_url}")
                
                # 이 URL로 실제 요청 테스트
                print(f"  🧪 URL 테스트 중...")
                try:
                    test_response = scraper.get(recommended_url, timeout=15)
                    test_soup = BeautifulSoup(test_response.text, 'html.parser')
                    
                    # 페이지 타이틀 확인
                    page_title = test_soup.find('title')
                    if page_title:
                        title_text = page_title.get_text()
                        print(f"    페이지 제목: {title_text}")
                        
                        # 홈페이지인지 게시글인지 판단
                        if "엑셀강의 대표채널" in title_text:
                            print(f"    ❌ 홈페이지로 리디렉션됨")
                        elif title_text.strip() and title_text != "오빠두엑셀":
                            print(f"    ✅ 개별 게시글 페이지")
                        else:
                            print(f"    ❓ 불확실")
                    
                    # 게시글 콘텐츠 요소 확인
                    content_indicators = [
                        ('post-content', test_soup.find(class_='post-content')),
                        ('xe-content', test_soup.find(class_='xe-content')),
                        ('board-content', test_soup.find(class_='board-content')),
                        ('question-content', test_soup.find(class_='question-content'))
                    ]
                    
                    found_content = False
                    for indicator_name, element in content_indicators:
                        if element:
                            content_text = element.get_text(strip=True)
                            if len(content_text) > 50:  # 의미있는 콘텐츠가 있는지
                                print(f"    ✅ {indicator_name} 발견: {content_text[:100]}...")
                                found_content = True
                                break
                    
                    if not found_content:
                        # 홈페이지 요소 확인
                        home_elements = test_soup.find_all(class_='slider-contents')
                        if home_elements:
                            print(f"    ❌ 홈페이지 슬라이더 콘텐츠 발견")
                        else:
                            print(f"    ❓ 콘텐츠 구조 불명확")
                
                except Exception as e:
                    print(f"    ❌ URL 테스트 실패: {e}")
            
            print("-" * 60)
        
        # 4. 올바른 링크 추출 로직 제안
        print(f"\n💡 올바른 링크 추출 로직:")
        print("1. post-item-modern 내에서 post-title-modern 클래스의 <a> 태그 찾기")
        print("2. user-info 링크는 제외")
        print("3. board_id, action=view, uid 파라미터가 있는 링크 선택")
        print("4. urljoin으로 절대 URL 생성")
        
    except Exception as e:
        print(f"❌ 분석 중 오류: {e}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    find_correct_oppadu_links()