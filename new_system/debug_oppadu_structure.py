#!/usr/bin/env python3
"""
오빠두 실제 HTML 구조 분석 스크립트
현재 크롤러가 잘못된 CSS 선택자를 사용하는 문제 해결
"""

import requests
import cloudscraper
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_oppadu_structure():
    """오빠두 실제 HTML 구조 분석"""
    
    url = "https://www.oppadu.com/community/question/"
    
    # CloudScraper 사용 (Cloudflare 우회)
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )
    
    # 한국 사용자 헤더 설정
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Referer': 'https://www.oppadu.com/'
    }
    scraper.headers.update(headers)
    
    try:
        print(f"🔍 오빠두 페이지 구조 분석: {url}")
        response = scraper.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        print("\n" + "="*80)
        print("📋 ACTUAL HTML STRUCTURE ANALYSIS")
        print("="*80)
        
        # 1. 페이지 제목 확인
        title = soup.find('title')
        print(f"📖 Page Title: {title.get_text() if title else 'Not found'}")
        
        # 2. 주요 컨테이너 찾기
        print("\n🔍 MAIN CONTAINERS:")
        containers = [
            'post-list-modern',  # 현재 사용하는 잘못된 선택자
            'post-item-modern',  # 현재 사용하는 잘못된 선택자
            'post-list',
            'post-item',
            'question-list',
            'board-list',
            'list-container',
            'content-list'
        ]
        
        for container in containers:
            elements = soup.find_all(class_=container)
            print(f"  .{container}: {len(elements)} elements found")
            if elements:
                print(f"    First element: {str(elements[0])[:200]}...")
        
        # 3. 게시글 링크 패턴 분석
        print("\n🔗 POST URL PATTERNS:")
        all_links = soup.find_all('a', href=True)
        post_links = []
        
        for link in all_links:
            href = link['href']
            # 게시글 URL 패턴 찾기
            if any(pattern in href for pattern in ['uid=', 'view', 'question', 'post']):
                post_links.append(href)
        
        print(f"  Total links found: {len(all_links)}")
        print(f"  Potential post links: {len(post_links)}")
        
        if post_links:
            print("  Sample post URLs:")
            for i, link in enumerate(post_links[:5]):
                print(f"    {i+1}. {link}")
        
        # 4. 답변 완료 표시 찾기
        print("\n✅ ANSWER COMPLETION INDICATORS:")
        completion_indicators = [
            '답변완료',
            '답변 완료',
            '해결완료',
            '해결 완료',
            'answered',
            'completed',
            'solved'
        ]
        
        for indicator in completion_indicators:
            elements = soup.find_all(string=lambda text: text and indicator in text)
            print(f"  '{indicator}': {len(elements)} occurrences")
            if elements:
                for elem in elements[:2]:
                    parent = elem.parent
                    print(f"    Parent tag: {parent.name if parent else 'None'}")
                    print(f"    Parent class: {parent.get('class') if parent else 'None'}")
        
        # 5. 실제 게시글 구조 분석
        print("\n📝 POST STRUCTURE ANALYSIS:")
        
        # 일반적인 게시판 구조 태그들 확인
        structure_tags = ['table', 'tr', 'td', 'ul', 'li', 'div']
        for tag in structure_tags:
            elements = soup.find_all(tag)
            if elements:
                # 게시글과 관련된 것들만 필터링
                relevant_elements = []
                for elem in elements:
                    classes = elem.get('class', [])
                    if any(keyword in ' '.join(classes).lower() for keyword in ['post', 'item', 'list', 'board', 'question']):
                        relevant_elements.append(elem)
                
                if relevant_elements:
                    print(f"  <{tag}> with post-related classes: {len(relevant_elements)}")
                    for elem in relevant_elements[:3]:
                        print(f"    Classes: {elem.get('class', [])}")
        
        # 6. 실제 HTML 샘플 출력
        print("\n🔧 RAW HTML SAMPLE (first 2000 chars):")
        print("-" * 50)
        print(response.text[:2000])
        print("-" * 50)
        
        # 7. 자바스크립트로 로드되는 내용인지 확인
        print("\n⚡ JAVASCRIPT CONTENT CHECK:")
        script_tags = soup.find_all('script')
        js_content_indicators = ['ajax', 'fetch', 'xhr', 'api', 'json']
        
        js_found = False
        for script in script_tags:
            script_text = script.get_text()
            for indicator in js_content_indicators:
                if indicator in script_text.lower():
                    js_found = True
                    print(f"  JavaScript loading detected: '{indicator}' found in script")
                    break
        
        if not js_found:
            print("  No obvious JavaScript content loading detected")
        
        # 8. 권장 수정사항 제시
        print("\n💡 RECOMMENDED FIXES:")
        print("  Based on the analysis, the crawler should be updated to:")
        print("  1. Use correct CSS selectors for actual HTML structure")
        print("  2. Handle any JavaScript-rendered content if needed")
        print("  3. Update URL extraction logic for proper post links")
        print("  4. Fix answer completion detection logic")
        
        return response.text
        
    except Exception as e:
        print(f"❌ Error analyzing structure: {e}")
        return None

if __name__ == "__main__":
    analyze_oppadu_structure()