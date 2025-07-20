#!/usr/bin/env python3
"""
오빠두 게시글 URL 추출 상세 분석
현재 크롤러가 추출하는 URL들이 실제 Q&A 게시글인지 확인
"""

import cloudscraper
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_oppadu_posts():
    """오빠두 게시글 URL 추출 로직 디버깅"""
    
    base_url = "https://www.oppadu.com"
    url = f"{base_url}/community/question/"
    
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
        'Referer': 'https://www.oppadu.com/'
    }
    scraper.headers.update(headers)
    
    try:
        print(f"🔍 오빠두 게시글 URL 추출 분석: {url}")
        response = scraper.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        print("\n" + "="*80)
        print("📋 POST URL EXTRACTION ANALYSIS")
        print("="*80)
        
        # 1. post-list-modern 컨테이너 확인
        post_list = soup.find('div', class_='post-list-modern')
        if not post_list:
            print("❌ post-list-modern 컨테이너를 찾을 수 없음")
            return
        else:
            print("✅ post-list-modern 컨테이너 발견")
        
        # 2. post-item-modern 항목들 확인
        post_items = post_list.find_all('div', class_='post-item-modern')
        print(f"📝 총 {len(post_items)}개의 post-item-modern 발견")
        
        answered_posts = []
        all_posts = []
        
        # 3. 각 게시글 항목 분석
        for i, item in enumerate(post_items):
            print(f"\n--- 게시글 {i+1} 분석 ---")
            
            # 답변 완료 배지 확인
            answer_badge = item.find(class_='answer-complete-badge')
            has_answer = answer_badge is not None
            print(f"  답변 완료 배지: {'✅ 있음' if has_answer else '❌ 없음'}")
            
            if answer_badge:
                badge_text = answer_badge.get_text(strip=True)
                print(f"  배지 텍스트: '{badge_text}'")
            
            # 게시글 링크 추출
            link_element = item.find('a', href=True)
            if link_element:
                post_url = urljoin(base_url, link_element['href'])
                post_title = link_element.get_text(strip=True)
                
                print(f"  제목: {post_title[:50]}...")
                print(f"  URL: {post_url}")
                
                all_posts.append({
                    'title': post_title,
                    'url': post_url,
                    'has_answer': has_answer
                })
                
                if has_answer:
                    answered_posts.append(post_url)
                    print("  🎯 답변 완료 게시글로 분류됨")
            else:
                print("  ❌ 링크 요소를 찾을 수 없음")
            
            # HTML 구조 확인
            print(f"  HTML 구조: {str(item)[:200]}...")
        
        # 4. 결과 요약
        print(f"\n" + "="*80)
        print(f"📊 분석 결과 요약")
        print(f"="*80)
        print(f"전체 게시글: {len(all_posts)}개")
        print(f"답변 완료 게시글: {len(answered_posts)}개")
        print(f"답변 완료율: {len(answered_posts)/len(all_posts)*100:.1f}%" if all_posts else "0%")
        
        # 5. 답변 완료 게시글 목록
        if answered_posts:
            print(f"\n✅ 답변 완료 게시글 URL 목록:")
            for i, url in enumerate(answered_posts, 1):
                print(f"  {i}. {url}")
        
        # 6. URL 패턴 분석
        print(f"\n🔗 URL 패턴 분석:")
        url_patterns = {}
        for post in all_posts:
            url = post['url']
            if '?board_id=' in url and 'action=view' in url and 'uid=' in url:
                url_patterns['standard_view'] = url_patterns.get('standard_view', 0) + 1
            elif '/community/question/' == url:
                url_patterns['base_page'] = url_patterns.get('base_page', 0) + 1
            else:
                url_patterns['other'] = url_patterns.get('other', 0) + 1
        
        for pattern, count in url_patterns.items():
            print(f"  {pattern}: {count}개")
        
        # 7. 문제 진단
        print(f"\n🔍 문제 진단:")
        base_page_count = url_patterns.get('base_page', 0)
        if base_page_count > 0:
            print(f"  ⚠️  WARNING: {base_page_count}개의 URL이 기본 페이지로 연결됨")
            print(f"     이는 실제 게시글이 아닌 프로모션 콘텐츠일 가능성이 높음")
        
        standard_count = url_patterns.get('standard_view', 0)
        if standard_count > 0:
            print(f"  ✅ {standard_count}개의 표준 게시글 URL 발견")
        
        # 8. 샘플 게시글 내용 확인 (답변 완료된 것 중 첫 번째)
        if answered_posts:
            print(f"\n🔍 샘플 게시글 내용 확인:")
            sample_url = answered_posts[0]
            print(f"URL: {sample_url}")
            
            try:
                sample_response = scraper.get(sample_url, timeout=30)
                sample_soup = BeautifulSoup(sample_response.text, 'html.parser')
                
                # 제목 확인
                title = sample_soup.find('h1') or sample_soup.find(class_='post-title')
                if title:
                    print(f"제목: {title.get_text(strip=True)}")
                
                # 질문 내용 확인
                post_content = sample_soup.find(class_='post-content')
                if post_content:
                    content_text = post_content.get_text(strip=True)[:200]
                    print(f"내용 미리보기: {content_text}...")
                
                # 답변 확인
                answer_badge = sample_soup.find(class_='selected-answer-badge')
                if answer_badge:
                    print("✅ 채택된 답변 발견")
                else:
                    print("❌ 채택된 답변 미발견")
                
            except Exception as e:
                print(f"샘플 게시글 확인 중 오류: {e}")
        
        return all_posts, answered_posts
        
    except Exception as e:
        print(f"❌ 분석 중 오류: {e}")
        return None, None

if __name__ == "__main__":
    debug_oppadu_posts()