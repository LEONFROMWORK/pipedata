#!/usr/bin/env python3
"""
오빠두 답변 추출 로직 디버깅
채택된 답변을 올바르게 추출하는지 확인
"""

import cloudscraper
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_oppadu_answer_extraction():
    """오빠두 답변 추출 로직 디버깅"""
    
    # 테스트할 URL (답변 완료된 게시글)
    test_url = "https://www.oppadu.com/community/question/?board_id=1&action=view&uid=79620&pg=1"
    
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
        'Referer': 'https://www.oppadu.com/community/question/'
    }
    scraper.headers.update(headers)
    
    print("🔍 오빠두 답변 추출 로직 디버깅")
    print("="*80)
    print(f"테스트 URL: {test_url}")
    print()
    
    try:
        response = scraper.get(test_url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 1. 채택된 답변 배지 찾기
        print("🏆 채택된 답변 배지 찾기:")
        selected_answer_badge = soup.find(class_='selected-answer-badge')
        if selected_answer_badge:
            print(f"  ✅ selected-answer-badge 발견: {selected_answer_badge.get_text(strip=True)}")
            print(f"  배지 HTML: {selected_answer_badge}")
        else:
            print(f"  ❌ selected-answer-badge 없음")
        
        # 2. 다양한 답변 관련 요소 찾기
        print(f"\n📝 답변 관련 요소들:")
        answer_patterns = [
            ('selected-answer-badge', soup.find_all(class_='selected-answer-badge')),
            ('answer-complete-badge', soup.find_all(class_='answer-complete-badge')),
            ('best-answer', soup.find_all(class_='best-answer')),
            ('accepted-answer', soup.find_all(class_='accepted-answer')),
            ('answer-content', soup.find_all(class_='answer-content')),
            ('reply-content', soup.find_all(class_='reply-content')),
            ('comment-content', soup.find_all(class_='comment-content')),
            ('answer-item', soup.find_all(class_='answer-item')),
            ('reply-item', soup.find_all(class_='reply-item')),
            ('comment-item', soup.find_all(class_='comment-item')),
        ]
        
        for pattern_name, elements in answer_patterns:
            if elements:
                print(f"  ✅ {pattern_name}: {len(elements)}개 발견")
                for i, element in enumerate(elements[:2]):  # 첫 2개만
                    text = element.get_text(strip=True)
                    print(f"    {i+1}: {text[:100]}...")
            else:
                print(f"  ❌ {pattern_name}: 없음")
        
        # 3. 현재 크롤러 로직 시뮬레이션
        print(f"\n🧪 현재 크롤러 로직 시뮬레이션:")
        
        if selected_answer_badge:
            print("  🎯 selected-answer-badge 발견, 답변 컨테이너 찾기 시도...")
            
            # 방법 1: 부모 요소에서 답변 내용 찾기
            print("  방법 1: 부모 요소에서 찾기")
            parent = selected_answer_badge.find_parent()
            if parent:
                answer_content = parent.find(class_='answer-content') or parent.find(class_='post-content')
                if answer_content:
                    text = answer_content.get_text(strip=True)
                    print(f"    ✅ 부모에서 답변 발견: {text[:150]}...")
                else:
                    print(f"    ❌ 부모에서 답변 콘텐츠 미발견")
            
            # 방법 2: 형제 요소에서 찾기
            print("  방법 2: 형제 요소에서 찾기")
            next_sibling = selected_answer_badge.find_next_sibling()
            if next_sibling:
                text = next_sibling.get_text(strip=True) if hasattr(next_sibling, 'get_text') else str(next_sibling)
                print(f"    ✅ 다음 형제 요소: {text[:150]}...")
            else:
                print(f"    ❌ 다음 형제 요소 없음")
            
            # 방법 3: 전체 답변 영역에서 찾기
            print("  방법 3: 전체 답변 영역에서 찾기")
            answer_section = soup.find(class_='answer-section') or soup.find(class_='answers')
            if answer_section:
                text = answer_section.get_text(strip=True)
                print(f"    ✅ 답변 영역 발견: {text[:150]}...")
            else:
                print(f"    ❌ 답변 영역 미발견")
        
        # 4. 페이지 구조 상세 분석
        print(f"\n🔍 페이지 구조 상세 분석:")
        
        # selected-answer-badge 주변 구조 분석
        if selected_answer_badge:
            print("  selected-answer-badge 주변 구조:")
            
            # 부모들을 순회하며 구조 파악
            current = selected_answer_badge
            level = 0
            while current and level < 5:
                print(f"    레벨 {level}: {current.name} - {current.get('class', [])}")
                if hasattr(current, 'get_text'):
                    content = current.get_text(strip=True)
                    if len(content) > 100:
                        print(f"      텍스트: {content[:100]}...")
                current = current.parent
                level += 1
        
        # 5. 모든 답변/댓글 요소 찾기
        print(f"\n📋 모든 답변/댓글 요소:")
        
        # 클래스에 'comment', 'reply', 'answer'가 포함된 모든 요소
        all_elements = soup.find_all(True)
        relevant_elements = []
        
        for element in all_elements:
            classes = element.get('class', [])
            class_str = ' '.join(classes).lower()
            if any(keyword in class_str for keyword in ['comment', 'reply', 'answer', 'response']):
                relevant_elements.append(element)
        
        print(f"  총 {len(relevant_elements)}개의 관련 요소 발견")
        for i, element in enumerate(relevant_elements[:10]):  # 첫 10개만
            classes = element.get('class', [])
            text = element.get_text(strip=True)
            if len(text) > 20:
                print(f"    {i+1}: {element.name}.{classes} - {text[:80]}...")
        
        # 6. 올바른 답변 추출 방법 제안
        print(f"\n💡 개선된 답변 추출 방법:")
        
        # 채택 답변이 있는 경우의 구조를 바탕으로 정확한 위치 찾기
        if selected_answer_badge:
            # 가장 가까운 답변 콘텐츠 찾기
            possible_containers = []
            
            # 부모 요소들을 순회하며 답변 콘텐츠 찾기
            current = selected_answer_badge.parent
            while current and len(possible_containers) < 3:
                # 답변 콘텐츠가 될 수 있는 요소들 찾기
                content_elements = current.find_all(['div', 'p'], class_=lambda x: x and any(
                    keyword in ' '.join(x).lower() for keyword in ['content', 'text', 'body']
                ))
                
                for elem in content_elements:
                    text = elem.get_text(strip=True)
                    if len(text) > 50 and text not in [e['text'] for e in possible_containers]:
                        possible_containers.append({
                            'element': elem,
                            'text': text,
                            'classes': elem.get('class', [])
                        })
                
                current = current.parent
            
            print(f"  발견된 가능한 답변 콘텐츠 {len(possible_containers)}개:")
            for i, container in enumerate(possible_containers):
                print(f"    {i+1}: {container['classes']} - {container['text'][:100]}...")
        
    except Exception as e:
        print(f"❌ 디버깅 중 오류: {e}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    debug_oppadu_answer_extraction()