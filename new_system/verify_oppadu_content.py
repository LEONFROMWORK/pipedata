#!/usr/bin/env python3
"""
오빠두 게시글 실제 콘텐츠 검증
추출된 URL들이 실제 Q&A 게시글인지 확인
"""

import cloudscraper
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_oppadu_content():
    """오빠두 게시글의 실제 콘텐츠가 Q&A인지 검증"""
    
    # 분석할 샘플 URL들 (위 분석에서 나온 답변 완료 게시글들)
    sample_urls = [
        "https://www.oppadu.com?board_id=1&action=view&uid=79620&pg=1",
        "https://www.oppadu.com?board_id=1&action=view&uid=79616&pg=1", 
        "https://www.oppadu.com?board_id=1&action=view&uid=79613&pg=1"
    ]
    
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
    
    print("🔍 오빠두 게시글 콘텐츠 검증")
    print("="*80)
    
    for i, url in enumerate(sample_urls, 1):
        print(f"\n📝 게시글 {i} 분석: {url}")
        print("-" * 60)
        
        try:
            response = scraper.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 1. 페이지 제목
            title = soup.find('title')
            page_title = title.get_text() if title else "제목 없음"
            print(f"페이지 제목: {page_title}")
            
            # 2. 게시글 제목
            post_title = soup.find('h1') or soup.find(class_='post-title')
            if post_title:
                print(f"게시글 제목: {post_title.get_text(strip=True)}")
            
            # 3. 질문 내용 확인
            post_content = soup.find(class_='post-content')
            if post_content:
                content_text = post_content.get_text(strip=True)
                print(f"질문 내용 길이: {len(content_text)} 문자")
                print(f"질문 미리보기: {content_text[:200]}...")
                
                # Q&A 특성 키워드 확인
                qa_keywords = ['엑셀', '함수', '수식', '셀', '질문', '문제', '도움', '방법']
                found_keywords = [kw for kw in qa_keywords if kw in content_text]
                print(f"Q&A 관련 키워드: {found_keywords}")
            
            # 4. 답변 섹션 확인
            answer_sections = soup.find_all(class_='answer-item') or soup.find_all(class_='reply-item')
            print(f"답변 개수: {len(answer_sections)}개")
            
            # 5. 채택된 답변 확인
            selected_answer = soup.find(class_='selected-answer-badge') or soup.find(class_='best-answer')
            if selected_answer:
                print("✅ 채택된 답변 있음")
                # 채택 답변 내용 찾기
                answer_content = None
                parent = selected_answer.find_parent()
                if parent:
                    answer_content = parent.find(class_='answer-content') or parent.find(class_='post-content')
                    if answer_content:
                        answer_text = answer_content.get_text(strip=True)
                        print(f"채택 답변 길이: {len(answer_text)} 문자")
                        print(f"채택 답변 미리보기: {answer_text[:150]}...")
            else:
                print("❌ 채택된 답변 미발견")
            
            # 6. 메타데이터 확인
            metadata_indicators = {
                'excel_version': ['엑셀버전', '엑셀 버전', 'Excel'],
                'os_version': ['OS버전', '운영체제', 'Windows'],
                'question_type': ['질문', '문의', '도움', '문제']
            }
            
            page_text = soup.get_text()
            for meta_type, keywords in metadata_indicators.items():
                found = any(keyword in page_text for keyword in keywords)
                print(f"{meta_type}: {'✅' if found else '❌'}")
            
            # 7. 콘텐츠 유형 판단
            print(f"\n📊 콘텐츠 유형 분석:")
            
            # 프로모션 콘텐츠 특성
            promo_indicators = ['광고', '프로모션', '이벤트', '할인', '구매', '상품']
            promo_found = any(indicator in page_text for indicator in promo_indicators)
            
            # Q&A 콘텐츠 특성  
            qa_indicators = ['질문', '답변', '해결', '도움', '함수', '수식', '문제', '방법']
            qa_found = sum(1 for indicator in qa_indicators if indicator in page_text)
            
            print(f"  프로모션 특성: {'❌ 발견됨' if promo_found else '✅ 없음'}")
            print(f"  Q&A 특성: {qa_found}개 키워드 발견")
            
            if qa_found >= 3 and not promo_found:
                print("  🎯 결론: 정상적인 Q&A 게시글")
            elif promo_found:
                print("  ⚠️  결론: 프로모션 콘텐츠 의심")
            else:
                print("  ❓ 결론: 불확실")
                
        except Exception as e:
            print(f"❌ 게시글 분석 중 오류: {e}")
    
    print(f"\n" + "="*80)
    print("✅ 콘텐츠 검증 완료")

if __name__ == "__main__":
    verify_oppadu_content()