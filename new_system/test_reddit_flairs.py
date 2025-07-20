#!/usr/bin/env python3
"""
Reddit 플레어 조사 스크립트
"""
import praw
from config import Config
from collections import Counter
import time

def test_reddit_flairs():
    """Reddit r/excel의 실제 플레어들을 조사"""
    
    print("🔍 Reddit r/excel 플레어 조사")
    print("=" * 40)
    
    # Reddit 인스턴스 생성
    reddit = praw.Reddit(
        client_id=Config.REDDIT_CLIENT_ID,
        client_secret=Config.REDDIT_CLIENT_SECRET,
        user_agent=Config.REDDIT_USER_AGENT,
    )
    
    subreddit = reddit.subreddit('excel')
    flair_counter = Counter()
    checked_count = 0
    
    try:
        print("📑 최신 100개 포스트의 플레어 조사 중...")
        
        for submission in subreddit.new(limit=100):
            checked_count += 1
            flair = submission.link_flair_text
            
            if flair:
                flair_counter[flair.lower()] += 1
                print(f"   {checked_count:3d}. '{flair}' - {submission.title[:50]}...")
            else:
                flair_counter['none'] += 1
                
            if checked_count % 25 == 0:
                print(f"\n   --> {checked_count}개 검사 완료\n")
        
        print(f"\n✅ 총 {checked_count}개 포스트 조사 완료")
        print("\n📊 플레어 분포:")
        for flair, count in flair_counter.most_common():
            percentage = (count / checked_count) * 100
            print(f"   '{flair}': {count}개 ({percentage:.1f}%)")
        
        print(f"\n🎯 'solved' 플레어: {flair_counter.get('solved', 0)}개 발견")
        
        # Hot 포스트도 확인
        print(f"\n🔥 Hot 포스트 100개도 확인 중...")
        hot_flair_counter = Counter()
        hot_checked = 0
        
        for submission in subreddit.hot(limit=100):
            hot_checked += 1
            flair = submission.link_flair_text
            
            if flair:
                hot_flair_counter[flair.lower()] += 1
                if 'solved' in flair.lower():
                    print(f"   🎯 HOT에서 solved 발견: '{flair}' - {submission.title[:50]}...")
            else:
                hot_flair_counter['none'] += 1
        
        print(f"\n🔥 Hot 포스트에서 'solved' 플레어: {hot_flair_counter.get('solved', 0)}개 발견")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    test_reddit_flairs()