"""
Reddit 이미지 403 우회를 위한 고급 기법 구현
최신 2025년 기법들을 종합적으로 적용하여 preview.redd.it 접근 성공률 극대화
"""
import asyncio
import random
import time
import re
import html
from urllib.parse import urlparse, parse_qs, unquote, urlencode
from typing import Dict, List, Optional, Tuple
import logging
import cloudscraper

logger = logging.getLogger('pipeline.reddit_bypasser')

class RedditImageBypasser:
    """Reddit 이미지 403 우회를 위한 종합적인 솔루션"""
    
    def __init__(self, reddit_credentials: Dict[str, str]):
        self.reddit_credentials = reddit_credentials
        
        # 최신 브라우저 User-Agent 목록 (2025년 기준)
        self.user_agents = [
            # Chrome 최신 버전들
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            
            # Firefox 최신 버전들
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0',
            
            # Edge 최신 버전
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
            
            # Safari 최신 버전
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15'
        ]
        
        # Reddit OAuth 세션 관리
        self.reddit_session = None
        self.session_expires = 0
        
        # 성공률 추적
        self.success_stats = {
            'total_attempts': 0,
            'successful_downloads': 0,
            'method_success': {}
        }
    
    async def get_reddit_oauth_session(self) -> Optional[str]:
        """Reddit OAuth 세션 토큰 획득"""
        try:
            if self.reddit_session and time.time() < self.session_expires:
                return self.reddit_session
            
            # Reddit OAuth 인증
            import aiohttp
            
            auth_data = {
                'grant_type': 'client_credentials'
            }
            
            auth_headers = {
                'User-Agent': f"ExcelQACollector/1.0 by /u/{self.reddit_credentials.get('username', 'test_user')}",
                'Authorization': f"Basic {self._get_basic_auth()}"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    'https://www.reddit.com/api/v1/access_token',
                    data=auth_data,
                    headers=auth_headers
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.reddit_session = data.get('access_token')
                        self.session_expires = time.time() + data.get('expires_in', 3600) - 60
                        logger.info("✅ Reddit OAuth 세션 획득 성공")
                        return self.reddit_session
                    else:
                        logger.warning(f"Reddit OAuth 실패: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Reddit OAuth 오류: {e}")
            return None
    
    def _get_basic_auth(self) -> str:
        """Reddit OAuth용 Basic 인증 헤더 생성"""
        import base64
        credentials = f"{self.reddit_credentials.get('client_id', '')}:{self.reddit_credentials.get('client_secret', '')}"
        return base64.b64encode(credentials.encode()).decode()
    
    def decode_reddit_url(self, url: str) -> str:
        """Reddit URL 디코딩 및 정리"""
        if not url:
            return url
        
        # HTML 엔티티 디코딩
        url = html.unescape(url)
        
        # URL 디코딩
        url = unquote(url)
        
        # Reddit 특화 디코딩 패턴들
        # amp; 패턴 정리
        url = url.replace('amp;s=', 's=')
        url = re.sub(r'amp;', '&', url)
        
        # 중복 프로토콜 제거
        url = re.sub(r'https?://https?://', 'https://', url)
        
        return url
    
    def get_alternative_reddit_urls(self, original_url: str) -> List[str]:
        """원본 URL에서 다양한 대안 URL 생성"""
        alternatives = [original_url]
        
        parsed = urlparse(original_url)
        
        # 1. preview.redd.it → i.redd.it 변환
        if 'preview.redd.it' in parsed.netloc:
            i_redd_url = original_url.replace('preview.redd.it', 'i.redd.it')
            # 쿼리 파라미터 제거한 버전
            clean_i_redd = i_redd_url.split('?')[0]
            alternatives.extend([i_redd_url, clean_i_redd])
        
        # 2. external-preview.redd.it → preview.redd.it 변환
        if 'external-preview.redd.it' in parsed.netloc:
            preview_url = original_url.replace('external-preview.redd.it', 'preview.redd.it')
            alternatives.append(preview_url)
        
        # 3. 다양한 크기/품질 파라미터 시도
        if '?' in original_url:
            base_url = original_url.split('?')[0]
            
            # 고품질 파라미터들
            quality_params = [
                'format=png&auto=webp&s=',
                'format=jpg&auto=webp&s=',
                'width=1024&format=png&auto=webp&s=',
                'width=512&format=jpg&auto=webp&s='
            ]
            
            for param in quality_params:
                alternatives.append(f"{base_url}?{param}")
        
        # 4. HTTPS → HTTP 대안 (최후의 수단)
        if original_url.startswith('https://'):
            alternatives.append(original_url.replace('https://', 'http://'))
        
        return list(set(alternatives))  # 중복 제거
    
    def get_reddit_headers(self, url: str, oauth_token: Optional[str] = None) -> Dict[str, str]:
        """Reddit 특화 헤더 생성"""
        parsed = urlparse(url)
        
        # 랜덤 User-Agent 선택
        user_agent = random.choice(self.user_agents)
        
        headers = {
            'User-Agent': user_agent,
            'Accept': 'image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'image',
            'Sec-Fetch-Mode': 'no-cors',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-Ch-Ua': '"Not A(Brand)";v="99", "Google Chrome";v="121", "Chromium";v="121"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"'
        }
        
        # Reddit 도메인별 특화 헤더
        if 'reddit' in parsed.netloc:
            headers.update({
                'Referer': 'https://www.reddit.com/',
                'Origin': 'https://www.reddit.com'
            })
            
            # OAuth 토큰이 있으면 Authorization 헤더 추가 (특정 엔드포인트만)
            if oauth_token and 'oauth.reddit.com' in parsed.netloc:
                headers['Authorization'] = f'Bearer {oauth_token}'
        
        # redd.it 이미지 도메인 특화
        elif 'redd.it' in parsed.netloc:
            headers.update({
                'Referer': 'https://www.reddit.com/',
                'Sec-Fetch-Site': 'same-site'
            })
        
        return headers
    
    async def download_reddit_image_with_bypass(self, url: str) -> Tuple[Optional[bytes], str]:
        """Reddit 이미지 다운로드 with 종합 우회 기법"""
        self.success_stats['total_attempts'] += 1
        
        # URL 전처리
        cleaned_url = self.decode_reddit_url(url)
        alternative_urls = self.get_alternative_reddit_urls(cleaned_url)
        
        logger.info(f"🎯 Reddit 이미지 다운로드 시도: {len(alternative_urls)}개 URL")
        
        # OAuth 세션 획득 시도
        oauth_token = await self.get_reddit_oauth_session()
        
        # 각 URL과 방법 조합 시도
        methods = [
            ('cloudscraper_basic', self._download_with_cloudscraper),
            ('cloudscraper_oauth', self._download_with_cloudscraper_oauth),
            ('session_spoofing', self._download_with_session_spoofing),
            ('proxy_rotation', self._download_with_proxy_simulation)
        ]
        
        for method_name, method_func in methods:
            for attempt_url in alternative_urls:
                try:
                    logger.debug(f"  시도: {method_name} + {attempt_url[:50]}...")
                    
                    result = await method_func(attempt_url, oauth_token)
                    if result:
                        self.success_stats['successful_downloads'] += 1
                        
                        # 성공 통계 업데이트
                        if method_name not in self.success_stats['method_success']:
                            self.success_stats['method_success'][method_name] = 0
                        self.success_stats['method_success'][method_name] += 1
                        
                        logger.info(f"✅ 성공! 방법: {method_name}")
                        return result, method_name
                    
                    # 실패 시 지연 (레이트 리밋 회피)
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    
                except Exception as e:
                    logger.debug(f"  실패: {method_name} - {e}")
                    continue
        
        logger.error(f"❌ 모든 방법 실패: {url}")
        return None, "all_failed"
    
    async def _download_with_cloudscraper(self, url: str, oauth_token: Optional[str]) -> Optional[bytes]:
        """Cloudscraper 기본 다운로드"""
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        
        headers = self.get_reddit_headers(url, oauth_token)
        
        # 지연 추가
        await asyncio.sleep(random.expovariate(0.3))  # 평균 3.3초
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: scraper.get(url, headers=headers, timeout=30, allow_redirects=True)
        )
        
        if response.status_code == 200 and len(response.content) > 1000:
            return response.content
        
        return None
    
    async def _download_with_cloudscraper_oauth(self, url: str, oauth_token: Optional[str]) -> Optional[bytes]:
        """OAuth 토큰을 활용한 다운로드"""
        if not oauth_token:
            return None
        
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'firefox',
                'platform': 'linux',
                'desktop': True
            }
        )
        
        headers = self.get_reddit_headers(url, oauth_token)
        headers['Authorization'] = f'Bearer {oauth_token}'
        
        await asyncio.sleep(random.expovariate(0.5))  # 평균 2초
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: scraper.get(url, headers=headers, timeout=30)
        )
        
        if response.status_code == 200 and len(response.content) > 1000:
            return response.content
        
        return None
    
    async def _download_with_session_spoofing(self, url: str, oauth_token: Optional[str]) -> Optional[bytes]:
        """세션 스푸핑을 통한 다운로드"""
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'safari',
                'platform': 'darwin',
                'desktop': True
            }
        )
        
        headers = self.get_reddit_headers(url, oauth_token)
        
        # Reddit 세션 쿠키 시뮬레이션
        fake_cookies = {
            'session_tracker': f'reddit_{int(time.time())}',
            'eu_cookie_v2': '1',
            'session': f'sess_{random.randint(100000, 999999)}',
            'csv': '2',
            'edgebucket': 'control_1'
        }
        
        await asyncio.sleep(random.expovariate(0.4))  # 평균 2.5초
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: scraper.get(url, headers=headers, cookies=fake_cookies, timeout=30)
        )
        
        if response.status_code == 200 and len(response.content) > 1000:
            return response.content
        
        return None
    
    async def _download_with_proxy_simulation(self, url: str, oauth_token: Optional[str]) -> Optional[bytes]:
        """프록시 시뮬레이션 다운로드"""
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': random.choice(['chrome', 'firefox', 'safari']),
                'platform': random.choice(['windows', 'darwin', 'linux']),
                'desktop': True
            }
        )
        
        headers = self.get_reddit_headers(url, oauth_token)
        
        # 프록시 헤더 시뮬레이션
        proxy_headers = {
            'X-Forwarded-For': f'{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}',
            'X-Real-IP': f'{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}',
            'X-Forwarded-Proto': 'https'
        }
        headers.update(proxy_headers)
        
        await asyncio.sleep(random.expovariate(0.2))  # 평균 5초
        
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: scraper.get(url, headers=headers, timeout=30)
        )
        
        if response.status_code == 200 and len(response.content) > 1000:
            return response.content
        
        return None
    
    def get_success_rate(self) -> float:
        """현재 성공률 반환"""
        if self.success_stats['total_attempts'] == 0:
            return 0.0
        return (self.success_stats['successful_downloads'] / self.success_stats['total_attempts']) * 100
    
    def get_detailed_stats(self) -> Dict:
        """상세 통계 반환"""
        return {
            'success_rate': self.get_success_rate(),
            'total_attempts': self.success_stats['total_attempts'],
            'successful_downloads': self.success_stats['successful_downloads'],
            'method_breakdown': self.success_stats['method_success']
        }

# 통합 테스트 함수
async def test_reddit_bypass():
    """Reddit 이미지 우회 테스트"""
    
    # Reddit 자격증명 (환경변수에서 로드)
    import os
    credentials = {
        'client_id': os.getenv('REDDIT_CLIENT_ID', ''),
        'client_secret': os.getenv('REDDIT_CLIENT_SECRET', ''),
        'username': 'test_user'
    }
    
    bypasser = RedditImageBypasser(credentials)
    
    # 테스트할 Reddit 이미지 URL들
    test_urls = [
        'https://preview.redd.it/76mukstfxhdf1.png',
        'https://preview.redd.it/some_test_image.jpg',
        'https://external-preview.redd.it/test_image.png'
    ]
    
    logger.info("🚀 Reddit 이미지 403 우회 테스트 시작")
    
    for i, url in enumerate(test_urls, 1):
        logger.info(f"[{i}/{len(test_urls)}] 테스트: {url}")
        
        result, method = await bypasser.download_reddit_image_with_bypass(url)
        
        if result:
            logger.info(f"✅ 성공! 크기: {len(result)} bytes, 방법: {method}")
        else:
            logger.error(f"❌ 실패: {method}")
        
        logger.info("-" * 50)
    
    # 최종 통계
    stats = bypasser.get_detailed_stats()
    logger.info("📊 최종 통계:")
    logger.info(f"   성공률: {stats['success_rate']:.1f}%")
    logger.info(f"   총 시도: {stats['total_attempts']}")
    logger.info(f"   성공: {stats['successful_downloads']}")
    logger.info(f"   방법별 성공: {stats['method_breakdown']}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_reddit_bypass())