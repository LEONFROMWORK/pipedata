"""
이미지 처리 유틸리티
Oppadu 게시물의 이미지를 다운로드하고 OCR로 텍스트 추출
"""
import asyncio
import logging
import hashlib
import mimetypes
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path
from urllib.parse import urljoin, urlparse
import aiohttp
import aiofiles
from datetime import datetime
import re
import base64

# OCR 라이브러리 (선택적)
try:
    import pytesseract
    from PIL import Image
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False

# 이미지 처리 라이브러리
try:
    from PIL import Image, ImageEnhance, ImageFilter
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

logger = logging.getLogger('shared.image_processor')

class ImageProcessor:
    """이미지 다운로드 및 OCR 처리"""
    
    def __init__(self, cache_dir: Path = None):
        """이미지 프로세서 초기화"""
        self.cache_dir = cache_dir or Path('/Users/kevin/bigdata/data/images')
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 지원되는 이미지 형식
        self.supported_formats = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
        
        # 세션 설정
        self.session = None
        
        # OCR 설정
        self.ocr_config = {
            'language': 'kor+eng',  # 한국어 + 영어
            'config': '--psm 6 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz가-힣()[]{}=+*/-.,;:!?<>%@#$^&_|~`"\' '
        }
        
        logger.info(f"이미지 프로세서 초기화 완료 (OCR: {OCR_AVAILABLE})")
    
    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        if self.session:
            await self.session.close()
    
    async def process_images_from_html(self, html_content: str, base_url: str) -> List[Dict[str, Any]]:
        """HTML에서 이미지 추출 및 처리"""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 모든 이미지 태그 찾기
            img_tags = soup.find_all('img')
            
            processed_images = []
            
            for img in img_tags:
                src = img.get('src')
                if not src:
                    continue
                
                # 상대 URL을 절대 URL로 변환
                image_url = urljoin(base_url, src)
                
                # 이미지 처리
                image_info = await self.process_single_image(image_url)
                if image_info:
                    # 이미지 컨텍스트 정보 추가
                    image_info['context'] = {
                        'alt_text': img.get('alt', ''),
                        'title': img.get('title', ''),
                        'width': img.get('width', ''),
                        'height': img.get('height', ''),
                        'position': len(processed_images) + 1
                    }
                    processed_images.append(image_info)
            
            logger.info(f"총 {len(processed_images)}개 이미지 처리 완료")
            return processed_images
            
        except Exception as e:
            logger.error(f"HTML 이미지 처리 중 오류: {e}")
            return []
    
    async def process_single_image(self, image_url: str) -> Optional[Dict[str, Any]]:
        """단일 이미지 처리"""
        try:
            # URL 유효성 검사
            if not self._is_valid_image_url(image_url):
                logger.debug(f"유효하지 않은 이미지 URL: {image_url}")
                return None
            
            # 캐시 확인
            image_hash = self._generate_image_hash(image_url)
            cache_info = await self._get_cached_image_info(image_hash)
            if cache_info:
                logger.debug(f"캐시된 이미지 정보 사용: {image_url}")
                return cache_info
            
            # 이미지 다운로드
            image_data = await self._download_image(image_url)
            if not image_data:
                return None
            
            # 이미지 파일 저장
            image_path = await self._save_image(image_hash, image_data)
            
            # OCR 처리
            extracted_text = await self._extract_text_from_image(image_path)
            
            # 이미지 정보 생성
            image_info = {
                'url': image_url,
                'hash': image_hash,
                'path': str(image_path),
                'size': len(image_data),
                'extracted_text': extracted_text,
                'processed_at': datetime.now().isoformat(),
                'ocr_available': OCR_AVAILABLE
            }
            
            # 캐시 저장
            await self._cache_image_info(image_hash, image_info)
            
            logger.debug(f"이미지 처리 완료: {image_url}")
            return image_info
            
        except Exception as e:
            logger.error(f"이미지 처리 중 오류: {image_url} - {e}")
            return None
    
    async def _download_image(self, image_url: str) -> Optional[bytes]:
        """이미지 다운로드"""
        try:
            if not self.session:
                raise Exception("세션이 초기화되지 않음")
            
            async with self.session.get(image_url) as response:
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '')
                    if content_type.startswith('image/'):
                        return await response.read()
                    else:
                        logger.warning(f"이미지가 아닌 콘텐츠 타입: {content_type}")
                        return None
                else:
                    logger.warning(f"이미지 다운로드 실패: {response.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"이미지 다운로드 중 오류: {e}")
            return None
    
    async def _save_image(self, image_hash: str, image_data: bytes) -> Path:
        """이미지 파일 저장"""
        # 확장자 추측
        try:
            from PIL import Image
            import io
            
            img = Image.open(io.BytesIO(image_data))
            format_lower = img.format.lower() if img.format else 'jpg'
            extension = f'.{format_lower}'
        except:
            extension = '.jpg'  # 기본값
        
        # 파일 경로 생성
        image_path = self.cache_dir / f"{image_hash}{extension}"
        
        # 파일 저장
        async with aiofiles.open(image_path, 'wb') as f:
            await f.write(image_data)
        
        return image_path
    
    async def _extract_text_from_image(self, image_path: Path) -> str:
        """이미지에서 텍스트 추출 (OCR)"""
        if not OCR_AVAILABLE:
            logger.debug("OCR 라이브러리가 없음")
            return ""
        
        try:
            # 이미지 전처리
            preprocessed_image = await self._preprocess_image(image_path)
            
            # OCR 실행
            text = pytesseract.image_to_string(
                preprocessed_image,
                lang=self.ocr_config['language'],
                config=self.ocr_config['config']
            )
            
            # 텍스트 정리
            cleaned_text = self._clean_ocr_text(text)
            
            logger.debug(f"OCR 텍스트 추출 완료: {len(cleaned_text)} 문자")
            return cleaned_text
            
        except Exception as e:
            logger.error(f"OCR 처리 중 오류: {e}")
            return ""
    
    async def _preprocess_image(self, image_path: Path) -> Image.Image:
        """이미지 전처리 (OCR 정확도 향상)"""
        try:
            # 이미지 열기
            image = Image.open(image_path)
            
            # 그레이스케일 변환
            if image.mode != 'L':
                image = image.convert('L')
            
            # 이미지 크기 조정 (너무 작으면 확대)
            width, height = image.size
            if width < 300 or height < 300:
                scale_factor = max(300 / width, 300 / height)
                new_width = int(width * scale_factor)
                new_height = int(height * scale_factor)
                image = image.resize((new_width, new_height), Image.LANCZOS)
            
            # 대비 향상
            enhancer = ImageEnhance.Contrast(image)
            image = enhancer.enhance(2.0)
            
            # 선명도 향상
            image = image.filter(ImageFilter.SHARPEN)
            
            return image
            
        except Exception as e:
            logger.error(f"이미지 전처리 중 오류: {e}")
            return Image.open(image_path)
    
    def _clean_ocr_text(self, text: str) -> str:
        """OCR 텍스트 정리"""
        if not text:
            return ""
        
        # 줄바꿈 정리
        text = re.sub(r'\n+', ' ', text)
        
        # 특수 문자 정리
        text = re.sub(r'[^\w\s가-힣()[]{}=+*/-.,;:!?<>%@#$^&_|~`"\']', ' ', text)
        
        # 공백 정리
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def _is_valid_image_url(self, url: str) -> bool:
        """이미지 URL 유효성 검사"""
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                return False
            
            # 확장자 검사
            path_lower = parsed.path.lower()
            if any(path_lower.endswith(ext) for ext in self.supported_formats):
                return True
            
            # 확장자가 없어도 이미지일 수 있음
            return True
            
        except Exception:
            return False
    
    def _generate_image_hash(self, image_url: str) -> str:
        """이미지 URL 해시 생성"""
        return hashlib.md5(image_url.encode()).hexdigest()
    
    async def _get_cached_image_info(self, image_hash: str) -> Optional[Dict[str, Any]]:
        """캐시된 이미지 정보 조회"""
        cache_file = self.cache_dir / f"{image_hash}.json"
        
        try:
            if cache_file.exists():
                async with aiofiles.open(cache_file, 'r', encoding='utf-8') as f:
                    import json
                    content = await f.read()
                    return json.loads(content)
        except Exception:
            pass
        
        return None
    
    async def _cache_image_info(self, image_hash: str, image_info: Dict[str, Any]) -> None:
        """이미지 정보 캐싱"""
        cache_file = self.cache_dir / f"{image_hash}.json"
        
        try:
            async with aiofiles.open(cache_file, 'w', encoding='utf-8') as f:
                import json
                await f.write(json.dumps(image_info, ensure_ascii=False, indent=2))
        except Exception as e:
            logger.warning(f"이미지 정보 캐싱 실패: {e}")

def extract_images_from_content(content: str, base_url: str) -> List[str]:
    """HTML 콘텐츠에서 이미지 URL 추출 (동기 버전)"""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(content, 'html.parser')
        
        img_tags = soup.find_all('img')
        image_urls = []
        
        for img in img_tags:
            src = img.get('src')
            if src:
                image_url = urljoin(base_url, src)
                image_urls.append(image_url)
        
        return image_urls
        
    except Exception as e:
        logger.error(f"이미지 URL 추출 중 오류: {e}")
        return []

# 비동기 이미지 처리 헬퍼 함수
async def process_images_async(image_urls: List[str], base_url: str) -> List[Dict[str, Any]]:
    """비동기 이미지 처리"""
    async with ImageProcessor() as processor:
        tasks = [processor.process_single_image(url) for url in image_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        processed_images = []
        for result in results:
            if isinstance(result, dict):
                processed_images.append(result)
            elif isinstance(result, Exception):
                logger.error(f"이미지 처리 중 오류: {result}")
        
        return processed_images