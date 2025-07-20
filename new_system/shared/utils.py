"""
공통 유틸리티 함수
모든 수집기가 사용하는 공통 함수들
"""
import json
import hashlib
import re
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import logging

logger = logging.getLogger('shared.utils')

def generate_unique_id(prefix: str = "qa") -> str:
    """고유 ID 생성"""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"

def calculate_text_hash(text: str) -> str:
    """텍스트의 해시값 계산"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def clean_text(text: str) -> str:
    """텍스트 정리"""
    if not text:
        return ""
    
    # 연속된 공백 제거
    text = re.sub(r'\s+', ' ', text)
    
    # 앞뒤 공백 제거
    text = text.strip()
    
    return text

def extract_code_blocks(text: str) -> List[str]:
    """텍스트에서 코드 블록 추출"""
    code_blocks = []
    
    # 마크다운 코드 블록 패턴
    markdown_pattern = r'```(?:\w+)?\n(.*?)\n```'
    matches = re.findall(markdown_pattern, text, re.DOTALL)
    code_blocks.extend(matches)
    
    # 인라인 코드 패턴
    inline_pattern = r'`([^`]+)`'
    matches = re.findall(inline_pattern, text)
    code_blocks.extend(matches)
    
    # Excel 수식 패턴
    formula_pattern = r'=\w+\([^)]*\)'
    matches = re.findall(formula_pattern, text)
    code_blocks.extend(matches)
    
    return list(set(code_blocks))  # 중복 제거

def calculate_quality_score(content: str, metadata: Dict[str, Any]) -> float:
    """품질 점수 계산"""
    score = 5.0  # 기본 점수
    
    # 텍스트 길이 기반 점수
    word_count = len(content.split())
    if word_count >= 50:
        score += 1.0
    elif word_count >= 20:
        score += 0.5
    
    # 코드 블록 포함 여부
    if extract_code_blocks(content):
        score += 1.0
    
    # 메타데이터 기반 점수
    if metadata.get('is_solved'):
        score += 1.0
    
    if metadata.get('upvotes', 0) > 0:
        score += min(2.0, metadata['upvotes'] / 10)
    
    return min(score, 10.0)  # 최대 10점

def save_jsonl(data: List[Dict[str, Any]], file_path: Union[str, Path]) -> None:
    """JSONL 형식으로 데이터 저장"""
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')

def load_jsonl(file_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """JSONL 형식에서 데이터 로드"""
    file_path = Path(file_path)
    if not file_path.exists():
        return []
    
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON line: {line}")
    
    return data

def is_valid_email(email: str) -> bool:
    """이메일 유효성 검사"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def is_valid_url(url: str) -> bool:
    """URL 유효성 검사"""
    pattern = r'^https?://[^\s/$.?#].[^\s]*$'
    return re.match(pattern, url) is not None

def extract_urls(text: str) -> List[str]:
    """텍스트에서 URL 추출"""
    pattern = r'https?://[^\s/$.?#].[^\s]*'
    return re.findall(pattern, text)

def format_datetime(dt: datetime) -> str:
    """datetime을 ISO 형식으로 변환"""
    return dt.isoformat()

def parse_datetime(dt_str: str) -> Optional[datetime]:
    """ISO 형식 문자열을 datetime으로 변환"""
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except ValueError:
        return None

def get_file_size(file_path: Union[str, Path]) -> int:
    """파일 크기 반환 (바이트)"""
    return Path(file_path).stat().st_size

def ensure_directory(path: Union[str, Path]) -> Path:
    """디렉토리 생성"""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path

def get_output_path(base_path: Union[str, Path], source: str, extension: str = 'jsonl') -> Path:
    """출력 파일 경로 생성"""
    base_path = Path(base_path)
    today = datetime.now()
    
    output_dir = base_path / f"year={today.year}" / f"month={today.month:02d}" / f"day={today.day:02d}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = today.strftime("%H%M%S")
    filename = f"{source}_{today.strftime('%Y%m%d')}_{timestamp}.{extension}"
    
    return output_dir / filename

def truncate_string(text: str, max_length: int = 100) -> str:
    """문자열 자르기"""
    if len(text) <= max_length:
        return text
    return text[:max_length] + "..."

def safe_get(dictionary: Dict[str, Any], key: str, default: Any = None) -> Any:
    """안전한 딕셔너리 값 가져오기"""
    try:
        return dictionary.get(key, default)
    except (AttributeError, TypeError):
        return default

def merge_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """딕셔너리 병합"""
    result = dict1.copy()
    result.update(dict2)
    return result

def validate_qa_entry(entry: Dict[str, Any]) -> bool:
    """Q&A 항목 유효성 검사"""
    required_fields = ['id', 'user_question', 'assistant_response', 'metadata']
    
    for field in required_fields:
        if field not in entry:
            return False
    
    # 필수 필드가 비어있지 않은지 확인
    if not entry['user_question'].strip():
        return False
    
    if not entry['assistant_response'].strip():
        return False
    
    return True