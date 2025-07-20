# 로컬-Railway 하이브리드 아키텍처 가이드

## 개요

이 가이드는 PipeData를 로컬에서 실행하면서 ExcelApp, ExcelApp-Rails, Excel-AI-Knowledge-Generator를 Railway에 배포하여 운영하는 하이브리드 아키텍처 설정 방법을 설명합니다.

## 아키텍처 구조

```
┌─────────────────────┐
│   로컬 서버         │
│                     │
│  PipeData API       │
│  - OppaduCrawler    │
│  - Selenium/Chrome  │
│  - OCR Processing   │
│  - Data Collection  │
└──────────┬──────────┘
           │ HTTPS
           ├─────────────────┐
           │                 │
           ▼                 ▼
┌──────────────────┐  ┌──────────────────┐
│  Railway Cloud   │  │  Railway Cloud   │
│                  │  │                  │
│  ExcelApp        │  │  ExcelApp-Rails  │
│  (Next.js)       │  │  (Ruby on Rails) │
└──────────────────┘  └──────────────────┘
           │                 │
           └────────┬────────┘
                    ▼
         ┌──────────────────────┐
         │    Railway Cloud     │
         │                      │
         │ Excel-AI-Knowledge   │
         │    Generator         │
         └──────────────────────┘
```

## 1. Railway 서비스 배포

### 1.1 ExcelApp (Next.js) 배포

1. Railway 대시보드에서 새 프로젝트 생성
2. GitHub 레포지토리 연결: `excelapp` 폴더
3. 환경변수 설정:

```env
# 필수 환경변수
DATABASE_URL=postgresql://...  # Railway가 자동 제공
NEXTAUTH_SECRET=your-random-secret-string
NEXTAUTH_URL=https://your-app.railway.app

# AI 서비스
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
GOOGLE_AI_API_KEY=...
OPENROUTER_API_KEY=sk-or-...

# OAuth (선택사항)
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
KAKAO_CLIENT_ID=...
KAKAO_CLIENT_SECRET=...

# PipeData 연동
PIPEDATA_API_TOKEN=your-secure-token-for-pipedata
NEXT_PUBLIC_APP_URL=https://your-app.railway.app
```

### 1.2 ExcelApp-Rails 배포

1. Railway 대시보드에서 새 프로젝트 생성
2. GitHub 레포지토리 연결: `excelapp-rails` 폴더
3. PostgreSQL과 Redis 서비스 추가
4. 환경변수 설정:

```env
# Rails 필수
RAILS_ENV=production
RAILS_MASTER_KEY=your-rails-master-key
SECRET_KEY_BASE=your-secret-key-base
DATABASE_URL=${{Postgres.DATABASE_URL}}
REDIS_URL=${{Redis.REDIS_URL}}
RAILS_HOST=your-rails-app.railway.app

# AI 서비스
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
GOOGLE_AI_API_KEY=...
OPENROUTER_API_KEY=sk-or-...

# OAuth
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
KAKAO_CLIENT_ID=...
KAKAO_CLIENT_SECRET=...

# PipeData 연동
PIPEDATA_API_TOKEN=your-secure-token-for-pipedata

# 관리자
ADMIN_EMAILS=admin@example.com
```

### 1.3 Excel-AI-Knowledge-Generator 배포

1. Railway 대시보드에서 새 프로젝트 생성
2. GitHub 레포지토리 연결: `excel-ai-knowledge-generator` 폴더
3. PostgreSQL 서비스 추가
4. 환경변수 설정:

```env
# Rails 필수
RAILS_ENV=production
RAILS_MASTER_KEY=your-rails-master-key
SECRET_KEY_BASE=your-secret-key-base
DATABASE_URL=${{Postgres.DATABASE_URL}}
RAILS_HOST=your-knowledge-app.railway.app

# AI 서비스
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
```

## 2. 로컬 PipeData 설정

### 2.1 환경 준비

1. Chrome 브라우저 설치
2. ChromeDriver 설치:
   ```bash
   # macOS
   brew install chromedriver
   
   # Ubuntu/Debian
   sudo apt-get install chromium-chromedriver
   ```

3. Tesseract OCR 설치:
   ```bash
   # macOS
   brew install tesseract
   
   # Ubuntu/Debian
   sudo apt-get install tesseract-ocr
   ```

### 2.2 환경변수 설정

`.env` 파일 생성 (`.env.local` 참고):

```bash
cd /Users/kevin/pipedata/new_system
cp .env.local .env
```

Railway 서비스 URL 업데이트:
```env
# Railway에 배포된 서비스들의 실제 URL로 변경
EXCELAPP_API_URL=https://excelapp-production.up.railway.app/api/training/pipedata
RAILS_API_URL=https://excelapp-rails-production.up.railway.app/api/v1/pipedata

# 토큰은 Railway 서비스와 동일하게 설정
EXCELAPP_API_TOKEN=your-secure-token-for-pipedata
RAILS_API_TOKEN=your-secure-token-for-pipedata
```

### 2.3 PipeData 실행

```bash
cd /Users/kevin/pipedata/new_system

# Python 가상환경 생성 및 활성화
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 의존성 설치
pip install -r ../requirements-full.txt

# API 서버 시작
python api_server.py
```

## 3. 통합 테스트

### 3.1 연결 테스트

```bash
# PipeData API 헬스체크
curl http://localhost:8000/api/health

# Railway 서비스 연결 테스트
curl -X POST http://localhost:8000/api/sync-to-excelapp \
  -H "Authorization: Bearer your-admin-token" \
  -H "Content-Type: application/json"
```

### 3.2 데이터 수집 테스트

```bash
# 파이프라인 실행 (작은 배치로 테스트)
curl -X POST http://localhost:8000/api/run-pipeline \
  -H "Authorization: Bearer your-admin-token" \
  -H "Content-Type: application/json" \
  -d '{
    "max_pages": 5,
    "target_count": 10,
    "sources": ["stackoverflow", "reddit", "oppadu"]
  }'
```

## 4. 운영 가이드

### 4.1 로컬 서버 관리

**systemd 서비스 생성 (Linux)**:
```bash
sudo nano /etc/systemd/system/pipedata.service
```

```ini
[Unit]
Description=PipeData API Server
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/Users/kevin/pipedata/new_system
Environment="PATH=/Users/kevin/pipedata/new_system/venv/bin"
ExecStart=/Users/kevin/pipedata/new_system/venv/bin/python api_server.py
Restart=always

[Install]
WantedBy=multi-user.target
```

**시작 및 활성화**:
```bash
sudo systemctl start pipedata
sudo systemctl enable pipedata
```

### 4.2 모니터링

1. **로컬 PipeData 로그**:
   ```bash
   tail -f /Users/kevin/pipedata/logs/pipeline.log
   ```

2. **Railway 서비스 로그**:
   - Railway 대시보드에서 각 서비스의 로그 확인

3. **동기화 상태**:
   ```bash
   curl http://localhost:8000/api/sync-status
   ```

### 4.3 백업

1. **로컬 데이터베이스 백업**:
   ```bash
   cp /Users/kevin/pipedata/data/combined_dataset.db \
      /Users/kevin/pipedata/backups/combined_dataset_$(date +%Y%m%d).db
   ```

2. **Railway PostgreSQL 백업**:
   - Railway 대시보드에서 데이터베이스 백업 설정

## 5. 트러블슈팅

### 문제: Selenium 오류
```bash
# ChromeDriver 버전 확인
chromedriver --version

# Chrome 브라우저 버전과 일치하는지 확인
google-chrome --version
```

### 문제: Railway 서비스 연결 실패
1. Railway 서비스 URL 확인
2. API 토큰 일치 여부 확인
3. CORS 설정 확인

### 문제: 메모리 부족
`.env` 파일에서 조정:
```env
MEMORY_LIMIT_MB=1024  # 줄이기
MAX_WORKERS=2         # 줄이기
SYNC_BATCH_SIZE=25    # 줄이기
```

## 6. 보안 고려사항

1. **API 토큰**:
   - 강력한 랜덤 토큰 사용
   - 정기적으로 로테이션
   - 환경변수로만 관리

2. **네트워크**:
   - HTTPS 통신 필수
   - 로컬 방화벽 설정
   - VPN 사용 권장

3. **데이터**:
   - 정기적인 백업
   - 민감한 데이터 암호화
   - 접근 로그 모니터링

## 7. 비용 최적화

1. **Railway 사용량**:
   - 필요시에만 서비스 활성화
   - 개발/스테이징 환경 분리
   - 리소스 제한 설정

2. **로컬 리소스**:
   - 야간에만 대량 수집 실행
   - 캐시 적극 활용
   - 오래된 데이터 정리

이 하이브리드 아키텍처를 통해 OppaduCrawler의 Selenium 요구사항을 충족하면서도 주요 서비스들을 클라우드에서 안정적으로 운영할 수 있습니다.