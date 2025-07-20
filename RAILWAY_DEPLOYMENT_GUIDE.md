# PipeData Railway 배포 가이드

## 개요

PipeData 시스템을 Railway 클라우드 플랫폼에 배포하여 ExcelApp과 자동 동기화하는 가이드입니다.

## 아키텍처

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   데이터 소스    │    │  PipeData      │    │   ExcelApp     │
│ (SO, Reddit 등) │───▶│  (Railway)     │───▶│  (Your Host)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                            │
                            ▼
                       ┌─────────────────┐
                       │   Dashboard    │
                       │   (모니터링)    │
                       └─────────────────┘
```

## 1. Railway 프로젝트 생성

### 1.1 Railway 계정 및 프로젝트 설정

```bash
# Railway CLI 설치
npm install -g @railway/cli

# 로그인
railway login

# 프로젝트 생성
railway create pipedata-excel-sync
```

### 1.2 GitHub 연동

1. Railway 대시보드에서 "Deploy from GitHub repo" 선택
2. `/Users/kevin/pipedata` 리포지토리 연결
3. `new_system/api_server.py`를 메인 서비스로 설정

## 2. 환경 변수 설정

Railway 프로젝트 설정에서 다음 환경 변수를 설정하세요:

### 2.1 필수 환경 변수

```bash
# ExcelApp 연동
EXCELAPP_API_URL=https://your-excelapp-domain.com/api/training/pipedata
EXCELAPP_API_TOKEN=your-secure-api-token-here

# 동기화 설정
SYNC_BATCH_SIZE=50
SYNC_INTERVAL_HOURS=6
QUALITY_THRESHOLD=7.0
SYNC_MODE=continuous

# AI API 키
OPENAI_API_KEY=sk-your-openai-key
ANTHROPIC_API_KEY=sk-ant-your-anthropic-key

# 성능 설정
MAX_WORKERS=4
MEMORY_LIMIT_MB=512
BATCH_SIZE=50

# Railway 설정
PORT=8000
RAILWAY_ENVIRONMENT=production
```

### 2.2 선택적 환경 변수

```bash
# 로깅
LOG_LEVEL=INFO

# 웹 스크래핑
USER_AGENT=PipeData-Railway-Bot/1.0
REQUEST_DELAY=2
MAX_RETRIES=3

# 보안
API_SECRET_KEY=your-secret-key
```

## 3. 데이터베이스 설정

### 3.1 SQLite 파일 시스템 (기본)

Railway는 임시 파일 시스템을 제공하므로 지속성을 위해 외부 저장소를 권장합니다.

### 3.2 PostgreSQL 연결 (권장)

```bash
# Railway에서 PostgreSQL 플러그인 추가
railway add postgresql

# 환경 변수 자동 설정됨
DATABASE_URL=${{ Postgres.DATABASE_URL }}
```

## 4. 배포 과정

### 4.1 코드 배포

```bash
# 로컬에서 배포
cd /Users/kevin/pipedata
railway up

# 또는 GitHub 자동 배포 설정
git push origin main
```

### 4.2 배포 확인

```bash
# 서비스 상태 확인
railway status

# 로그 확인
railway logs

# 도메인 확인
railway domain
```

## 5. ExcelApp 연동 설정

### 5.1 ExcelApp 환경 변수 업데이트

```bash
# ExcelApp .env 파일에 추가
PIPEDATA_API_URL=https://your-railway-domain.railway.app
PIPEDATA_API_TOKEN=your-secure-api-token
```

### 5.2 API 토큰 보안

1. 강력한 API 토큰 생성:
```bash
openssl rand -hex 32
```

2. Railway와 ExcelApp 양쪽에 동일한 토큰 설정

## 6. 동기화 테스트

### 6.1 수동 동기화 테스트

```bash
# PipeData API 호출
curl -X POST https://your-railway-domain.railway.app/api/sync-to-excelapp

# 동기화 상태 확인
curl https://your-railway-domain.railway.app/api/sync-status
```

### 6.2 통합 테스트 실행

```bash
# 로컬에서 통합 테스트
cd new_system
python integration_test.py
```

## 7. 모니터링 및 유지보수

### 7.1 헬스 체크

Railway가 자동으로 `/api/health` 엔드포인트를 모니터링합니다.

### 7.2 로그 모니터링

```bash
# 실시간 로그 확인
railway logs --tail

# 오류 로그 필터링
railway logs | grep ERROR
```

### 7.3 메트릭 확인

Railway 대시보드에서 다음을 모니터링:
- CPU 사용률
- 메모리 사용률
- 네트워크 트래픽
- 응답 시간

## 8. 자동 스케일링

### 8.1 리소스 제한 설정

```toml
# railway.toml
[deploy]
replicas = 1
healthcheckPath = "/api/health"
healthcheckTimeout = 30

[environments.production]
variables = { 
  MEMORY_LIMIT_MB = "512"
}
```

### 8.2 부하 분산

고부하 시 Railway Pro 플랜에서 자동 스케일링이 지원됩니다.

## 9. 백업 및 복구

### 9.1 데이터 백업

```bash
# SQLite 백업 (로컬)
cp data/combined_dataset.db data/backup_$(date +%Y%m%d).db

# PostgreSQL 백업 (Railway)
railway connect postgres
pg_dump database_name > backup.sql
```

### 9.2 배포 롤백

```bash
# 이전 배포로 롤백
railway rollback
```

## 10. 비용 최적화

### 10.1 리소스 튜닝

- **배치 크기**: 메모리 사용량에 따라 조정
- **워커 수**: CPU 코어 수에 맞춰 설정
- **동기화 간격**: 데이터 업데이트 빈도에 맞춰 조정

### 10.2 사용량 모니터링

Railway 대시보드에서 월간 사용량과 비용을 확인하세요.

## 11. 문제 해결

### 11.1 일반적인 문제

**배포 실패**
```bash
# 로그 확인
railway logs

# 환경 변수 확인
railway variables
```

**동기화 실패**
```bash
# ExcelApp API 연결 확인
curl -H "X-PipeData-Token: your-token" https://your-excelapp-domain.com/api/training/pipedata

# PipeData 서비스 상태 확인
curl https://your-railway-domain.railway.app/api/health
```

**메모리 부족**
```bash
# 메모리 제한 늘리기
railway variables set MEMORY_LIMIT_MB=1024
```

### 11.2 지원

- Railway 문서: https://docs.railway.app
- Railway Discord: https://discord.gg/railway
- 이 프로젝트 Issues: GitHub Issues

## 12. 보안 체크리스트

- [ ] API 토큰이 강력하고 안전하게 저장됨
- [ ] HTTPS를 통한 모든 통신
- [ ] 환경 변수에 민감한 정보 저장
- [ ] 정기적인 보안 업데이트
- [ ] 로그에서 민감한 정보 제거

## 결론

이 가이드를 따라 PipeData를 Railway에 성공적으로 배포하고 ExcelApp과 자동 동기화를 설정할 수 있습니다. 추가 질문이나 문제가 있으면 GitHub Issues를 통해 문의하세요.