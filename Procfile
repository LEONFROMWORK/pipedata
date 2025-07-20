# Railway 배포용 Procfile

# 웹 서비스 (API 서버)
web: cd new_system && python api_server.py

# ExcelApp 동기화 작업 (백그라운드)
sync: cd new_system && python -c "from excelapp_sync import main; main()"

# 데이터 수집 작업 (백그라운드)
collect: cd new_system && python main.py --mode continuous