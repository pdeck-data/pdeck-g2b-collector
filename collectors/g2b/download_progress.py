import os
from utils.drive import download_progress_json, test_drive_connection
from utils.logger import log

LOCAL_PATH = "progress.json"
FILE_ID = os.getenv("GDRIVE_PROGRESS_FILE_ID")


if __name__ == "__main__":
    log("🔽 Drive → progress.json 다운로드 시작")

    # 🔧 1. 환경변수 검증
    if not FILE_ID:
        log("❌ GDRIVE_PROGRESS_FILE_ID 환경변수가 설정되지 않음")
        log("   → GitHub Secrets 또는 .env 파일에서 설정 확인")
        raise SystemExit(1)

    # 🔧 2. Drive 연결 테스트 (선택사항)
    log("🔍 Google Drive 연결 테스트...")
    if not test_drive_connection():
        log("❌ Google Drive 연결 실패")
        log("   → service_account.json 파일 및 권한 확인")
        raise SystemExit(1)

    # 🔧 3. 기존 파일 제거 (선택사항)
    if os.path.exists(LOCAL_PATH):
        log(f"🗑️ 기존 파일 제거: {LOCAL_PATH}")
        os.remove(LOCAL_PATH)

    # 🔧 4. 개선된 다운로드 함수 사용
    log(f"📥 다운로드 시작: {FILE_ID[:20]}... → {LOCAL_PATH}")
    
    # 방법 1: progress 전용 함수 사용 (추천)
    progress_data = download_progress_json(FILE_ID, LOCAL_PATH)
    
    if progress_data is not None:
        log("✅ progress.json 다운로드 완료")
        
        # 🔧 5. 다운로드된 내용 요약 표시
        current_job = progress_data.get("current_job", "Unknown")
        current_year = progress_data.get("current_year", 0)
        current_month = progress_data.get("current_month", 0) 
        total_collected = progress_data.get("total_collected", 0)
        daily_api_calls = progress_data.get("daily_api_calls", 0)
        
        log(f"📋 Progress 현황:")
        log(f"   └─ 진행 위치: {current_job} {current_year}년 {current_month}월")
        log(f"   └─ 누적 수집: {total_collected:,}건")
        log(f"   └─ 오늘 API: {daily_api_calls}/500")
        
        # 🔧 6. 파일 크기 확인
        if os.path.exists(LOCAL_PATH):
            file_size = os.path.getsize(LOCAL_PATH)
            log(f"📄 파일 크기: {file_size} bytes")
        
    else:
        log("❌ progress.json 다운로드 실패")
        log("   → 파일 ID 확인: " + FILE_ID)
        log("   → 파일 공유 권한 확인")
        log("   → 기본 progress 데이터가 생성되었는지 확인")
        
        # 실패해도 기본값이 생성되었는지 확인
        if os.path.exists(LOCAL_PATH):
            log("ℹ️ 기본 progress.json이 생성되었습니다")
            raise SystemExit(0)  # 기본값으로라도 성공
        else:
            raise SystemExit(1)  # 완전 실패


    # 🔧 방법 2: 기존 방식도 남겨두기 (호환성용)
    # from utils.drive import download_file
    # success = download_file(FILE_ID, LOCAL_PATH)
    # if success:
    #     log("✅ progress.json 다운로드 완료")  
    # else:
    #     log("❌ progress.json 다운로드 실패")
    #     raise SystemExit(1)