import os
import json
from utils.drive import upload_progress_json, upload_file, test_drive_connection
from utils.logger import log

LOCAL_PATH = "progress.json"
FILE_ID = os.getenv("GDRIVE_PROGRESS_FILE_ID")


def validate_progress_file(file_path):
    """
    🔧 새로 추가: progress.json 파일 검증
    """
    try:
        if not os.path.exists(file_path):
            return False, "파일이 존재하지 않음"
            
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            return False, "파일이 비어있음"
            
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # 필수 필드 확인
        required_fields = ["current_job", "current_year", "current_month", "total_collected", "daily_api_calls"]
        for field in required_fields:
            if field not in data:
                return False, f"필수 필드 누락: {field}"
                
        # 데이터 타입 확인
        if not isinstance(data["current_year"], int) or data["current_year"] < 2000:
            return False, f"잘못된 연도: {data['current_year']}"
            
        if not isinstance(data["current_month"], int) or not (1 <= data["current_month"] <= 12):
            return False, f"잘못된 월: {data['current_month']}"
            
        if not isinstance(data["total_collected"], int) or data["total_collected"] < 0:
            return False, f"잘못된 누적 수집 건수: {data['total_collected']}"
            
        return True, data
        
    except json.JSONDecodeError as e:
        return False, f"JSON 파싱 오류: {e}"
    except Exception as e:
        return False, f"파일 검증 오류: {e}"


if __name__ == "__main__":
    log("🔼 progress.json → Drive 업로드 시작")

    # 🔧 1. 환경변수 검증
    if not FILE_ID:
        log("❌ GDRIVE_PROGRESS_FILE_ID 환경변수가 설정되지 않음")
        log("   → GitHub Secrets 또는 .env 파일에서 설정 확인")
        raise SystemExit(1)

    # 🔧 2. 로컬 파일 존재 및 유효성 검증
    log(f"🔍 로컬 파일 검증: {LOCAL_PATH}")
    
    is_valid, result = validate_progress_file(LOCAL_PATH)
    
    if not is_valid:
        log(f"❌ progress.json 검증 실패: {result}")
        log("   → collect_all.py 실행으로 올바른 파일 생성 필요")
        raise SystemExit(1)
        
    progress_data = result
    log("✅ progress.json 검증 완료")
    
    # 🔧 3. 업로드할 내용 요약 표시
    current_job = progress_data.get("current_job", "Unknown")
    current_year = progress_data.get("current_year", 0)
    current_month = progress_data.get("current_month", 0)
    total_collected = progress_data.get("total_collected", 0)
    daily_api_calls = progress_data.get("daily_api_calls", 0)
    
    log(f"📋 업로드할 Progress 현황:")
    log(f"   └─ 진행 위치: {current_job} {current_year}년 {current_month}월")
    log(f"   └─ 누적 수집: {total_collected:,}건")
    log(f"   └─ 오늘 API: {daily_api_calls}/500")
    
    file_size = os.path.getsize(LOCAL_PATH)
    log(f"   └─ 파일 크기: {file_size} bytes")

    # 🔧 4. Drive 연결 테스트 (선택사항)
    log("🔍 Google Drive 연결 테스트...")
    if not test_drive_connection():
        log("❌ Google Drive 연결 실패")
        log("   → service_account.json 파일 및 권한 확인")
        raise SystemExit(1)

    # 🔧 5. 개선된 업로드 함수 사용
    log(f"📤 업로드 시작: {LOCAL_PATH} → {FILE_ID[:20]}...")
    
    # 방법 1: progress 전용 함수 사용 (추천)
    success = upload_progress_json(progress_data, FILE_ID, LOCAL_PATH)
    
    if success:
        log("✅ progress.json 업로드 완료")
        log(f"🔗 파일 확인: https://drive.google.com/file/d/{FILE_ID}/view")
        
    else:
        log("❌ progress.json 업로드 실패")
        log("   → 파일 ID 확인: " + FILE_ID)
        log("   → 서비스 계정 권한 확인")
        log("   → Google Drive API 할당량 확인")
        
        # 🔧 6. 대안: 기존 방식으로 재시도
        log("🔄 기존 방식으로 재시도...")
        
        success_fallback = upload_file(LOCAL_PATH, FILE_ID, create_if_not_exists=True)
        
        if success_fallback:
            log("✅ 기존 방식으로 업로드 성공")
        else:
            log("❌ 모든 업로드 방식 실패")
            raise SystemExit(1)
            
    # 🔧 7. 업로드 후 검증 (선택사항)
    log("🔍 업로드 후 검증...")
    
    # 간단한 검증: 다운로드해서 내용 비교
    try:
        from utils.drive import download_progress_json
        
        downloaded_data = download_progress_json(FILE_ID, f"{LOCAL_PATH}.verify")
        
        if downloaded_data:
            # 주요 필드만 비교
            upload_ok = (
                downloaded_data.get("current_year") == progress_data.get("current_year") and
                downloaded_data.get("current_month") == progress_data.get("current_month") and
                downloaded_data.get("total_collected") == progress_data.get("total_collected")
            )
            
            if upload_ok:
                log("✅ 업로드 검증 완료")
                # 검증용 파일 삭제
                verify_file = f"{LOCAL_PATH}.verify"
                if os.path.exists(verify_file):
                    os.remove(verify_file)
            else:
                log("⚠ 업로드 검증 실패: 데이터 불일치")
        else:
            log("⚠ 업로드 검증 실패: 다운로드 불가")
            
    except Exception as e:
        log(f"⚠ 업로드 검증 중 오류 (업로드는 성공): {e}")

    log("✅ 전체 업로드 프로세스 완료")


    # 🔧 방법 2: 기존 방식도 남겨두기 (호환성용)  
    # success = upload_file(LOCAL_PATH, FILE_ID)
    # if success:
    #     log("✅ progress.json 업로드 완료")
    # else:
    #     log("❌ progress.json 업로드 실패")
    #     raise SystemExit(1)