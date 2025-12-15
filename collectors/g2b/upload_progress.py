import os
from utils.drive import upload_file
from utils.logger import log
from utils.slack import send_slack_message

LOCAL_PATH = "progress.json"
DRIVE_FILE_ID = os.getenv("GDRIVE_PROGRESS_FILE_ID")

if __name__ == "__main__":
    log("📤 progress.json 업로드 시작")

    if not DRIVE_FILE_ID:
        log("❌ ERROR: GDRIVE_PROGRESS_FILE_ID 환경변수 없음")
        raise SystemExit(1)

    if not os.path.exists(LOCAL_PATH):
        log("❌ progress.json 파일 없음 — 업로드 불가")
        raise SystemExit(1)

    success = upload_file(LOCAL_PATH, DRIVE_FILE_ID)

    if success:
        log("✅ Google Drive 업로드 완료")
        send_slack_message("📁 progress.json 동기화 완료 (Google Drive 업데이트됨)")
    else:
        log("⚠️ 업로드 실패")
        send_slack_message("⚠️ progress.json 업로드 실패 — 상태 저장 불완전")
