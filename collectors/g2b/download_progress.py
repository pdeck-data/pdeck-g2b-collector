import os
from utils.drive import download_file
from utils.logger import log
from utils.slack import send_slack_message

LOCAL_PATH = "progress.json"
DRIVE_FILE_ID = os.getenv("GDRIVE_PROGRESS_FILE_ID")

if __name__ == "__main__":
    log("🔽 progress.json 다운로드 시작")

    if not DRIVE_FILE_ID:
        log("❌ ERROR: GDRIVE_PROGRESS_FILE_ID 환경변수가 없음")
        raise SystemExit(1)

    if os.path.exists(LOCAL_PATH):
        os.remove(LOCAL_PATH)
        log("🗑 기존 progress.json 삭제")

    success = download_file(DRIVE_FILE_ID, LOCAL_PATH)

    if success:
        log("✅ progress.json 다운로드 완료")
    else:
        log("⚠️ 다운로드 실패 — 기본 progress.json 사용")
