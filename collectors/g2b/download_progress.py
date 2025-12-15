import os
from utils.drive import download_file
from utils.logger import log
from utils.slack import send_slack_message

# progress.json 로컬 저장 위치
LOCAL_PATH = "collectors/g2b/progress.json"

# Google Drive File ID (GitHub Secrets에서 불러옴)
DRIVE_FILE_ID = os.getenv("GDRIVE_PROGRESS_FILE_ID")

if __name__ == "__main__":
    log("🔽 Downloading progress.json from Google Drive...")

    # Drive 파일 ID 누락 체크
    if not DRIVE_FILE_ID:
        log("❌ ERROR: 환경변수 GDRIVE_PROGRESS_FILE_ID가 설정되지 않았습니다.")
        raise SystemExit(1)

    # 로컬 파일 존재하면 삭제 (Drive 버전을 항상 우선 적용)
    if os.path.exists(LOCAL_PATH):
        os.remove(LOCAL_PATH)
        log("🗑 기존 progress.json 삭제 완료")

    success = download_file(DRIVE_FILE_ID, LOCAL_PATH)

    if success:
        log("✅ progress.json 다운로드 완료")
    else:
        log("⚠️ progress.json 다운로드 실패 — 기본 progress.json이 사용될 수 있음")
