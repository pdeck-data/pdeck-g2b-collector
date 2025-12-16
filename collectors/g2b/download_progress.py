import sys
import os

# utils 경로 강제 등록
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utils.drive import download_file
from utils.logger import log
from utils.slack import send_slack_message


LOCAL_PATH = "progress.json"
FILE_ID = os.getenv("GDRIVE_PROGRESS_FILE_ID")


if __name__ == "__main__":

    log("🔽 Drive → progress.json 다운로드 시작")

    if not FILE_ID:
        msg = "❌ GDRIVE_PROGRESS_FILE_ID 환경변수가 없습니다."
        log(msg)
        send_slack_message(msg)
        raise SystemExit(1)

    if os.path.exists(LOCAL_PATH):
        os.remove(LOCAL_PATH)
        log("🗑 기존 progress.json 삭제")

    ok = download_file(FILE_ID, LOCAL_PATH)

    if ok:
        log("✔ progress.json 다운로드 완료")
        send_slack_message("🔽 progress.json 다운로드 성공")
    else:
        log("⚠ 다운로드 실패 → 기본값 사용")
        send_slack_message("⚠ progress.json 다운로드 실패 → 기본값 적용")
