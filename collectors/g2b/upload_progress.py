import sys
import os

# utils 경로 등록
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utils.drive import upload_file
from utils.logger import log
from utils.slack import send_slack_message


LOCAL_PATH = "progress.json"
FILE_ID = os.getenv("GDRIVE_PROGRESS_FILE_ID")


if __name__ == "__main__":

    log("📤 progress.json 업로드 시작")

    if not FILE_ID:
        log("❌ 환경변수 GDRIVE_PROGRESS_FILE_ID 없음")
        raise SystemExit(1)

    if not os.path.exists(LOCAL_PATH):
        log("❌ progress.json 파일 없음 → 업로드 불가")
        raise SystemExit(1)

    ok = upload_file(LOCAL_PATH, FILE_ID)

    if ok:
        log("✔ progress.json 업로드 완료")
        send_slack_message("📤 progress.json 업로드 성공")
    else:
        log("⚠ 업로드 실패")
        send_slack_message("⚠ progress.json 업로드 실패")
