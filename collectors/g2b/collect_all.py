import sys
import os
import json
from datetime import datetime

# ==================================
# 🔥 utils 경로 강제 등록 (중요!)
# ==================================
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utils.g2b_client import fetch_raw_data, append_to_year_file
from utils.logger import log
from utils.slack import send_slack_message
from utils.drive import upload_file


PROGRESS_PATH = "progress.json"


def load_progress():
    """progress.json 로드"""
    if not os.path.exists(PROGRESS_PATH):
        log("⚠ progress.json 없음 → 기본값 사용")
        return {
            "current_업무": "물품",
            "current_year": 2014,
            "current_month": 1,
            "daily_api_calls": 500,
            "total_collected": 0,
            "last_run_date": None,
        }

    with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_progress(p):
    with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
        json.dump(p, f, ensure_ascii=False, indent=2)


def next_month(year, month):
    if month == 12:
        return year + 1, 1
    return year, month + 1


if __name__ == "__main__":
    log("🚀 G2B 자동 수집 시작")

    p = load_progress()

    업무 = p["current_업무"]
    year = p["current_year"]
    month = p["current_month"]

    send_slack_message(f"""
🚀 G2B 수집 시작
• 업무: {업무}
• 위치: {year}년 {month}월
• 누적: {p['total_collected']:,}건

perl
코드 복사
""")

    try:
        # 원본 데이터 다운로드
        xml_path = fetch_raw_data(업무, year, month)
        log(f"📁 다운로드 완료: {xml_path}")

        # 연 단위 파일에 append
        count = append_to_year_file(xml_path, year)
        log(f"📈 신규 {count}건 추가됨")

        # 누적 증가
        p["total_collected"] += count

    except Exception as e:
        send_slack_message(f"❌ 수집 오류 발생: {e}")
        raise

    # 날짜, 위치 업데이트
    p["current_year"], p["current_month"] = next_month(year, month)
    p["last_run_date"] = datetime.now().strftime("%Y-%m-%d")

    save_progress(p)
    log("💾 progress.json 저장 완료")

    # Slack 완료 메시지
    send_slack_message(f"""
✔ G2B 수집 완료
• 처리: {year}년 {month}월
• 신규: {count:,}건
• 누적: {p["total_collected"]:,}건
• 다음: {p["current_year"]}-{p["current_month"]}

bash
코드 복사
""")
