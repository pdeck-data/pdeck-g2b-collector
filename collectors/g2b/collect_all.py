import json
import os
from utils.g2b_client import get_monthly_data
from utils.logger import log
from utils.slack import send_slack_message

PROGRESS_PATH = "progress.json"


def load_progress():
    if not os.path.exists(PROGRESS_PATH):
        log("⚠️ progress.json 없음 — 기본값 사용")
        return {"current_year": 2014, "current_month": 3, "total_collected": 0}

    with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_progress(progress):
    with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def increment_month(year, month):
    if month == 12:
        return year + 1, 1
    return year, month + 1


if __name__ == "__main__":
    progress = load_progress()
    year = progress["current_year"]
    month = progress["current_month"]

    # ▣ Slack 시작 메시지
    send_slack_message(
        f"📡 G2B Auto Collector — 실행 시작\n\n"
        f"• 기간: {year}년 {month}월\n"
        f"• 업무: 물품\n"
        f"• 누적 건수: {progress.get('total_collected',0)}건\n"
        f"• 실행 환경: GitHub Actions (UTC+9)\n\n"
        "────────────────────\n\n"
        "⏳ Collecting data..."
    )

    # 수집
    try:
        items = get_monthly_data(year, month)
    except Exception as e:
        log(f"❌ API 오류: {e}")
        items = []

    if items:
        progress["total_collected"] += len(items)

    next_year, next_month = increment_month(year, month)

    progress["current_year"] = next_year
    progress["current_month"] = next_month

    save_progress(progress)

    # ▣ Slack 종료 메시지
    send_slack_message(
        f"✅ G2B Auto Collector — 실행 완료\n\n"
        f"• 처리 월: {year}-{month}\n"
        f"• 신규 수집: {len(items)}건\n"
        f"• 누적 건수: {progress['total_collected']}건\n"
        f"• 다음 예정: {next_year}-{next_month}\n\n"
        "────────────────────\n\n"
        "🔄 progress.json 저장 및 Google Drive 동기화 대기"
    )

    log("✔ 전체 수집 종료")
