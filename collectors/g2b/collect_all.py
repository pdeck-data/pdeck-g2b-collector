import json
import os
from datetime import datetime

from utils.g2b_client import get_monthly_data
from utils.drive import upload_file
from utils.slack import send_slack_message
from utils.logger import log


# progress.json 경로 — 루트에서 사용
PROGRESS_PATH = "progress.json"


def load_progress():
    """progress.json 읽기"""
    if not os.path.exists(PROGRESS_PATH):
        log("⚠️ progress.json이 없어 기본값으로 시작합니다.")
        return {
            "current_업무": "물품",
            "current_year": 2024,
            "current_month": 1,
            "daily_api_calls": 500,
            "last_run_date": None,
            "total_collected": 0
        }

    with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
        progress = json.load(f)

    log(f"📋 progress.json 로드 완료 → {progress}")
    return progress


def save_progress(progress):
    """progress.json 저장"""
    with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)
    log("💾 progress.json 저장 완료")


def increment_month(year, month):
    """다음 월 계산"""
    if month == 12:
        return year + 1, 1
    return year, month + 1


if __name__ == "__main__":
    log("🚀 G2B 자동 수집 시작")

    # -------------------------------
    # 1) 진행 상태 로드
    # -------------------------------
    progress = load_progress()
    업무 = progress.get("current_업무", "물품")
    year = progress.get("current_year", 2024)
    month = progress.get("current_month", 1)
    total_collected = progress.get("total_collected", 0)

    # -------------------------------
    # 2) Slack — 수집 시작 메시지
    # -------------------------------
    send_slack_message(
        f":large_blue_circle: 데이터 수집 시작\n"
        f"• 업무: {업무}\n"
        f"• 진행: {year}년 {month}월\n"
        f"• 누적: {total_collected:,}건"
    )

    log(f"📌 현재 목표: {업무} {year}-{month}")

    # -------------------------------
    # 3) 수집 실행
    # -------------------------------
    try:
        items = get_monthly_data(year, month)
    except Exception as e:
        log(f"❌ API 오류: {e}")
        send_slack_message(f"❌ API 오류 발생: {e}")
        raise SystemExit(1)

    # -------------------------------
    # 4) 수집 건수 처리
    # -------------------------------
    new_count = len(items) if items else 0
    log(f"📈 신규 수집 건수: {new_count}건")

    progress["total_collected"] = total_collected + new_count

    # -------------------------------
    # 5) 다음 월 업데이트
    # -------------------------------
    next_year, next_month = increment_month(year, month)
    progress["current_year"] = next_year
    progress["current_month"] = next_month
    progress["last_run_date"] = datetime.now().strftime("%Y-%m-%d")

    log(f"➡️ 다음 실행 월: {next_year}-{next_month}")

    # -------------------------------
    # 6) progress.json 저장
    # -------------------------------
    save_progress(progress)

    # -------------------------------
    # 7) Slack — 완료 메시지
    # -------------------------------
    send_slack_message(
        f":white_check_mark: 데이터 수집 완료\n"
        f"• 처리: {업무} {year}년 {month}월\n"
        f"• 신규: {new_count:,}건\n"
        f"• 누적: {progress['total_collected']:,}건\n"
        f"• 다음: {next_year}년 {next_month}월"
    )
