import json
import os

from utils.g2b_client import get_monthly_data
from utils.drive import download_file, upload_file
from utils.slack import send_slack_message
from utils.logger import log


# progress.json 경로 통일
PROGRESS_PATH = "progress.json"


def load_progress():
    """progress.json 읽기"""
    if not os.path.exists(PROGRESS_PATH):
        log("⚠️ progress.json 파일이 없어 기본 설정을 사용합니다.")
        return {"current_year": 2024, "current_month": 1, "total_collected": 0}

    with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
        progress = json.load(f)

    # 디버깅: 현재 키 출력
    log(f"📋 progress.json의 키들: {list(progress.keys())}")
    log(f"📋 progress.json 내용: {progress}")

    return progress


def save_progress(progress):
    """진행 상황 저장"""
    with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def increment_month(year, month):
    """다음 수집 월 계산"""
    if month == 12:
        return year + 1, 1
    return year, month + 1


if __name__ == "__main__":
    log("🚀 G2B 자동 수집 시작")

    # 1. 이전 진행 상황 로드
    progress = load_progress()
    year = progress.get("current_year", 2024)  # 실제 키 이름 사용
    month = progress.get("current_month", 1)   # 실제 키 이름 사용

    log(f"📌 현재 진행 월: {year}-{month}")

    # 2. 해당 월 데이터 수집 실행
    try:
        items = get_monthly_data(year, month)
    except Exception as e:
        log(f"❌ API 수집 중 오류 발생: {e}")
        items = []

    # 3. 수집 결과 로그
    if items:
        log(f"📈 신규 수집 건수: {len(items)}건")
        # 기존 total_collected에 추가
        progress["total_collected"] = progress.get(
            "total_collected", 0) + len(items)
    else:
        log("ℹ️ 신규 데이터 없음 또는 수집 실패")

    # 4. 다음 달로 업데이트
    next_year, next_month = increment_month(year, month)
    progress["current_year"] = next_year    # 실제 키 이름 사용
    progress["current_month"] = next_month  # 실제 키 이름 사용
    progress["last_run_date"] = "2025-12-15"  # 오늘 날짜로 업데이트

    log(f"➡️ 다음 진행 월: {next_year}-{next_month}")

    # 5. 업데이트된 progress.json 저장
    save_progress(progress)
    log("💾 progress.json 저장 완료")

    # 6. Slack 알림
    send_slack_message(
        f"G2B 수집 완료: {year}-{month} → 다음: {next_year}-{next_month}"
    )
