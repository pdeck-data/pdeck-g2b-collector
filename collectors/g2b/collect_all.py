from utils.drive import download_progress_json, upload_progress_json
from utils.g2b_client import fetch_raw_data, append_to_year_file
from utils.slack import send_slack_message
from utils.logger import log
import os
import sys
import datetime

# 프로젝트 루트 경로 추가 (collectors/g2b에서 2단계 상위로)
project_root = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)


def get_korea_date():
    """한국 시간 기준 날짜 반환"""
    from datetime import datetime, timedelta
    # UTC + 9시간
    korea_time = datetime.utcnow() + timedelta(hours=9)
    return korea_time.strftime('%Y-%m-%d')


if __name__ == "__main__":
    log("🚀 G2B 기업 매출 데이터 수집 시작")

    # 1. 초기화 및 Progress 로드
    if not os.getenv("API_KEY"):
        log("❌ API_KEY 환경변수 누락")
        sys.exit(1)

    progress = download_progress_json(os.getenv("GDRIVE_PROGRESS_FILE_ID"))
    if not progress:
        log("❌ Progress 로드 실패")
        sys.exit(1)

    # 날짜 변경 시 API 카운트 리셋 로직
    today = get_korea_date()
    if progress.get("last_api_reset_date") != today:
        progress["daily_api_calls"] = 0
        progress["last_api_reset_date"] = today
        log(f"📅 새로운 날 시작 - API 카운트 리셋: {today}")

    job = progress["current_job"]
    year = progress["current_year"]
    month = progress["current_month"]

    # 2. 수집 진행 여부 판단
    if progress['daily_api_calls'] >= 500:
        log("🛑 일일 API 한도 도달. 수집 종료.")
        sys.exit(0)

    send_slack_message(
        f"🚀 수집 시작: {job} {year}-{month} (API: {progress['daily_api_calls']}/500)")

    # 3. API 호출 (개선된 fetch_raw_data 사용)
    log(f"📞 API 호출 중: {job} {year}-{month:02d}")
    result = fetch_raw_data(job, year, month)

    # ==========================================
    # 🎯 핵심: 결과 코드(Code)에 따른 분기 처리
    # ==========================================

    should_update_progress = False
    slack_msg = ""

    if result['success']:
        # Case A: 데이터 있음 (Code 00)
        if result['code'] == '00' and result['data']:
            save_path = append_to_year_file(job, year, result['data'])
            log(f"✅ 저장 완료: {result['count']}건")
            slack_msg = f"✅ 수집 성공 ({year}-{month:02d}): {result['count']}건 저장"
            should_update_progress = True

        # Case B: 데이터 없음 (Code 03) - 에러 아님! 진행해야 함
        elif result['code'] == '03':
            log(f"ℹ️ 데이터 없음 ({year}-{month:02d}). 다음 달로 넘어갑니다.")
            slack_msg = f"⏩ 수집 건너뜀 ({year}-{month:02d}): 데이터 없음 (정상)"
            should_update_progress = True

    else:
        # Case C: 실패 (트래픽 초과, 서버 에러 등)
        log(f"❌ 수집 실패: {result['msg']}")
        slack_msg = f"❌ 수집 실패 ({year}-{month:02d}): {result['msg']} (코드: {result.get('code')})"

        # TIMEOUT 에러인 경우 특별 처리
        if result.get('code') == 'TIMEOUT':
            slack_msg += "\n⏱️ 서버 응답 지연 - 다음 실행에서 재시도"

        should_update_progress = False  # 🛑 Progress 유지 -> 재시도 유도

    # 4. Progress 업데이트 (성공 or 데이터없음 일 때만)
    if should_update_progress:
        progress["total_collected"] += result.get('count', 0)
        progress["daily_api_calls"] += 1
        progress["last_run_date"] = get_korea_date()

        # 날짜 증가 로직
        next_y, next_m = (year + 1, 1) if month == 12 else (year, month + 1)
        progress["current_year"] = next_y
        progress["current_month"] = next_m

        # Drive 업로드
        if upload_progress_json(progress, os.getenv("GDRIVE_PROGRESS_FILE_ID")):
            slack_msg += f"\n📅 다음 일정: {next_y}-{next_m:02d}"
        else:
            slack_msg += f"\n⚠️ Drive 저장 실패 (로컬만 갱신됨)"

    else:
        # 실패 시 API 카운트만 늘리고 날짜는 유지
        progress["daily_api_calls"] += 1
        upload_progress_json(progress, os.getenv("GDRIVE_PROGRESS_FILE_ID"))
        slack_msg += "\n🔄 다음 실행에서 재시도합니다."

    # 5. 최종 알림
    send_slack_message(slack_msg)
    log("🏁 수집 스크립트 종료")
