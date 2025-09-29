import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
import time
import json
import sys

load_dotenv()

SERVICE_KEY = os.getenv('API_KEY')
SLACK_TOKEN = os.getenv('SLACK_TOKEN')
SLACK_CHANNEL_ID = os.getenv('SLACK_CHANNEL_ID')
BASE_URL = "http://apis.data.go.kr/1230000/ao/CntrctInfoService"

PROGRESS_FILE = 'data/logs/progress.json'
MAX_DAILY_CALLS = 500

# ✅ 한국 시간대 설정
KST = timezone(timedelta(hours=9))


def send_slack_message(message, is_error=False):
    """Slack Bot Token으로 메시지 전송"""
    if not SLACK_TOKEN or not SLACK_CHANNEL_ID:
        return

    emoji = "🔴" if is_error else "✅"

    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "channel": SLACK_CHANNEL_ID,
        "text": f"{emoji} {message}",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{emoji} *{message.split('*')[1] if '*' in message else 'API 데이터 수집'}*"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message.replace('*', '')
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"🤖 API 데이터 수집 봇 | {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}"
                    }
                ]
            }
        ]
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        result = response.json()
        if not result.get('ok'):
            print(f"⚠️ Slack 메시지 전송 실패: {result.get('error')}")
    except Exception as e:
        print(f"⚠️ Slack 오류: {e}")


def load_progress():
    """진행 상황 불러오기"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            progress = json.load(f)

            today = datetime.now(KST).strftime('%Y-%m-%d')
            if progress.get('last_run_date') != today:
                progress['daily_api_calls'] = 0
                progress['last_run_date'] = today

            return progress

    return {
        'current_업무': '물품',
        'current_year': 2005,
        'current_month': 1,
        'daily_api_calls': 0,
        'last_run_date': datetime.now(KST).strftime('%Y-%m-%d'),
        'total_collected': 0
    }


def save_progress(progress):
    """진행 상황 저장"""
    os.makedirs('data/logs', exist_ok=True)
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def get_month_data(업무코드, year, month, progress, max_retries=3):
    """특정 월 데이터 수집"""
    endpoint = f"/getCntrctInfoList{업무코드}"
    url = BASE_URL + endpoint

    month_start = f"{year}{month:02d}010000"

    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    last_day = (next_month - relativedelta(days=1)).day
    month_end = f"{year}{month:02d}{last_day}2359"

    all_items = []
    page = 1

    while True:
        if progress['daily_api_calls'] >= MAX_DAILY_CALLS:
            print(
                f"\n⚠️ 일일 API 호출 제한 도달! ({progress['daily_api_calls']}/{MAX_DAILY_CALLS})")
            return None

        params = {
            'serviceKey': SERVICE_KEY,
            'numOfRows': 999,
            'pageNo': page,
            'inqryDiv': '1',
            'inqryBgnDt': month_start,
            'inqryEndDt': month_end
        }

        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                progress['daily_api_calls'] += 1

                if '<resultCode>00</resultCode>' in response.text:
                    if '<item>' not in response.text:
                        return all_items

                    all_items.append(response.text)
                    page += 1
                    time.sleep(0.5)
                    break
                else:
                    if attempt < max_retries - 1:
                        print(
                            f"      ⚠️ API 에러 (재시도 {attempt + 1}/{max_retries})")
                        time.sleep(3)
                    else:
                        print(f"      ⚠️ API 에러 (페이지 {page})")
                        return all_items

            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"      ⚠️ 오류 (재시도 {attempt + 1}/{max_retries})")
                    time.sleep(3)
                else:
                    print(f"      ❌ 오류: {str(e)[:100]}")
                    return all_items

    return all_items


def collect_with_resume():
    """중단 지점부터 재개 가능한 수집"""
    start_time = datetime.now(KST)

    print("="*70)
    print("🚀 계약 데이터 수집 (자동 재개)")
    print("="*70)

    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/logs', exist_ok=True)

    progress = load_progress()
    today_start_count = progress['daily_api_calls']

    print(f"\n📊 진행 상황:")
    print(f"   - 현재 업무: {progress['current_업무']}")
    print(
        f"   - 현재 위치: {progress['current_year']}년 {progress['current_month']}월")
    print(f"   - 오늘 API 호출: {progress['daily_api_calls']}/{MAX_DAILY_CALLS}")
    print(f"   - 누적 수집: {progress.get('total_collected', 0):,}건\n")

    # Slack 시작 알림
    send_slack_message(
        f"*데이터 수집 시작*\n\n"
        f"• 업무: `{progress['current_업무']}`\n"
        f"• 위치: `{progress['current_year']}년 {progress['current_month']}월`\n"
        f"• 누적: `{progress.get('total_collected', 0):,}건`"
    )

    업무구분 = {
        '물품': 'Thng',
        '용역': 'Servc',
        '공사': 'Cnstwk'
    }

    업무_리스트 = list(업무구분.keys())
    start_idx = 업무_리스트.index(progress['current_업무'])

    end_year = datetime.now(KST).year
    today_collected = 0

    for 이름 in 업무_리스트[start_idx:]:
        코드 = 업무구분[이름]
        print(f"\n{'='*70}")
        print(f"📦 {이름} ({코드}) 수집 중...")
        print(f"{'='*70}")

        start_year = progress['current_year'] if 이름 == progress['current_업무'] else 2005

        for year in range(start_year, end_year + 1):
            filename = f"data/raw/{이름}_{year}.xml"

            if os.path.exists(filename) and not (year == progress['current_year'] and 이름 == progress['current_업무']):
                print(f"\n📅 {year}년 - ⏭️  이미 완료")
                continue

            print(f"\n📅 {year}년")

            year_data = []
            if os.path.exists(filename):
                print(f"   📂 기존 파일 발견 - 이어서 진행")
                with open(filename, 'r', encoding='utf-8') as f:
                    content = f.read()
                    items = content.split('<item>')[1:]
                    year_data = [
                        f'<item>{item}' for item in items if item.strip()]

            start_month = progress['current_month'] if (
                year == progress['current_year'] and 이름 == progress['current_업무']) else 1

            for month in range(start_month, 13):
                if year == datetime.now(KST).year and month > datetime.now(KST).month:
                    break

                print(f"   {month:02d}월 수집 중...", end=' ')

                month_data = get_month_data(코드, year, month, progress)

                if month_data is None:
                    if year_data:
                        save_year_file(filename, year_data, 이름)
                        print(f"\n   💾 {filename} 임시 저장 완료!")

                    progress['current_업무'] = 이름
                    progress['current_year'] = year
                    progress['current_month'] = month
                    save_progress(progress)

                    elapsed = (datetime.now(KST) - start_time).seconds

                    # Slack 중지 알림
                    send_slack_message(
                        f"*일일 API 제한 도달* ⏸️\n\n"
                        f"• 진행: `{이름} {year}년 {month}월`\n"
                        f"• 오늘 수집: `{today_collected:,}건`\n"
                        f"• API 호출: `{progress['daily_api_calls']}/{MAX_DAILY_CALLS}회`\n"
                        f"• 소요시간: `{elapsed//60}분`\n"
                        f"• 누적: `{progress.get('total_collected', 0):,}건`\n\n"
                        f"_내일 자동으로 이어서 수집합니다!_"
                    )

                    print(f"\n⏸️  일일 제한으로 일시 중지")
                    print(f"💾 진행 상황 저장: {이름} {year}년 {month}월")
                    print(f"✅ 내일 다시 실행하면 여기서부터 이어집니다!")
                    return

                if month_data:
                    year_data.extend(month_data)
                    count = sum(data.count('<item>') for data in month_data)
                    today_collected += count
                    progress['total_collected'] = progress.get(
                        'total_collected', 0) + count
                    print(f"✅ {count:,}건")
                else:
                    print(f"⚪ 데이터 없음")

                progress['current_month'] = month + 1
                save_progress(progress)
                time.sleep(1)

            if year_data:
                save_year_file(filename, year_data, 이름)
                print(f"   💾 {filename} 저장 완료!")

            progress['current_year'] = year + 1
            progress['current_month'] = 1
            save_progress(progress)

        progress['current_업무'] = 업무_리스트[업무_리스트.index(
            이름) + 1] if 업무_리스트.index(이름) < len(업무_리스트) - 1 else '완료'
        progress['current_year'] = 2005
        progress['current_month'] = 1
        save_progress(progress)

        time.sleep(10)

    # 완료 알림
    elapsed = datetime.now(KST) - start_time
    send_slack_message(
        f"*전체 수집 완료!* 🎉\n\n"
        f"• 오늘 수집: `{today_collected:,}건`\n"
        f"• 총 누적: `{progress.get('total_collected', 0):,}건`\n"
        f"• 소요시간: `{int(elapsed.total_seconds()//3600)}시간 {int((elapsed.total_seconds()%3600)//60)}분`\n\n"
        f"_모든 데이터 수집이 완료되었습니다!_"
    )

    print("\n" + "="*70)
    print("🎉 전체 수집 완료!")
    print("="*70)


def save_year_file(filename, year_data, 업무명):
    """연도별 XML 파일 저장"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<contracts>\n')
        for data in year_data:
            if '<item>' in data:
                items = data.split('<item>')[1:]
                for item in items:
                    f.write('<item>' + item)
        f.write('</contracts>\n')


if __name__ == "__main__":
    try:
        collect_with_resume()
    except KeyboardInterrupt:
        print("\n\n⚠️  사용자가 중단했습니다!")
        send_slack_message("*사용자가 수집을 중단했습니다* ⚠️", is_error=True)
    except Exception as e:
        print(f"\n💥 오류 발생: {e}")
        send_slack_message(f"*오류 발생* 💥\n\n```{str(e)[:300]}```", is_error=True)
