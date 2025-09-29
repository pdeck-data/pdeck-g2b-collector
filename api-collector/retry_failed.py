import requests
import os
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import json

load_dotenv()

SERVICE_KEY = os.getenv('API_KEY')
BASE_URL = "http://apis.data.go.kr/1230000/ao/CntrctInfoService"

# 실패 로그 파일
FAILED_LOG = 'data/logs/failed_months.json'


def log_failed_month(업무, year, month, error):
    """실패한 월 기록"""
    os.makedirs('data/logs', exist_ok=True)

    if os.path.exists(FAILED_LOG):
        with open(FAILED_LOG, 'r', encoding='utf-8') as f:
            failed = json.load(f)
    else:
        failed = []

    failed.append({
        '업무': 업무,
        'year': year,
        'month': month,
        'error': str(error),
        'timestamp': datetime.now().isoformat()
    })

    with open(FAILED_LOG, 'w', encoding='utf-8') as f:
        json.dump(failed, f, ensure_ascii=False, indent=2)


def get_failed_months():
    """실패한 월 목록 가져오기"""
    if not os.path.exists(FAILED_LOG):
        return []

    with open(FAILED_LOG, 'r', encoding='utf-8') as f:
        failed = json.load(f)

    # 중복 제거
    unique = {}
    for item in failed:
        key = f"{item['업무']}_{item['year']}_{item['month']}"
        unique[key] = item

    return list(unique.values())


def retry_month(업무코드, 업무명, year, month, max_retries=5):
    """
    특정 월 재시도 (더 공격적인 재시도)
    """
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

    print(f"   🔄 재시도 중: {업무명} {year}년 {month:02d}월")

    while True:
        params = {
            'serviceKey': SERVICE_KEY,
            'numOfRows': 999,
            'pageNo': page,
            'inqryDiv': '1',
            'inqryBgnDt': month_start,
            'inqryEndDt': month_end
        }

        success = False

        for attempt in range(max_retries):
            try:
                print(
                    f"      페이지 {page}, 시도 {attempt + 1}/{max_retries}...", end=' ')

                response = requests.get(
                    url, params=params, timeout=60)  # 타임아웃 증가

                if '<resultCode>00</resultCode>' in response.text:
                    if '<item>' not in response.text:
                        print(f"✅ 완료 (총 {page-1} 페이지)")
                        return all_items

                    all_items.append(response.text)
                    page += 1
                    print("✅")
                    success = True
                    time.sleep(1)  # 안전한 간격
                    break

                else:
                    print(f"❌ API 에러")
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 10  # 점점 길게 대기
                        print(f"         {wait_time}초 대기...")
                        time.sleep(wait_time)

            except requests.exceptions.Timeout:
                print(f"⏱️ 타임아웃")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 15
                    print(f"         {wait_time}초 대기 후 재시도...")
                    time.sleep(wait_time)

            except Exception as e:
                print(f"💥 {e}")
                if attempt < max_retries - 1:
                    time.sleep(10)

        if not success:
            print(f"      ❌ 최대 재시도 초과")
            return all_items

    return all_items


def retry_all_failed():
    """모든 실패한 월 재시도"""
    failed_months = get_failed_months()

    if not failed_months:
        print("✅ 실패한 월이 없습니다!")
        return

    print("=" * 70)
    print(f"🔄 실패한 월 재수집 ({len(failed_months)}개)")
    print("=" * 70)

    # 업무별로 그룹화
    by_type = {}
    for item in failed_months:
        업무 = item['업무']
        if 업무 not in by_type:
            by_type[업무] = []
        by_type[업무].append(item)

    # 업무별 통계
    print("\n📊 실패 목록:")
    for 업무, items in by_type.items():
        print(f"   {업무}: {len(items)}개월")
        for item in items[:3]:  # 처음 3개만 표시
            print(f"      - {item['year']}년 {item['month']:02d}월")
        if len(items) > 3:
            print(f"      ... 외 {len(items)-3}개")

    print("\n" + "=" * 70)

    업무코드_맵 = {
        '물품': 'Thng',
        '용역': 'Servc',
        '공사': 'Cnstwk'
    }

    success_count = 0
    still_failed = []

    for item in failed_months:
        업무명 = item['업무']
        year = item['year']
        month = item['month']
        업무코드 = 업무코드_맵.get(업무명)

        if not 업무코드:
            print(f"⚠️ 알 수 없는 업무: {업무명}")
            continue

        try:
            month_data = retry_month(업무코드, 업무명, year, month)

            if month_data:
                # 파일에 추가
                filename = f"data/raw/{업무명}_{year}.xml"

                # 파일이 없으면 생성
                if not os.path.exists(filename):
                    with open(filename, 'w', encoding='utf-8') as f:
                        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                        f.write('<contracts>\n')
                        f.write('</contracts>\n')

                # 기존 파일에 추가
                with open(filename, 'r', encoding='utf-8') as f:
                    existing = f.read()

                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(existing.replace('</contracts>', ''))
                    for data in month_data:
                        items = data.split('<item>')[1:]
                        for item_data in items:
                            f.write('<item>' + item_data)
                    f.write('</contracts>\n')

                count = sum(data.count('<item>') for data in month_data)
                print(f"   ✅ 성공: {count:,}건 추가됨")
                success_count += 1
            else:
                print(f"   ⚪ 데이터 없음 (정상)")
                success_count += 1

        except Exception as e:
            print(f"   ❌ 실패: {e}")
            still_failed.append(item)

        print()
        time.sleep(5)  # 다음 월까지 대기

    # 결과 요약
    print("=" * 70)
    print("📊 재시도 결과")
    print("=" * 70)
    print(f"성공: {success_count}/{len(failed_months)}")

    if still_failed:
        print(f"\n⚠️ 여전히 실패: {len(still_failed)}개")
        for item in still_failed:
            print(f"   - {item['업무']} {item['year']}년 {item['month']:02d}월")

        # 실패 목록 업데이트
        with open(FAILED_LOG, 'w', encoding='utf-8') as f:
            json.dump(still_failed, f, ensure_ascii=False, indent=2)
    else:
        print("\n🎉 모두 성공!")
        # 실패 로그 삭제
        if os.path.exists(FAILED_LOG):
            os.remove(FAILED_LOG)


def check_failed():
    """실패한 월 확인만"""
    failed_months = get_failed_months()

    if not failed_months:
        print("✅ 실패한 월이 없습니다!")
        return

    print("📊 실패한 월 목록")
    print("=" * 70)

    by_type = {}
    for item in failed_months:
        업무 = item['업무']
        if 업무 not in by_type:
            by_type[업무] = []
        by_type[업무].append(item)

    for 업무, items in sorted(by_type.items()):
        print(f"\n{업무} ({len(items)}개):")
        for item in sorted(items, key=lambda x: (x['year'], x['month'])):
            print(f"  - {item['year']}년 {item['month']:02d}월")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == 'check':
            check_failed()
        elif sys.argv[1] == 'retry':
            retry_all_failed()
        else:
            print("사용법:")
            print("  python retry_failed.py check   # 실패 목록 확인")
            print("  python retry_failed.py retry   # 재시도")
    else:
        print("💡 실패한 월 관리 도구")
        print()
        print("명령어:")
        print("  check  - 실패한 월 목록 확인")
        print("  retry  - 실패한 월 재수집")
        print()
        print("예시:")
        print("  python retry_failed.py check")
        print("  python retry_failed.py retry")
