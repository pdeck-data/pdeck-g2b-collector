import os
import requests
from utils.logger import log

API_KEY = os.getenv("API_KEY")


def get_monthly_data(year: int, month: int):
    """나라장터 API에서 해당 월 데이터 조회 — 템플릿"""

    if not API_KEY:
        raise ValueError("❌ API_KEY 환경변수가 설정되지 않았습니다.")

    # 월 01, 02 같은 형태로 맞춤
    month_str = f"{month:02d}"

    url = f"https://apis.data.go.kr/1230000/SomeEndpoint?" \
          f"serviceKey={API_KEY}&pblntfNo={year}{month_str}"

    log(f"🌐 API 요청: {url}")

    try:
        res = requests.get(url, timeout=20)
        res.raise_for_status()
    except Exception as e:
        log(f"❌ API 요청 실패: {e}")
        return []

    # TODO: 실제 XML → dict 파싱 넣기
    # items = parse_xml(res.text)

    # 지금은 예시로 빈 리스트 반환
    items = []

    log(f"📦 API 응답 처리 완료 ({len(items)}건)")
    return items
