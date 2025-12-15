import requests
from utils.logger import log

BASE_URL = "https://www.g2b.go.kr/api/... (실제 endpoint로 교체)"


def get_monthly_data(year, month):
    """
    특정 연/월 데이터를 수집하고 item 리스트를 반환하는 템플릿
    """
    url = f"{BASE_URL}?year={year}&month={month}"

    log(f"🌐 Request: {url}")

    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
    except Exception as e:
        log(f"❌ API Error: {e}")
        return []

    # ↓↓ 실제 XML 파싱이 들어갈 부분 ↓↓
    try:
        # xml → item 리스트로 변환
        items = parse_xml_to_items(response.text)
    except Exception as e:
        log(f"❌ XML Parse Error: {e}")
        return []

    log(f"📦 {len(items)} items collected")
    return items


# 파서 템플릿
def parse_xml_to_items(xml_text):
    """
    XML을 파싱해서 item 리스트로 만드는 템플릿 메서드.
    실제 구조에 맞게 커스터마이즈 필요.
    """
    # 예시 반환
    return []
