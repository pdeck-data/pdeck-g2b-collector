import os
import time
import requests
import calendar
import xml.etree.ElementTree as ET
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import random

# logger 임포트 (같은 utils 폴더 내)
try:
    from .logger import log
except ImportError:
    try:
        from utils.logger import log
    except ImportError:
        # 로거가 없으면 print로 대체
        def log(msg):
            print(f"[LOG] {msg}")


class G2BClient:
    # ✅ 1. 핵심: 계약정보 서비스 URL로 변경 (매출 데이터용)
    BASE_URL = "http://apis.data.go.kr/1230000/ao/CntrctInfoService"

    # 작업별 오퍼레이션 매핑
    OPERATION_MAP = {
        "물품": "getCntrctInfoListThng",
        "공사": "getCntrctInfoListCnstwk",
        "용역": "getCntrctInfoListServc",
        "외자": "getCntrctInfoListFrgcpt"
    }

    def __init__(self, api_key):
        self.api_key = api_key
        self.session = self._create_session()

    def _create_session(self):
        """강화된 세션 설정 - 재시도 및 타임아웃 최적화"""
        session = requests.Session()

        # 간단한 재시도 설정 (호환성 문제 해결)
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504, 408],
            backoff_factor=2
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def fetch_data(self, job_type, year, month, retries=5):
        """
        API 호출 및 정밀한 에러 핸들링 - 페이징 처리 포함
        """
        if not self.api_key:
            raise ValueError("API_KEY가 설정되지 않았습니다.")

        if job_type not in self.OPERATION_MAP:
            return {'success': False, 'code': 'ERR', 'msg': f"잘못된 업무 구분: {job_type}"}

        operation = self.OPERATION_MAP[job_type]

        # ✅ 수정된 날짜 계산 로직 - 시간 포함
        last_day = calendar.monthrange(year, month)[1]
        start_dt = f"{year}{month:02d}010000"        # YYYYMMDDHHMM 형식
        end_dt = f"{year}{month:02d}{last_day}2359"   # YYYYMMDDHHMM 형식

        url = f"{self.BASE_URL}/{operation}"

        # 🔄 페이징을 통한 전체 데이터 수집
        all_items = []
        page_no = 1
        max_pages = 450  # 안전장치: 최대 50페이지까지만

        log(f"📞 API 호출 시작: {job_type} {year}-{month:02d}")

        while page_no <= max_pages:
            params = {
                "ServiceKey": self.api_key,  # 대문자 S
                "numOfRows": 100,           # 페이지당 100건씩
                "pageNo": page_no,
                "inqryDiv": 1,      # 1: 계약체결일 기준
                "inqryBgnDt": start_dt,      # 수정된 파라미터명
                "inqryEndDt": end_dt,        # 수정된 파라미터명
                "type": "xml"
            }

            success = False

            for attempt in range(1, retries + 1):
                try:
                    log(f"🔄 페이지 {page_no} 호출 시도 {attempt}/{retries}")

                    # 📈 점진적 타임아웃 증가 전략
                    timeout_seconds = 60 + (attempt * 30)

                    # 랜덤 대기 (서버 부하 분산)
                    if attempt > 1:
                        wait_time = random.uniform(2, 5) + (attempt * 1)
                        log(f"⏳ {wait_time:.1f}초 대기 중...")
                        time.sleep(wait_time)

                    # HTTP 요청
                    response = self.session.get(
                        url,
                        params=params,
                        timeout=timeout_seconds,
                        headers={
                            'User-Agent': 'G2B-Collector/1.0',
                            'Accept': 'application/xml',
                            'Connection': 'keep-alive'
                        }
                    )
                    response.encoding = 'utf-8'

                    if response.status_code != 200:
                        log(f"⚠ HTTP 오류 {response.status_code} (시도 {attempt}/{retries})")
                        continue

                    # XML 파싱 및 결과 코드 분석
                    try:
                        root = ET.fromstring(response.text)
                        result_code = root.findtext('.//resultCode')
                        result_msg = root.findtext('.//resultMsg')

                        if not result_code:
                            log(f"⚠ XML 구조 이상 - resultCode 누락")
                            continue

                        # [Case 1] 정상 성공 (00)
                        if result_code == '00':
                            items = root.findall('.//item')
                            total_count = root.findtext('.//totalCount')
                            
                            log(f"✅ 페이지 {page_no}: {len(items)}건 수집 (전체: {total_count}건)")
                            
                            if items:
                                all_items.extend(items)
                                success = True
                                break
                            else:
                                # 더 이상 데이터 없음
                                log(f"ℹ️ 페이지 {page_no}: 데이터 없음 - 수집 완료")
                                success = True
                                page_no = max_pages + 1  # 루프 종료
                                break

                        # [Case 2] 데이터 없음 (03)
                        elif result_code == '03':
                            log(f"ℹ️ 페이지 {page_no}: 데이터 없음")
                            success = True
                            page_no = max_pages + 1  # 루프 종료
                            break

                        # [Case 3] 트래픽/인증 에러 (20, 22, 99)
                        elif result_code in ['20', '21', '22', '99']:
                            log(f"🚨 API 제한 오류: {result_msg}")
                            return {
                                'success': False,
                                'code': result_code,
                                'msg': f"API 호출 제한/인증 오류: {result_msg}"
                            }

                        # [Case 4] 서버 에러 (05, 08 등)
                        else:
                            log(f"⚠ API 서버 메시지: {result_msg} (코드: {result_code}) - 재시도")
                            continue

                    except ET.ParseError as e:
                        log(f"⚠ XML 파싱 실패: {str(e)[:100]} (시도 {attempt}/{retries})")
                        continue

                except requests.Timeout as e:
                    log(f"⏱️ 타임아웃 발생 ({timeout_seconds}초): {str(e)} (시도 {attempt}/{retries})")
                    continue

                except requests.ConnectionError as e:
                    log(f"🌐 연결 오류: {str(e)[:100]} (시도 {attempt}/{retries})")
                    continue

                except requests.RequestException as e:
                    log(f"⚠ 네트워크 오류: {str(e)[:100]} (시도 {attempt}/{retries})")
                    continue

            if not success:
                log(f"❌ 페이지 {page_no}: {retries}회 시도 후 실패")
                return {'success': False, 'code': 'TIMEOUT', 'msg': f'페이지 {page_no} 수집 실패'}

            page_no += 1

            # 페이지 간 대기 (서버 부하 방지)
            if page_no <= max_pages:
                time.sleep(random.uniform(1, 3))

        # 수집 완료 처리
        if all_items:
            log(f"🎉 전체 수집 완료: {len(all_items)}건")
            
            # 전체 XML 생성
            combined_xml = self._create_combined_xml(all_items, year, month, job_type)
            
            return {
                'success': True,
                'code': '00',
                'msg': '정상 수집',
                'data': combined_xml,
                'count': len(all_items)
            }
        else:
            log(f"ℹ️ 수집된 데이터 없음")
            return {
                'success': True,
                'code': '03',
                'msg': '데이터 없음 (정상)',
                'data': None,
                'count': 0
            }

    def _create_combined_xml(self, items, year, month, job_type):
        """여러 페이지의 아이템들을 하나의 XML로 결합"""
        header = '<?xml version="1.0" encoding="UTF-8"?>\n'
        header += '<response>\n'
        header += '  <header>\n'
        header += '    <resultCode>00</resultCode>\n'
        header += '    <resultMsg>정상</resultMsg>\n'
        header += '  </header>\n'
        header += '  <body>\n'
        header += '    <items>\n'
        
        footer = '    </items>\n'
        footer += f'    <totalCount>{len(items)}</totalCount>\n'
        footer += '  </body>\n'
        footer += '</response>\n'
        
        # 아이템들을 XML 문자열로 변환
        items_xml = ""
        for item in items:
            items_xml += ET.tostring(item, encoding='unicode') + '\n'
        
        return header + items_xml + footer


# 호환성 래퍼 함수
def fetch_raw_data(job_type, year, month):
    client = G2BClient(os.getenv("API_KEY"))
    return client.fetch_data(job_type, year, month)


# ✅ 4. 지수 님의 파일 저장 로직 유지 (데이터 폴더 생성, 헤더 처리 등)
def append_to_year_file(job, year, xml_text):
    if not xml_text:
        return None

    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)
    filename = os.path.join(data_dir, f"{job}_{year}.xml")

    file_exists = os.path.exists(filename)

    try:
        with open(filename, "a", encoding="utf-8") as f:
            # 새 파일이면 루트 태그 시작
            if not file_exists:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write(f'<root year="{year}" category="{job}">\n')

            # 주석 및 데이터 추가
            f.write(f"\n\n")
            f.write(xml_text)
            f.write("\n")

        log(f"💾 파일 저장: {filename}")
        return filename
    except Exception as e:
        log(f"❌ 파일 저장 실패: {e}")
        return None