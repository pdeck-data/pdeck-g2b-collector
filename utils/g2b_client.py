import os
import time
import requests
import xml.etree.ElementTree as ET
from utils.logger import log

API_KEY = os.getenv("API_KEY")


def fetch_raw_data(job, year, month, retries=5):
    """
    나라장터 원본 XML 다운로드 (재시도 포함)
    
    개선사항:
    1. XML 응답 검증 추가
    2. 빈 응답 처리
    3. API 오류 코드 체크
    4. 더 명확한 에러 처리
    """
    
    # 🔧 수정 1: API 키 검증
    if not API_KEY:
        raise ValueError("API_KEY 환경변수가 설정되지 않았습니다")
    
    url = "https://apis.data.go.kr/1230000/ScsbidInfoService/getBidInfoList"

    params = {
        "serviceKey": API_KEY,
        "pageNo": 1,
        "numOfRows": 9999,
        "inqryDiv": 1,
        "inqryBgnDt": f"{year}{month:02d}01",  # 🔧 수정 2: 02 → 02d (더 명확한 포맷팅)
        "inqryEndDt": f"{year}{month:02d}28",  # 🚨 문제: 28일로 고정되어 있음!
        "type": "xml",
    }
    
    # 🔧 수정 3: 월말일 계산 (28일 고정 문제 해결)
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    params["inqryEndDt"] = f"{year}{month:02d}{last_day}"

    last_error = None
    
    for attempt in range(1, retries + 1):
        try:
            log(f"📡 API 호출 시도 {attempt}/{retries}: {job} {year}년 {month}월")
            
            # 🔧 수정 4: 타임아웃 추가
            resp = requests.get(url, params=params, timeout=30)

            if resp.status_code == 200:
                xml_text = resp.text.strip()
                
                # 🔧 수정 5: 빈 응답 체크
                if not xml_text:
                    log(f"⚠ 빈 XML 응답: {year}-{month}")
                    last_error = Exception(f"빈 응답 수신: {year}-{month}")
                    if attempt < retries:
                        time.sleep(2 + attempt)
                        continue
                    raise last_error
                
                # 🔧 수정 6: XML 검증 및 아이템 개수 확인
                validation_result = validate_xml_response(xml_text, job, year, month)
                
                if not validation_result["valid"]:
                    log(f"⚠ XML 검증 실패: {validation_result['error']}")
                    last_error = Exception(f"XML 검증 실패: {validation_result['error']}")
                    if attempt < retries:
                        time.sleep(2 + attempt)
                        continue
                    raise last_error
                
                # ✅ 성공
                item_count = validation_result["item_count"]
                if item_count > 0:
                    log(f"✅ XML 다운로드 성공: {job} {year}-{month} ({item_count:,}건)")
                else:
                    log(f"ℹ️ 데이터 없음: {job} {year}-{month} (정상 응답)")
                    
                return xml_text, item_count  # 🔧 수정 7: 아이템 개수도 반환

            else:
                last_error = Exception(f"HTTP {resp.status_code} 오류")
                log(f"⚠ API 오류 {resp.status_code} → 재시도 {attempt}/{retries}")
                
        except requests.Timeout:
            last_error = Exception("API 요청 타임아웃 (30초)")
            log(f"⚠ API 타임아웃 → 재시도 {attempt}/{retries}")
            
        except requests.RequestException as e:
            last_error = Exception(f"네트워크 오류: {str(e)}")
            log(f"⚠ 네트워크 오류 {e} → 재시도 {attempt}/{retries}")
            
        except Exception as e:
            last_error = e
            log(f"⚠ 예상치 못한 오류 {e} → 재시도 {attempt}/{retries}")

        # 재시도 대기 (마지막 시도가 아닌 경우)
        if attempt < retries:
            wait_time = 2 + attempt  # 점진적 대기
            log(f"⏳ {wait_time}초 대기 후 재시도...")
            time.sleep(wait_time)

    # 모든 재시도 실패
    error_msg = f"API 반복 오류 발생: {job} {year}-{month} (최종 오류: {last_error})"
    log(f"❌ {error_msg}")
    raise last_error or Exception(error_msg)


def validate_xml_response(xml_text, job, year, month):
    """
    🔧 새로 추가: XML 응답 검증
    
    Returns:
        dict: {
            "valid": bool,
            "item_count": int, 
            "error": str
        }
    """
    try:
        root = ET.fromstring(xml_text)
        
        # API 오류 코드 체크
        result_code = root.find('.//resultCode')
        if result_code is not None and result_code.text != "00":
            result_msg = root.find('.//resultMsg')
            error_msg = result_msg.text if result_msg is not None else "알 수 없는 오류"
            return {
                "valid": False,
                "item_count": 0,
                "error": f"API 오류 {result_code.text}: {error_msg}"
            }
        
        # 아이템 개수 확인
        items = root.findall('.//item')
        item_count = len(items)
        
        # 아이템이 있다면 실제 데이터가 있는지 확인
        if item_count > 0:
            first_item = items[0]
            if len(list(first_item)) == 0:  # 빈 아이템
                return {"valid": True, "item_count": 0, "error": ""}
                
        return {"valid": True, "item_count": item_count, "error": ""}
        
    except ET.ParseError as e:
        return {
            "valid": False, 
            "item_count": 0,
            "error": f"XML 파싱 오류: {str(e)}"
        }
    except Exception as e:
        return {
            "valid": False,
            "item_count": 0, 
            "error": f"XML 검증 오류: {str(e)}"
        }


def append_to_year_file(job, year, xml_text):
    """
    연단위 파일에 월 데이터를 계속 Append
    
    개선사항:
    1. 파일 존재 여부 확인
    2. 디렉토리 생성
    3. 파일 크기 체크
    4. 더 안전한 파일 작업
    """
    
    # 🔧 수정 1: 데이터 디렉토리 생성
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)
    
    filename = os.path.join(data_dir, f"{job}_{year}.xml")  # 🔧 수정 2: data/ 폴더에 저장
    
    # 🔧 수정 3: 파일 존재 여부 및 크기 확인
    file_exists = os.path.exists(filename)
    file_size_before = os.path.getsize(filename) if file_exists else 0
    
    try:
        # 🔧 수정 4: 더 안전한 파일 작업
        with open(filename, "a", encoding="utf-8") as f:
            if not file_exists:
                # 새 파일이면 XML 헤더 추가
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write(f'<root year="{year}" category="{job}">\n')
                log(f"📄 새 연단위 파일 생성: {filename}")
            
            f.write(f"\n<!-- {year}년 {job} 데이터 추가 - {time.strftime('%Y-%m-%d %H:%M:%S')} -->\n")
            f.write(xml_text)
            f.write("\n")
            
        # 🔧 수정 5: 파일 크기 확인 및 로그
        file_size_after = os.path.getsize(filename)
        size_added_mb = (file_size_after - file_size_before) / (1024 * 1024)
        
        log(f"💾 연단위 파일 저장 완료 → {filename} (+{size_added_mb:.1f}MB)")
        
        return filename
        
    except IOError as e:
        error_msg = f"파일 저장 실패: {filename} - {str(e)}"
        log(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"예상치 못한 파일 오류: {filename} - {str(e)}"
        log(f"❌ {error_msg}")
        raise Exception(error_msg)


def finalize_year_file(job, year):
    """
    🔧 새로 추가: 연단위 파일 완료 처리 (XML 태그 닫기)
    """
    data_dir = "data"
    filename = os.path.join(data_dir, f"{job}_{year}.xml")
    
    if os.path.exists(filename):
        try:
            with open(filename, "a", encoding="utf-8") as f:
                f.write("</root>\n")
            log(f"🔒 연단위 파일 완료: {filename}")
        except Exception as e:
            log(f"⚠ 파일 완료 처리 실패: {e}")


# 🔧 추가: API 호출 횟수 추적을 위한 전역 변수
_api_call_count = 0
_daily_limit = 500

def get_api_call_count():
    """현재 API 호출 횟수 반환"""
    return _api_call_count

def increment_api_call_count():
    """API 호출 횟수 증가"""
    global _api_call_count
    _api_call_count += 1
    return _api_call_count

def reset_api_call_count():
    """API 호출 횟수 리셋 (일일 초기화용)"""
    global _api_call_count
    _api_call_count = 0
    log("🔄 API 호출 횟수 리셋")

def is_api_limit_reached():
    """API 호출 한도 도달 여부"""
    return _api_call_count >= _daily_limit