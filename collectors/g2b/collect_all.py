import json
import os
import xml.etree.ElementTree as ET
from utils.logger import log
from utils.slack import send_slack_message
from utils.g2b_client import fetch_raw_data, append_to_year_file
from utils.drive import (
    download_progress_json, 
    upload_progress_json, 
    upload_file,
    test_drive_connection
)

# 🔧 환경변수에서 가져오기
GDRIVE_PROGRESS_FILE_ID = os.getenv("GDRIVE_PROGRESS_FILE_ID")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")


def count_items_in_xml(xml_text):
    """XML에서 실제 아이템 개수 세기"""
    try:
        root = ET.fromstring(xml_text)
        items = root.findall('.//item')
        
        # 빈 아이템 필터링
        valid_items = []
        for item in items:
            if len(list(item)) > 0:  # 실제 데이터가 있는 아이템만
                valid_items.append(item)
                
        return len(valid_items)
        
    except ET.ParseError as e:
        log(f"⚠ XML 파싱 실패, 개수 확인 불가: {e}")
        return 0
    except Exception as e:
        log(f"⚠ 아이템 개수 확인 중 오류: {e}")
        return 0


def increment_month(y, m):
    return (y + 1, 1) if m == 12 else (y, m + 1)


def should_continue_collection(progress):
    """수집 계속 여부 판단"""
    daily_limit = 500
    
    if progress["daily_api_calls"] >= daily_limit:
        log(f"⚠ 일일 API 한도 도달: {progress['daily_api_calls']}/{daily_limit}")
        return False, f"일일 API 한도 도달 ({progress['daily_api_calls']}/{daily_limit})"
    
    # 2024년 12월까지만 수집한다고 가정 (필요에 따라 수정)
    current_year = progress["current_year"]
    current_month = progress["current_month"]
    
    if current_year > 2024:
        return False, f"수집 완료: {current_year}년은 목표 범위 초과"
        
    return True, ""


def upload_data_file_to_drive(local_filename):
    """
    🔧 새로 추가: 수집한 데이터 파일을 Google Drive에 업로드
    예: 물품_2014.xml 파일을 Drive 폴더에 업로드
    """
    if not os.path.exists(local_filename):
        log(f"⚠ 업로드할 파일이 존재하지 않음: {local_filename}")
        return False
        
    try:
        # 파일을 Google Drive 폴더에 업로드 (새 파일 생성)
        success = upload_file(
            local_path=local_filename, 
            file_id="new_file",  # 새 파일로 생성
            create_if_not_exists=True
        )
        
        if success:
            log(f"✅ 데이터 파일 Drive 업로드 완료: {local_filename}")
        else:
            log(f"❌ 데이터 파일 Drive 업로드 실패: {local_filename}")
            
        return success
        
    except Exception as e:
        log(f"❌ 데이터 파일 업로드 중 오류: {e}")
        return False


if __name__ == "__main__":
    log("🚀 G2B 자동 수집 시작")
    
    # 🔧 1. Drive 연결 테스트 (선택사항, 하지만 권장)
    if not test_drive_connection():
        log("❌ Google Drive 연결 실패, 수집 중단")
        send_slack_message(
            "```\n"
            "❌ G2B 수집 실패\n"
            "• 사유: Google Drive 연결 실패\n"
            "• 조치: 서비스 계정 키 및 권한 확인 필요\n"
            "```"
        )
        exit(1)

    # 🔧 2. Progress 다운로드 (개선된 함수 사용)
    if not GDRIVE_PROGRESS_FILE_ID:
        log("❌ GDRIVE_PROGRESS_FILE_ID 환경변수가 설정되지 않음")
        exit(1)
        
    progress = download_progress_json(GDRIVE_PROGRESS_FILE_ID)
    if progress is None:
        log("❌ Progress 데이터를 불러올 수 없음")
        exit(1)
    
    job = progress["current_job"]
    year = progress["current_year"]
    month = progress["current_month"]
    initial_total = progress["total_collected"]
    
    # 수집 계속 여부 확인
    can_continue, stop_reason = should_continue_collection(progress)
    if not can_continue:
        log(f"🛑 수집 중단: {stop_reason}")
        send_slack_message(
            f"```\n"
            f"🛑 G2B 수집 중단\n"
            f"• 사유: {stop_reason}\n"
            f"• 현재 위치: {job} {year}년 {month}월\n"
            f"• 누적: {progress['total_collected']:,}건\n"
            f"```"
        )
        exit(0)

    # Slack 시작 메시지
    send_slack_message(
        f"```\n"
        f"🚀 G2B 수집 시작\n"
        f"• 진행: {job} {year}년 {month}월\n"
        f"• API 사용: {progress['daily_api_calls']}/500\n"
        f"• 누적: {progress['total_collected']:,}건\n"
        f"```"
    )

    # 🔧 3. API 호출 및 결과 검증 (개선된 로직)
    collection_success = False
    collected_count = 0
    error_message = ""
    
    try:
        xml_text, item_count = fetch_raw_data(job, year, month)  # 개선된 함수에서 튜플 반환
        
        # XML 데이터 검증
        if xml_text and item_count >= 0:  # item_count가 0이어도 정상 (해당 월에 데이터 없음)
            # 연단위 파일에 저장
            filename = append_to_year_file(job, year, xml_text)
            
            # 실제 수집된 건수 계산
            collected_count = count_items_in_xml(xml_text)
            collection_success = True
            
            log(f"✅ 수집 및 저장 완료: {collected_count:,}건")
            
            # 🔧 4. 데이터 파일을 Google Drive에도 백업 (선택사항)
            # upload_data_file_to_drive(filename)
            
        else:
            error_message = "API 응답은 받았지만 유효한 데이터가 없음"
            log(f"⚠ {error_message}")
            
    except Exception as e:
        error_message = str(e)
        log(f"❌ 수집 오류: {e}")

    # 🔧 5. 성공한 경우에만 progress 업데이트
    if collection_success:
        # Progress 데이터 업데이트
        progress["total_collected"] += collected_count
        progress["daily_api_calls"] += 1
        
        # 다음 월로 이동 (성공한 경우에만!)
        next_year, next_month = increment_month(year, month)
        progress["current_year"] = next_year
        progress["current_month"] = next_month
        
        # 🔧 6. Progress를 Google Drive에 업로드 (개선된 함수 사용)
        upload_success = upload_progress_json(progress, GDRIVE_PROGRESS_FILE_ID)
        
        if not upload_success:
            log("⚠ Progress 업로드 실패, 하지만 수집은 완료됨")
            # Slack에 경고 메시지 추가
            upload_warning = "\n⚠ Progress 업로드 실패 - 수동 확인 필요"
        else:
            upload_warning = ""
        
        # 성공 Slack 메시지
        send_slack_message(
            f"```\n"
            f"✅ G2B 수집 완료\n"
            f"• 진행: {job} {year}년 {month}월\n"
            f"• 오늘 수집: {collected_count:,}건\n"
            f"• API 호출: {progress['daily_api_calls']}/500\n"
            f"• 누적: {progress['total_collected']:,}건\n"
            f"• 다음: {job} {next_year}년 {next_month}월\n"
            f"```{upload_warning}"
        )
        
        log("✅ 전체 프로세스 완료 - Progress 업데이트됨")
        
    else:
        # 🔧 7. 실패한 경우 Progress 유지, API 호출 수만 증가
        progress["daily_api_calls"] += 1  # API 호출은 했으니 카운트 증가
        
        # Progress 업로드 (API 호출 카운트만 업데이트)
        upload_success = upload_progress_json(progress, GDRIVE_PROGRESS_FILE_ID)
        
        if not upload_success:
            log("⚠ Progress 업로드도 실패")
            upload_warning = "\n⚠ Progress 업로드도 실패 - 수동 확인 필요"
        else:
            upload_warning = ""
        
        # 실패 Slack 메시지
        send_slack_message(
            f"```\n"
            f"❌ G2B 수집 실패\n"
            f"• 진행: {job} {year}년 {month}월\n"
            f"• 오류: {error_message}\n"
            f"• API 호출: {progress['daily_api_calls']}/500\n"
            f"• 누적: {progress['total_collected']:,}건\n"
            f"⚠ Progress 유지됨 - 다음 실행에서 재시도\n"
            f"```{upload_warning}"
        )
        
        log("⚠ 프로세스 완료 - Progress 유지됨 (재시도 준비)")
        
        # GitHub Actions에서 실패로 인식하게 하려면 exit(1)
        # 하지만 일시적 API 오류는 정상적인 상황이므로 exit(0) 사용
        exit(0)