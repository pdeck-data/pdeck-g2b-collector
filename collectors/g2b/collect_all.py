#!/usr/bin/env python3
import os
import sys
import time
import traceback
from datetime import datetime
import pytz

# -----------------------------------------------------------
# ⚡️ [최종 솔루션] 경로 강제 지정 (3단계 상위가 무조건 루트다)
# -----------------------------------------------------------
# 현재 파일: .../pdeck-g2b-collector/collectors/g2b/collect_all.py
# 목표 루트: .../pdeck-g2b-collector/ (여기에 utils가 있음)

current_file_path = os.path.abspath(__file__)             # 1. 현재 파일 경로
g2b_dir = os.path.dirname(current_file_path)              # 2. .../collectors/g2b
collectors_dir = os.path.dirname(g2b_dir)                 # 3. .../collectors
project_root = os.path.dirname(collectors_dir)            # 4. .../ (프로젝트 루트)

# 시스템 경로 맨 앞에 프로젝트 루트를 강제로 꽂아넣음
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 디버깅: 경로가 잘 잡혔는지 눈으로 확인 (로그에 찍힘)
print(f"✅ 프로젝트 루트 강제 지정: {project_root}")
print(f"📂 루트 폴더 내용물: {os.listdir(project_root)}")

# -----------------------------------------------------------
# ✅ 이제 Import는 실패할 수가 없음
# -----------------------------------------------------------
try:
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError
    
    # utils 모듈 로드
    from utils.drive import (
        download_progress_json, 
        upload_progress_json,
        test_drive_connection
    )
    from utils.g2b_client import G2BClient
    from utils.logger import log
    from utils.slack import send_slack_message
    from utils.auth import get_drive_service
    
except ImportError as e:
    # 만약 여기서도 에러나면 그건 파일이 없는 거임
    print(f"\n🚫 치명적 오류: Import 실패. {e}")
    print(f"현재 sys.path: {sys.path}")
    sys.exit(1)

# 설정값
PROGRESS_FILE_ID = "1_AKg04eOjQy3KBcjhp2xkkm1jzBcAjn-"
SHARED_DRIVE_ID = "0AOi7Y50vK8xiUk9PVA"
API_KEY = os.getenv("API_KEY")
MAX_API_CALLS = 500

def upload_file_to_shared_drive(local_path, filename):
    """Shared Drive에 파일 업로드"""
    try:
        log(f"📤 Shared Drive 업로드 시작: {filename} ({os.path.getsize(local_path):,} bytes)")
        
        # Drive 서비스 초기화
        service = get_drive_service()
        if not service:
            log("❌ Google Drive 서비스 초기화 실패")
            return False
        
        # 파일 메타데이터
        file_metadata = {
            'name': filename,
            'parents': [SHARED_DRIVE_ID]
        }
        
        # 파일 업로드
        media = MediaFileUpload(local_path, resumable=True, chunksize=1024*1024)
        
        # Shared Drive 지원
        request = service.files().create(
            body=file_metadata,
            media_body=media,
            supportsAllDrives=True,
            fields='id'
        )
        
        # 업로드 실행 (청크 단위)
        response = None
        while response is None:
            try:
                status, response = request.next_chunk()
                if status:
                    log(f"📊 업로드 진행률: {int(status.progress() * 100)}%")
            except HttpError as error:
                if error.resp.status in [500, 503]:
                    log(f"⚠️ 서버 오류, 재시도 중...")
                    time.sleep(5)
                    continue
                else:
                    log(f"❌ HTTP 오류: {error.resp.status} - {error}")
                    return False
        
        if response:
            file_id = response.get('id')
            log(f"✅ Shared Drive 업로드 완료: {filename} (ID: {file_id})")
            return True
        else:
            return False
            
    except Exception as e:
        log(f"❌ Shared Drive 업로드 실패: {filename} - {e}")
        return False

def append_to_year_file(job, year, xml_content):
    """XML 내용을 연도별 파일에 추가"""
    filename = f"{job}_{year}.xml"
    
    # 🔧 데이터 저장 경로도 프로젝트 루트 기준 data 폴더로 고정
    data_dir = os.path.join(project_root, "data")
    local_path = os.path.join(data_dir, filename)
    
    # 디렉토리 생성
    os.makedirs(data_dir, exist_ok=True)
    
    # XML 헤더 확인 및 추가
    if not os.path.exists(local_path):
        # 새 파일 생성
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<root>\n')
            f.write(xml_content)
            f.write('\n</root>')
        log(f"📝 새 파일 생성: {filename}")
    else:
        # 기존 파일에 추가 (</root> 태그 앞에 삽입)
        with open(local_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # </root> 제거하고 새 데이터 추가
        content = content.replace('</root>', '')
        content += xml_content + '\n</root>'
        
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write(content)
        log(f"📝 파일 업데이트: {filename}")
    
    return local_path, filename

def get_next_period(job, year, month):
    """다음 수집 기간 계산"""
    jobs = ["물품", "공사", "용역", "외자"]
    
    if month < 12:
        return job, year, month + 1
    else:
        # 12월이면 다음 업무로 이동
        try:
            current_idx = jobs.index(job)
            if current_idx < len(jobs) - 1:
                # 다음 업무
                return jobs[current_idx + 1], year, 1
            else:
                # 모든 업무 완료, 다음 연도
                return jobs[0], year + 1, 1
        except ValueError:
            # 알 수 없는 업무면 물품부터 시작
            return "물품", year, month + 1

def main():
    try:
        log("🚀 G2B 데이터 수집 시작")
        
        # Google Drive 연결 테스트
        if not test_drive_connection():
            raise Exception("Google Drive 연결 실패")
        
        # Progress 파일 다운로드
        progress = download_progress_json(PROGRESS_FILE_ID)
        if not progress:
            log("❌ progress.json 로드 실패")
            return False
        
        # ✅ 한국시간 기준 자동 API 리셋 로직
        korea_tz = pytz.timezone('Asia/Seoul')
        today_korea = datetime.now(korea_tz).strftime('%Y-%m-%d')
        
        if progress.get('last_api_reset_date') != today_korea:
            progress['daily_api_calls'] = 0
            progress['last_api_reset_date'] = today_korea
            log(f"🔄 일일 API 카운트 자동 리셋: {today_korea}")
        
        log(f"📋 현재 진행상황: {progress['current_job']} {progress['current_year']}년 {progress['current_month']}월")
        log(f"📊 API 사용량: {progress['daily_api_calls']}/{MAX_API_CALLS}")
        
        # API 클라이언트 초기화
        log(f"🔑 API_KEY 상태: {len(API_KEY) if API_KEY else 'None'}글자")
        log(f"🔑 API_KEY 앞자리: {API_KEY[:10] if API_KEY else 'None'}...")

        if not API_KEY:
            raise Exception("API_KEY 환경변수가 설정되지 않았습니다!")

        client = G2BClient(API_KEY)
        
        # 수집할 데이터 계산
        total_new_items = 0
        uploaded_files = []
        
        # API 한도까지 계속 수집
        while progress['daily_api_calls'] < MAX_API_CALLS:
            job = progress['current_job']
            year = progress['current_year']
            month = progress['current_month']
            
            log(f"📥 수집 시작: {job} {year}년 {month}월")
            
            try:
                # 데이터 수집
                xml_content, item_count, api_calls_used = client.fetch_data(job, year, month)
                
                # API 사용량 업데이트
                progress['daily_api_calls'] += api_calls_used
                log(f"📊 API 사용: +{api_calls_used} (총 {progress['daily_api_calls']}/{MAX_API_CALLS})")
                
                # 데이터가 있으면 저장
                if xml_content and item_count > 0:
                    # 연도별 파일에 저장
                    local_path, filename = append_to_year_file(job, year, xml_content)
                    
                    # ✅ Shared Drive에 업로드
                    upload_success = upload_file_to_shared_drive(local_path, filename)
                    if upload_success:
                        uploaded_files.append(filename)
                        log(f"☁️ Shared Drive 업로드 완료: {filename}")
                    
                    total_new_items += item_count
                    progress['total_collected'] += item_count
                    
                    log(f"✅ 수집 완료: {item_count:,}건")
                else:
                    log(f"ℹ️ 데이터 없음: {job} {year}년 {month}월")
                
                # 다음 기간으로 이동
                next_job, next_year, next_month = get_next_period(job, year, month)
                progress['current_job'] = next_job
                progress['current_year'] = next_year
                progress['current_month'] = next_month
                
                # 2025년을 넘어가면 중단
                if next_year > 2025:
                    log("🎉 모든 데이터 수집 완료! (2024-2025)")
                    break
                    
            except Exception as e:
                log(f"⚠️ 수집 실패: {job} {year}년 {month}월 - {e}")
                # 실패해도 다음으로 이동 (무한 루프 방지)
                next_job, next_year, next_month = get_next_period(job, year, month)
                progress['current_job'] = next_job
                progress['current_year'] = next_year
                progress['current_month'] = next_month
            
            # API 한도 도달 확인
            if progress['daily_api_calls'] >= MAX_API_CALLS:
                log(f"📊 일일 API 한도 도달: {progress['daily_api_calls']}/{MAX_API_CALLS}")
                break
        
        # Progress 업데이트
        progress['last_run_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Progress 파일 업로드
        upload_success = upload_progress_json(progress, PROGRESS_FILE_ID)
        
        # 결과 슬랙 전송 (안전한 포맷팅)
        message = (
            f"🎯 **G2B 수집 완료**\n"
            f"```\n"
            f"• 진행: {progress['current_job']} {progress['current_year']}년 {progress['current_month']}월\n"
            f"• 오늘 수집: {total_new_items:,}건\n"
            f"• API 호출: {progress['daily_api_calls']}/{MAX_API_CALLS}\n"
            f"• 누적: {progress['total_collected']:,}건\n"
            f"• 업로드 파일: {len(uploaded_files)}개\n"
            f"```"
        )
        
        send_slack_message(message)
        log("🎉 수집 작업 완료")
        
        return True
        
    except Exception as e:
        error_msg = f"❌ G2B 수집 실패: {str(e)}\n```{traceback.format_exc()}```"
        log(error_msg)
        send_slack_message(error_msg)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)