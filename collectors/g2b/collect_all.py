#!/usr/bin/env python3
import os
import sys
import json
import traceback
from datetime import datetime

# 경로 추가
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# ✅ 올바른 import (함수 기반)
from utils.drive import (
    download_progress_json, 
    upload_progress_json,
    upload_file,
    test_drive_connection
)
from utils.g2b_client import G2BClient
from utils.logger import log
from utils.slack import send_slack_message

# 설정값
PROGRESS_FILE_ID = "1_AKg04eOjQy3KBcjhp2xkkm1jzBcAjn-"
API_KEY = os.getenv("API_KEY")
MAX_API_CALLS = 500

def append_to_year_file(job, year, xml_content):
    """XML 내용을 연도별 파일에 추가"""
    filename = f"{job}_{year}.xml"
    local_path = f"data/{filename}"
    
    # 디렉토리 생성
    os.makedirs("data", exist_ok=True)
    
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
        
        log(f"📋 현재 진행상황: {progress['current_job']} {progress['current_year']}년 {progress['current_month']}월")
        log(f"📊 API 사용량: {progress['daily_api_calls']}/{MAX_API_CALLS}")
        
        # API 클라이언트 초기화
        # API 클라이언트 초기화 (디버깅 추가)
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
                    
                    # Google Drive에 업로드
                    upload_success = upload_file(local_path, filename)
                    if upload_success:
                        uploaded_files.append(filename)
                        log(f"☁️ Google Drive 업로드 완료: {filename}")
                    
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
        
        # 결과 슬랙 전송
        message = f"""🎯 **G2B 수집 완료**
```
• 진행: {progress['current_job']} {progress['current_year']}년 {progress['current_month']}월
• 오늘 수집: {total_new_items:,}건
• API 호출: {progress['daily_api_calls']}/{MAX_API_CALLS}
• 누적: {progress['total_collected']:,}건
• 업로드 파일: {len(uploaded_files)}개
```"""
        
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