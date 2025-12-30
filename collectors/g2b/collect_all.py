#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime
import pytz
import os
import sys

# utils 모듈들 import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.g2b_client import G2BClient
from utils.drive import GoogleDriveClient
from utils.slack import SlackClient
from utils.logger import log

def get_korea_date():
    """한국 시간 기준 현재 날짜 반환"""
    korea_tz = pytz.timezone('Asia/Seoul')
    korea_now = datetime.now(korea_tz)
    return korea_now.strftime('%Y-%m-%d')

def check_and_reset_daily_api_count(progress):
    """날짜 체크 및 API 카운트 자동 리셋"""
    today_korea = get_korea_date()
    last_date = progress.get('last_run_date', '')
    
    if last_date != today_korea:
        old_count = progress.get('daily_api_calls', 0)
        progress['daily_api_calls'] = 0
        progress['last_run_date'] = today_korea
        progress['last_api_reset_date'] = today_korea
        log(f"📅 새로운 날 시작 - API 카운트 리셋: {today_korea}")
        return True
    return False

def load_progress():
    """Google Drive에서 progress.json 다운로드"""
    drive_client = GoogleDriveClient()
    
    try:
        log("📥 progress.json 다운로드 시작")
        file_content = drive_client.download_file("progress.json")
        
        if file_content:
            progress = json.loads(file_content.decode('utf-8'))
            log("✅ progress.json 로드 완료")
            return progress
        else:
            log("📝 progress.json 없음 - 새로 시작")
            return {
                "current_job": "물품",
                "current_year": 2005,
                "current_month": 1,
                "total_collected": 0,
                "daily_api_calls": 0,
                "last_run_date": "",
                "last_api_reset_date": ""
            }
    except Exception as e:
        log(f"❌ progress.json 로드 실패: {e}")
        return {
            "current_job": "물품",
            "current_year": 2005,
            "current_month": 1,
            "total_collected": 0,
            "daily_api_calls": 0,
            "last_run_date": "",
            "last_api_reset_date": ""
        }

def save_progress(progress):
    """progress.json을 Google Drive에 업로드"""
    try:
        log("📤 progress.json 업로드 시작")
        progress_json = json.dumps(progress, indent=2, ensure_ascii=False)
        
        # 로컬에 임시 저장
        with open("progress.json", "w", encoding="utf-8") as f:
            f.write(progress_json)
        
        # Google Drive에 업로드
        drive_client = GoogleDriveClient()
        drive_client.upload_file("progress.json", "progress.json")
        log("✅ progress.json 업로드 완료")
        
    except Exception as e:
        log(f"❌ progress.json 업로드 실패: {e}")
        raise

def collect_and_save_data(client, job, year, month, progress, drive_client):
    """데이터 수집하고 저장 (API 카운트 정확히 추적)"""
    try:
        log(f"📞 API 호출 중: {job} {year}-{month:02d}")
        
        # API 호출 전 카운트 저장
        api_calls_before = progress.get('daily_api_calls', 0)
        
        # 데이터 수집 (페이지네이션 포함)
        all_data, total_items, api_calls_used = client.fetch_paginated_data(job, year, month)
        
        # ✅ API 카운트 정확히 업데이트
        progress['daily_api_calls'] = api_calls_before + api_calls_used
        log(f"📊 API 사용량: +{api_calls_used}회 (총: {progress['daily_api_calls']}/500)")
        
        if all_data and total_items > 0:
            # 파일 저장
            filename = f"{job}_{year}_{month:02d}.xml"
            
            # 로컬 저장
            os.makedirs("data", exist_ok=True)
            local_path = f"data/{filename}"
            
            with open(local_path, 'w', encoding='utf-8') as f:
                f.write(all_data)
            
            log(f"💾 로컬 저장: {local_path} ({total_items:,}건)")
            
            # ✅ Google Drive에도 XML 파일 업로드
            try:
                drive_client.upload_file(filename, local_path)
                log(f"☁️ Google Drive 업로드 완료: {filename}")
            except Exception as e:
                log(f"⚠ Google Drive 업로드 실패: {e} (로컬 저장은 성공)")
            
            # Progress 업데이트
            progress['total_collected'] += total_items
            log(f"✅ {job} {year}-{month:02d}: {total_items:,}건 수집 완료")
            
            return total_items, api_calls_used
        else:
            log(f"📭 {job} {year}-{month:02d}: 데이터 없음")
            return 0, api_calls_used
            
    except Exception as e:
        log(f"❌ {job} {year}-{month:02d} 수집 실패: {e}")
        return 0, 0

def get_next_period(job, year, month):
    """다음 수집할 업무/년도/월 계산"""
    jobs = ["물품", "공사", "용역", "외자"]
    current_job_idx = jobs.index(job)
    
    # 월 증가
    month += 1
    if month > 12:
        month = 1
        year += 1
        
    # 2025년 넘으면 다음 업무로
    if year > 2025:
        current_job_idx += 1
        if current_job_idx >= len(jobs):
            return None, None, None  # 모든 수집 완료
        job = jobs[current_job_idx]
        year = 2005
        month = 1
        
    return job, year, month

def main():
    """메인 실행 함수"""
    try:
        log("🚀 G2B 기업 매출 데이터 수집 시작")
        
        # API 클라이언트 초기화
        api_key = os.getenv("API_KEY")
        if not api_key:
            raise Exception("API_KEY 환경변수가 설정되지 않음")
        
        client = G2BClient(api_key)
        drive_client = GoogleDriveClient()
        
        # Progress 로드
        progress = load_progress()
        
        # 날짜 체크 및 API 카운트 리셋
        check_and_reset_daily_api_count(progress)
        
        # Slack 시작 알림
        try:
            slack_client = SlackClient()
            start_message = f"""🚀 **G2B 데이터 수집 시작**
📅 날짜: {get_korea_date()}
📊 현재 진행: {progress['current_job']} {progress['current_year']}-{progress['current_month']:02d}
📈 누적 수집: {progress['total_collected']:,}건
🔢 오늘 API 사용: {progress['daily_api_calls']}/500"""
            slack_client.send_message(start_message)
        except Exception as e:
            log(f"⚠ Slack 시작 알림 실패: {e}")
        
        # ✅ API 한도 다 쓸 때까지 계속 수집
        max_api_calls = 500
        collected_today = []
        
        while progress['daily_api_calls'] < max_api_calls:
            current_job = progress['current_job']
            current_year = progress['current_year'] 
            current_month = progress['current_month']
            
            # 남은 API 호출 수 체크
            remaining_calls = max_api_calls - progress['daily_api_calls']
            log(f"🔄 수집 중: {current_job} {current_year}-{current_month:02d} (남은 API: {remaining_calls})")
            
            # 데이터 수집
            collected_items, api_used = collect_and_save_data(
                client, current_job, current_year, current_month, 
                progress, drive_client
            )
            
            if collected_items > 0:
                collected_today.append(f"{current_job} {current_year}-{current_month:02d}: {collected_items:,}건")
            
            # 다음 월로 이동
            next_job, next_year, next_month = get_next_period(
                current_job, current_year, current_month
            )
            
            if next_job is None:
                log("🎉 모든 데이터 수집 완료!")
                break
                
            progress['current_job'] = next_job
            progress['current_year'] = next_year
            progress['current_month'] = next_month
            
            # Progress 저장 (중간 저장)
            save_progress(progress)
            
            # API 한도 체크
            if progress['daily_api_calls'] >= max_api_calls:
                log(f"⏰ 일일 API 한도 도달: {progress['daily_api_calls']}/{max_api_calls}")
                break
                
            # 요청 간 대기 (Rate Limiting)
            time.sleep(1)
        
        # 최종 Progress 저장
        save_progress(progress)
        
        # 완료 알림
        try:
            completion_message = f"""✅ **G2B 수집 완료**
📊 API 사용량: {progress['daily_api_calls']}/500
📈 오늘 수집: {len(collected_today)}개 월
📋 수집 내역:
{chr(10).join(collected_today[:10])}
{'...' if len(collected_today) > 10 else ''}

🎯 다음 실행: {progress['current_job']} {progress['current_year']}-{progress['current_month']:02d}"""
            slack_client.send_message(completion_message)
        except Exception as e:
            log(f"⚠ Slack 완료 알림 실패: {e}")
        
        log("🏁 수집 스크립트 종료")
        
    except Exception as e:
        log(f"❌ 스크립트 실행 오류: {e}")
        
        # 에러 알림
        try:
            slack_client = SlackClient()
            error_message = f"""❌ **G2B 수집 오류**
🚫 오류: {str(e)}
📅 시간: {get_korea_date()}"""
            slack_client.send_message(error_message)
        except:
            pass
        
        raise

if __name__ == "__main__":
    main()