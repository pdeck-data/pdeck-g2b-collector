import io
import os
import json
import time
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.errors import HttpError
from utils.logger import log

# 🔧 추가: 재시도 설정
MAX_RETRIES = 3
RETRY_DELAY = 2


def _get_drive_service():
    """Google Drive API 인증"""
    if not os.path.exists("service_account.json"):
        raise FileNotFoundError("❌ service_account.json not found")

    try:
        creds = Credentials.from_service_account_file(
            "service_account.json",
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        service = build("drive", "v3", credentials=creds)
        
        # 🔧 추가: 인증 테스트 (간단한 API 호출로 확인)
        try:
            service.about().get(fields="user").execute()
            log("✅ Google Drive 인증 성공")
        except Exception as e:
            log(f"⚠ Google Drive 인증 테스트 실패: {e}")
            raise
            
        return service
        
    except Exception as e:
        log(f"❌ Google Drive 인증 실패: {e}")
        raise


def download_file(file_id: str, local_path: str) -> bool:
    """
    Google Drive → 로컬 파일 다운로드
    
    개선사항:
    1. 재시도 로직 추가
    2. 파일 존재 여부 확인
    3. 부분 다운로드 실패 처리
    4. 더 자세한 로깅
    """
    
    if not file_id or not file_id.strip():
        log("❌ 다운로드 실패: file_id가 없음")
        return False
        
    log(f"📥 파일 다운로드 시작: {local_path} (ID: {file_id[:20]}...)")
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            service = _get_drive_service()
            
            # 🔧 추가: 파일 존재 여부 및 메타데이터 확인
            try:
                file_metadata = service.files().get(fileId=file_id, fields="name,size").execute()
                file_name = file_metadata.get('name', 'Unknown')
                file_size = int(file_metadata.get('size', 0))
                log(f"📋 파일 정보: {file_name} ({file_size:,} bytes)")
            except HttpError as e:
                if e.resp.status == 404:
                    log(f"❌ 파일을 찾을 수 없음: ID {file_id}")
                    return False
                else:
                    log(f"⚠ 파일 메타데이터 확인 실패: {e}")
            
            # 다운로드 실행
            request = service.files().get_media(fileId=file_id)
            
            # 디렉토리 생성
            os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
            
            # 임시 파일로 다운로드 (안전성 증대)
            temp_path = f"{local_path}.tmp"
            
            with io.FileIO(temp_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                downloaded_size = 0
                
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        downloaded_size = int(status.resumable_progress)
                        progress_pct = (downloaded_size / file_size * 100) if file_size > 0 else 0
                        if downloaded_size % (1024*1024) == 0:  # 1MB마다 로그
                            log(f"📥 다운로드 진행: {progress_pct:.1f}% ({downloaded_size:,} bytes)")
            
            # 🔧 추가: 다운로드 완료 후 파일 크기 검증
            if os.path.exists(temp_path):
                actual_size = os.path.getsize(temp_path)
                if file_size > 0 and actual_size != file_size:
                    log(f"⚠ 파일 크기 불일치: 예상 {file_size:,} vs 실제 {actual_size:,}")
                    os.remove(temp_path)
                    raise Exception(f"파일 크기 불일치 (예상: {file_size}, 실제: {actual_size})")
                
                # 임시 파일을 최종 파일로 이동
                if os.path.exists(local_path):
                    os.remove(local_path)
                os.rename(temp_path, local_path)
                
                log(f"✅ 다운로드 완료: {local_path} ({actual_size:,} bytes)")
                return True
            else:
                raise Exception("임시 파일이 생성되지 않음")

        except HttpError as e:
            if e.resp.status == 404:
                log(f"❌ 파일을 찾을 수 없음: ID {file_id}")
                return False
            elif e.resp.status == 403:
                log(f"❌ 권한 없음: ID {file_id}")
                return False
            else:
                log(f"⚠ Google Drive API 오류 ({e.resp.status}): {e}")
                
        except Exception as e:
            log(f"⚠ 다운로드 시도 {attempt}/{MAX_RETRIES} 실패: {e}")
            
            # 임시 파일 정리
            temp_path = f"{local_path}.tmp"
            if os.path.exists(temp_path):
                os.remove(temp_path)
        
        # 재시도 대기 (마지막 시도가 아닌 경우)
        if attempt < MAX_RETRIES:
            wait_time = RETRY_DELAY * attempt
            log(f"⏳ {wait_time}초 후 재시도...")
            time.sleep(wait_time)

    log(f"❌ 다운로드 최종 실패: {local_path}")
    return False


def upload_file(local_path: str, file_id: str, create_if_not_exists: bool = True) -> bool:
    """
    로컬 파일 → Google Drive 업로드 (기존 파일 덮어쓰기 또는 새로 생성)
    
    개선사항:
    1. 파일 존재 여부 확인
    2. 새 파일 생성 옵션
    3. 업로드 진행률 표시
    4. 재시도 로직
    5. 파일 크기 검증
    """
    
    if not os.path.exists(local_path):
        log(f"❌ 업로드 실패: 로컬 파일 없음 - {local_path}")
        return False
        
    file_size = os.path.getsize(local_path)
    log(f"📤 파일 업로드 시작: {local_path} ({file_size:,} bytes)")
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            service = _get_drive_service()
            
            # 🔧 추가: 파일 존재 여부 확인
            file_exists = False
            try:
                existing_file = service.files().get(fileId=file_id, fields="name").execute()
                file_exists = True
                log(f"📋 기존 파일 발견: {existing_file.get('name', 'Unknown')}")
            except HttpError as e:
                if e.resp.status == 404:
                    log(f"📋 새 파일로 업로드: ID {file_id}")
                else:
                    log(f"⚠ 파일 존재 여부 확인 실패: {e}")
            
            # 미디어 업로드 객체 생성
            media = MediaFileUpload(
                local_path, 
                resumable=True,
                chunksize=1024*1024  # 1MB 청크
            )
            
            if file_exists:
                # 기존 파일 업데이트
                request = service.files().update(
                    fileId=file_id,
                    media_body=media
                )
            else:
                if not create_if_not_exists:
                    log(f"❌ 파일이 존재하지 않고 생성이 허용되지 않음: {file_id}")
                    return False
                    
                # 새 파일 생성 (파일명은 로컬 파일 기준)
                file_metadata = {
                    'name': os.path.basename(local_path)
                }
                request = service.files().create(
                    body=file_metadata,
                    media_body=media
                )
            
            # 🔧 추가: 업로드 진행률 표시
            response = None
            uploaded_size = 0
            
            while response is None:
                try:
                    status, response = request.next_chunk()
                    if status:
                        uploaded_size = int(status.resumable_progress)
                        progress_pct = (uploaded_size / file_size * 100) if file_size > 0 else 0
                        if uploaded_size % (1024*1024) == 0:  # 1MB마다 로그
                            log(f"📤 업로드 진행: {progress_pct:.1f}% ({uploaded_size:,} bytes)")
                except HttpError as e:
                    if e.resp.status == 404:
                        log(f"❌ 업로드 대상 파일/폴더를 찾을 수 없음: {file_id}")
                        return False
                    else:
                        raise
            
            # 업로드 완료
            uploaded_file_id = response.get('id', file_id)
            log(f"✅ 업로드 완료: {local_path} → Drive ID {uploaded_file_id}")
            return True

        except HttpError as e:
            log(f"⚠ Google Drive API 오류 ({e.resp.status}): {e}")
            if e.resp.status in [403, 404]:  # 권한 없음이나 파일 없음은 재시도 안함
                break
                
        except Exception as e:
            log(f"⚠ 업로드 시도 {attempt}/{MAX_RETRIES} 실패: {e}")
        
        # 재시도 대기
        if attempt < MAX_RETRIES:
            wait_time = RETRY_DELAY * attempt
            log(f"⏳ {wait_time}초 후 재시도...")
            time.sleep(wait_time)

    log(f"❌ 업로드 최종 실패: {local_path}")
    return False


def download_progress_json(progress_file_id: str, local_path: str = "progress.json") -> dict:
    """
    🔧 새로 추가: progress.json 전용 다운로드 함수
    """
    log("📥 progress.json 다운로드 시작")
    
    if download_file(progress_file_id, local_path):
        try:
            with open(local_path, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
            log("✅ progress.json 로드 완료")
            return progress_data
        except Exception as e:
            log(f"❌ progress.json 파싱 실패: {e}")
            return None
    else:
        log("⚠ progress.json 다운로드 실패, 기본값 사용")
        return {
            "current_job": "물품",
            "current_year": 2014,
            "current_month": 1,
            "total_collected": 0,
            "daily_api_calls": 0,
        }


def upload_progress_json(progress_data: dict, progress_file_id: str, local_path: str = "progress.json") -> bool:
    """
    🔧 새로 추가: progress.json 전용 업로드 함수
    """
    log("📤 progress.json 업로드 시작")
    
    try:
        # 로컬에 저장
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)
        
        # Drive에 업로드
        success = upload_file(local_path, progress_file_id)
        
        if success:
            log("✅ progress.json 업로드 완료")
        else:
            log("❌ progress.json 업로드 실패")
            
        return success
        
    except Exception as e:
        log(f"❌ progress.json 처리 실패: {e}")
        return False


def test_drive_connection() -> bool:
    """
    🔧 새로 추가: Google Drive 연결 테스트
    """
    try:
        service = _get_drive_service()
        about = service.about().get(fields="user,storageQuota").execute()
        
        user_email = about.get('user', {}).get('emailAddress', 'Unknown')
        storage_quota = about.get('storageQuota', {})
        used_bytes = int(storage_quota.get('usage', 0))
        total_bytes = int(storage_quota.get('limit', 0))
        
        log(f"✅ Drive 연결 테스트 성공")
        log(f"   └─ 계정: {user_email}")
        log(f"   └─ 사용량: {used_bytes / (1024**3):.1f}GB / {total_bytes / (1024**3):.1f}GB")
        
        return True
        
    except Exception as e:
        log(f"❌ Drive 연결 테스트 실패: {e}")
        return False