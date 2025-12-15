import io
import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# 공통 인증 생성


def _get_drive_service():
    if not os.path.exists("service_account.json"):
        raise FileNotFoundError("❌ service_account.json not found")

    creds = Credentials.from_service_account_file(
        "service_account.json",
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)


# 파일 다운로드
def download_file(file_id, local_path):
    try:
        service = _get_drive_service()
        request = service.files().get_media(fileId=file_id)

        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        with io.FileIO(local_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        print(f"✅ Downloaded: {local_path}")
        return True

    except Exception as e:
        print(f"❌ Download error: {e}")
        return False


# 파일 업로드 (기존 파일 덮어쓰기)
def upload_file(local_path, file_id):
    try:
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"{local_path} does not exist")

        service = _get_drive_service()

        media = MediaFileUpload(local_path, resumable=True)

        service.files().update(
            fileId=file_id,
            media_body=media
        ).execute()

        print(f"⬆️ Uploaded: {local_path}")
        return True

    except Exception as e:
        print(f"❌ Upload error: {e}")
        return False
