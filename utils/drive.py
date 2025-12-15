import os
import io
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from utils.logger import log


def get_drive_service():
    if not os.path.exists("service_account.json"):
        raise FileNotFoundError("❌ service_account.json not found")

    creds = Credentials.from_service_account_file(
        "service_account.json",
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)


def download_file(file_id: str, output_path: str):
    try:
        service = get_drive_service()
        request = service.files().get_media(fileId=file_id)

        fh = io.FileIO(output_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        log(f"📥 Google Drive 파일 다운로드 완료 → {output_path}")
        return True

    except Exception as e:
        log(f"❌ Download error: {e}")
        return False


def upload_file(local_path: str, file_id: str):
    try:
        service = get_drive_service()

        media = MediaIoBaseUpload(
            io.FileIO(local_path, "rb"),
            mimetype="application/json",
            resumable=True
        )

        service.files().update(
            fileId=file_id,
            media_body=media
        ).execute()

        log("📤 Google Drive 업로드 완료")
        return True

    except Exception as e:
        log(f"❌ Upload error: {e}")
        return False
