from datetime import datetime


def log(message: str):
    """통일된 로그 포맷"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
