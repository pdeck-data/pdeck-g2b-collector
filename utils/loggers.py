import os
from datetime import datetime

LOG_DIR = "data/logs"


def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"

    print(line)

    # 로그 파일로 저장
    os.makedirs(LOG_DIR, exist_ok=True)
    logfile = os.path.join(
        LOG_DIR, f"g2b_run_{datetime.now().strftime('%Y-%m-%d')}.log")

    with open(logfile, "a", encoding="utf-8") as f:
        f.write(line + "\n")
