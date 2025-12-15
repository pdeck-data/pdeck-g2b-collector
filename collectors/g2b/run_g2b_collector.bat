@echo off
REM === G2B 자동 수집 실행 스크립트 ===
REM 날짜별 로그 파일 생성 (옵션)
set LOGDIR=C:\Users\ekapr\Documents\GitHub\api-collector\data\logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set LOGFILE=%LOGDIR%\g2b_run_%date:~0,10%.log

REM 수집 스크립트 폴더로 이동
cd C:\Users\ekapr\Documents\GitHub\api-collector\api-collector\collectors\g2b

REM Python으로 수집 실행 (표준출력 + 오류 모두 로그에 저장)
C:\Users\ekapr\AppData\Local\Microsoft\WindowsApps\python.exe collect_all.py >> "%LOGFILE%" 2>&1
