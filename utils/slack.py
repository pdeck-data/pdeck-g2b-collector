import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from utils.logger import log

SLACK_TOKEN = os.getenv("SLACK_TOKEN")
CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

client = WebClient(token=SLACK_TOKEN)


def send_slack_message(text: str):
    if not SLACK_TOKEN or not CHANNEL_ID:
        log("⚠️ Slack 토큰 또는 채널 ID가 없어 Slack으로 메시지를 보내지 않습니다.")
        return

    try:
        client.chat_postMessage(channel=CHANNEL_ID, text=text)
        log("📨 Slack 메시지 전송 성공")
    except SlackApiError as e:
        log(f"❌ Slack 메시지 전송 실패: {e.response['error']}")
