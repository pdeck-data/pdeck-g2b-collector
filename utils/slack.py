import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")


def send_slack_message(text: str):
    """Slack 채널로 메시지 전송"""
    if not SLACK_TOKEN or not SLACK_CHANNEL_ID:
        print("⚠️ Slack 설정 없음 → 메시지 전송 생략")
        return

    client = WebClient(token=SLACK_TOKEN)

    try:
        client.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
            text=text
        )
        print("📨 Slack 메시지 전송 완료")

    except SlackApiError as e:
        print(f"❌ Slack API Error: {e.response['error']}")
