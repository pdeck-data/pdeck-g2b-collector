import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

SLACK_TOKEN = os.getenv("SLACK_TOKEN")  # SLACK_BOT_TOKEN → SLACK_TOKEN
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL_ID")

client = WebClient(token=SLACK_TOKEN)


def send_slack_message(text):
    try:
        client.chat_postMessage(channel=SLACK_CHANNEL, text=text)
        print("📨 Slack message sent")
    except SlackApiError as e:
        print(f"❌ Slack error: {e}")
