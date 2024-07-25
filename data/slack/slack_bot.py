import os
import sys
from loguru import logger
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
logger.info(f"SLACK_BOT_TOKEN: {BOT_TOKEN}")
if not BOT_TOKEN:
    raise ValueError("SLACK_BOT_TOKEN is not set in the environment variables")
client = WebClient(token=BOT_TOKEN)

try:
    response = client.chat_postMessage(channel="C07DRAU9S3W", text="Hello, World!")
    assert response["message"]["text"] == "Hello, World!"
except SlackApiError as e:
    print(f"Error sending message: {e.response['error']}")

# If you want to print the response
print(response)
