import os
import sys
from loguru import logger
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
CHANNEL = os.environ.get("SLACK_CHANNEL")
logger.info(f"SLACK_BOT_TOKEN: {BOT_TOKEN}")
if not BOT_TOKEN:
    raise ValueError("SLACK_BOT_TOKEN is not set in the environment variables")
client = WebClient(token=BOT_TOKEN)


def send_message(message):
    logger.info(f"Sending message: {message}")
    if not CHANNEL:
        logger.error("SLACK_CHANNEL is not set in the environment variables")
        return
    try:
        response = client.chat_postMessage(channel=CHANNEL, text=message)
        logger.info(f"Message sent: {response}")
    except SlackApiError as e:
        logger.error(f"Error sending message: {e.response['error']}")
