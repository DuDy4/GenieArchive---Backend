import os
import sys
import asyncio
from loguru import logger
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.rtm_v2 import RTMClient

from concurrent.futures import ThreadPoolExecutor

from common.utils import env_utils


BOT_TOKEN = env_utils.get("SLACK_BOT_TOKEN")
CHANNEL = env_utils.get("SLACK_CHANNEL")
if not BOT_TOKEN:
    raise ValueError("SLACK_BOT_TOKEN is not set in the environment variables")
client = WebClient(token=BOT_TOKEN)


def send_message(message, channel=CHANNEL):
    logger.info(f"Sending message: {message}")
    if not channel:
        logger.error("SLACK_CHANNEL is not set in the environment variables")
        return
    try:
        response = client.chat_postMessage(channel=channel, text=message)
        logger.info(f"Message sent: {response}")
    except SlackApiError as e:
        logger.error(f"Error sending message: {e.response['error']}")


#
#
# def handle_message(client: WebClient, event: dict):
#     data = event["data"]
#     logger.info(f"Received message: {data}")
#
#     # Check if the message is in the specified channel and not sent by a bot
#     if "subtype" in data and data["subtype"] == "bot_message":
#         return
#
#     channel_id = data["channel"]
#     user = data["user"]
#     text = data.get("text", "")
#     logger.info(f"Message from user {user} in channel {channel_id}: {text}")
#
#     # Perform some action based on the message text
#     if "hello" in text.lower():
#         response_text = f"Hello <@{user}>!"
#         send_message(response_text, channel=channel_id)
#
#
# def check_token_permissions():
#     client = WebClient(token=BOT_TOKEN)
#     try:
#         response = client.auth_test()
#         if response["ok"] and response["bot_id"]:
#             print("Token is valid and has the necessary permissions.")
#         else:
#             raise ValueError("Invalid token or insufficient permissions.")
#     except SlackApiError as e:
#         print(f"Error testing token: {e.response['error']}")
#         raise
#
#
# async def run_rtm_client():
#     rtm_client = RTMClient(token=BOT_TOKEN)
#     check_token_permissions()
#     rtm_client.on(event_type="message")(handle_message)
#     logger.info("Starting RTM client")
#     await asyncio.to_thread(rtm_client.start)
