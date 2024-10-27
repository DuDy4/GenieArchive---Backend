import base64
import json
import os
import asyncio
import datetime

from email.mime.text import MIMEText
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from data.data_common.dependencies.dependencies import google_creds_repository, profiles_repository
from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.topics import Topic
from common.genie_logger import GenieLogger
from common.utils import env_utils

logger = GenieLogger()

CONSUMER_GROUP = "emailmanagerconsumergroup"
APP_URL = env_utils.get("APP_URL")


class EmailManager(GenieConsumer):
    def __init__(self):
        super().__init__(
            topics=[Topic.FINISHED_NEW_PROFILE],
            consumer_group=CONSUMER_GROUP,
        )
        self.profiles_repository = profiles_repository()
        self.gmail_sender = GmailSender()

    async def process_event(self, event):
        logger.info(f"EmailManager processing event: {event}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")

        match topic:
            case Topic.FINISHED_NEW_PROFILE:
                logger.info("Handling new salesforce contact")
                await self.handle_finished_new_profile(event)

    async def handle_finished_new_profile(self, event):
        event_body = event.body_as_str()
        profile = ProfileDTO.from_json(json.loads(event_body))
        logger.info(f"Profile: {profile}, type: {type(profile)}")
        self.gmail_sender.send_before_the_meeting_link_email(profile)


class GmailSender:
    SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

    def __init__(self):
        self.google_creds_repo = google_creds_repository()

    def authenticate_gmail(self, user_email):
        """Authenticate with Gmail API using stored credentials from google_creds table."""
        google_credentials = self.google_creds_repo.get_creds(user_email)
        if not google_credentials:
            raise Exception("Google credentials not found for user.")

        creds = Credentials(
            token=google_credentials.get("access_token"),
            refresh_token=google_credentials.get("refresh_token"),
            client_id=env_utils.get("GOOGLE_CLIENT_ID"),
            client_secret=env_utils.get("GOOGLE_CLIENT_SECRET"),
            token_uri="https://oauth2.googleapis.com/token",
        )

        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            # Update tokens in the database after refreshing
            self.google_creds_repo.update_google_creds(
                user_email=user_email,
                user_access_token=creds.token,
                user_refresh_token=creds.refresh_token,
            )

        return build("gmail", "v1", credentials=creds)

    def create_email(self, recipient, subject, body_text):
        message = MIMEText(body_text)
        message["to"] = recipient
        message["from"] = "your_email@gmail.com"  # Use the Gmail address that has authorized this app
        message["subject"] = subject
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
        return {"raw": encoded_message}

    def send_email(self, user_email, recipient, subject, body_text):
        """Sends an email using Gmail API with credentials from google_creds table."""
        try:
            service = self.authenticate_gmail(user_email)
            email_msg = self.create_email(recipient, subject, body_text)
            result = service.users().messages().send(userId="me", body=email_msg).execute()
            logger.info(f"Email sent successfully to {recipient}, Message ID: {result['id']}")
        except Exception as e:
            logger.error(f"An error occurred while sending email: {e}")

    def send_before_the_meeting_link_email(self, profile):
        """Sends a 'before the meeting' email for a profile."""
        user_email = profile.email  # Assuming `profile` has an email attribute
        name = profile.name.replace(" ", "-")
        link = f"{APP_URL}/profiles/{name}/before-the-meeting"
        subject = "Before the Meeting"
        body_text = f"Hello {profile.name},\n\nPlease review this link before the meeting:\n{link}\n\nBest regards,\nYour Team"
        self.send_email(user_email, profile.email, subject, body_text)  # Send to user's email
