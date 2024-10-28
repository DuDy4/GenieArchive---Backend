import base64
import json
import os
import asyncio
import datetime
from urllib.parse import quote


from email.mime.text import MIMEText
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.dependencies.dependencies import (
    google_creds_repository,
    meetings_repository,
    tenants_repository,
)
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.topics import Topic
from common.genie_logger import GenieLogger
from common.utils import env_utils

logger = GenieLogger()

CONSUMER_GROUP = "email_manager_consumer_group"
APP_URL = env_utils.get("APP_URL")


class EmailManager(GenieConsumer):
    def __init__(self):
        super().__init__(
            topics=[Topic.NEW_UPCOMING_MEETING],
            consumer_group=CONSUMER_GROUP,
        )
        self.meetings_repository = meetings_repository()
        self.tenants_repository = tenants_repository()
        self.email_sender = GmailSender()

    async def process_event(self, event):
        logger.info(f"EmailManager processing event: {event}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")

        match topic:
            case Topic.NEW_UPCOMING_MEETING:
                logger.info("Handling new upcoming meeting event")
                return await self.handle_meeting_reminder(event)
            case _:
                logger.info(f"Unknown topic: {topic}. No handler found for this event")
                return {"status": "error", "message": f"Unknown topic: {topic}"}

    async def handle_meeting_reminder(self, event):
        event_body = event.body_as_str()
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        meeting_uuid = event_body.get("meeting_uuid")
        logger.info(f"Handling meeting reminder for meeting UUID: {meeting_uuid}")
        already_sent = self.meetings_repository.has_sent_meeting_reminder(meeting_uuid)
        if already_sent:
            logger.info(f"Reminder already sent for meeting with UUID {meeting_uuid}")
            return {
                "status": "success",
                "message": f"Reminder already sent for meeting with UUID {meeting_uuid}",
            }
        meeting = self.meetings_repository.get_meeting_data(meeting_uuid)
        if not meeting:
            logger.error(f"Meeting with UUID {meeting_uuid} not found")
            return {"status": "error", "message": f"Meeting with UUID {meeting_uuid} not found"}
        logger.info(f"Sending reminder for meeting with UUID {meeting_uuid}")
        tenant_id = meeting.tenant_id
        if not tenant_id:
            logger.error(f"Tenant ID not found for meeting with UUID {meeting_uuid}")
            return {"status": "error", "message": f"Tenant ID not found for meeting with UUID {meeting_uuid}"}
        tenant_email = self.tenants_repository.get_tenant_email(tenant_id)
        if not tenant_email:
            logger.error(f"Tenant email not found for tenant ID {tenant_id}")
            return {"status": "error", "message": f"Tenant email not found for tenant ID {tenant_id}"}

        message_to_send = self.email_sender.create_meeting_reminder_email(meeting)
        logger.info(f"Sending reminder email to {tenant_email}: {message_to_send}")


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

    def create_meeting_reminder_email(self, meeting: MeetingDTO):
        """Creates a meeting reminder email."""
        body_text = f"""
        Hello,\n\n
        This is a reminder for your upcoming meeting on {meeting.start_time}.\n
        If you want to refresh your memory about the meeting, please visit the following link: {self.create_meeting_link(meeting)}\n
        \n\nBest regards,\nGenieAI
        """
        return body_text

    def create_meeting_link(self, meeting: MeetingDTO):
        """Creates a meeting link for a meeting."""
        encoded_subject = quote(meeting.subject) if meeting.subject else ""
        return f"{APP_URL}/meeting/{meeting.uuid}?name={encoded_subject}"


if __name__ == "__main__":
    email_manager = EmailManager()
    asyncio.run(email_manager.start())
