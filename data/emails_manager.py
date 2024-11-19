import base64
import json
import os
import asyncio
import datetime
import traceback
from urllib.parse import quote


from email.mime.text import MIMEText
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from common.utils.email_utils import filter_email_objects, filter_emails
from data.data_common.data_transfer_objects.company_dto import CompanyDTO
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO, MeetingClassification
from ai.langsmith.langsmith_loader import Langsmith
from data.data_common.dependencies.dependencies import (
    google_creds_repository,
    meetings_repository,
    tenants_repository,
    profiles_repository,
    companies_repository,
)
from data.api_services.embeddings import GenieEmbeddingsClient
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from common.genie_logger import GenieLogger
from common.utils import env_utils


logger = GenieLogger()

SENDER_EMAIL_ADDRESS = env_utils.get("SENDER_EMAIL_ADDRESS")
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
        self.companies_repository = companies_repository()
        self.profiles_repository = profiles_repository()
        self.email_sender = GmailSender()
        self.langsmith = Langsmith()
        self.embeddings_client = GenieEmbeddingsClient()
        self.email_address = SENDER_EMAIL_ADDRESS

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
        if not meeting or not meeting.classification == MeetingClassification.EXTERNAL:
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

        filtered_emails = filter_emails(tenant_email, meeting.participants_emails)
        logger.info(f"Filtered emails: {filtered_emails}")
        filtered_profiles = self.profiles_repository.get_profiles_dto_by_email_list(filtered_emails)
        if not filtered_profiles:
            logger.error("No profiles found for filtered emails")
            return {"status": "error", "message": "No profiles found for filtered emails"}
        target_companies = self.companies_repository.get_companies_from_domains([email.split("@")[1] for email in filtered_emails])
        if not target_companies:
            logger.error("No target companies found for filtered emails")
            return {"status": "error", "message": "No target companies found for filtered emails"}
        target_company = target_companies[0]
        target_company.employees = None

        seller_context = None
        if tenant_email:
            seller_context = self.embeddings_client.search_materials_by_prospect_data(
                tenant_email, filtered_profiles[0]
            )
            seller_context = " || ".join(seller_context) if seller_context else None

        meeting_summary = await self.langsmith.get_meeting_summary(
            meeting_data=meeting, seller_context=seller_context, profiles=filtered_profiles, company_data=target_company
        )
        logger.info(f"Meeting summary response: {meeting_summary}")

        if not meeting_summary:
            logger.error("No meeting summary response received")
            return {"status": "error", "message": "No meeting summary response received"}

        email_content = self.email_sender.create_meeting_reminder_email_body(meeting, meeting_summary,
                                                                             target_company, filtered_profiles)
        result = self.email_sender.send_email(
            user_email=self.email_address,
            recipient=tenant_email if tenant_email else 'asaf@genieai.ai',
            subject="Meeting Reminder",
            body_text=email_content  # Pass the HTML content here
        )
        if result:
            self.meetings_repository.update_senders_meeting_reminder(meeting_uuid)
            logger.info(f"Meeting reminder sent successfully for meeting with UUID {meeting_uuid}")
            return {"status": "success", "message": f"Meeting reminder sent successfully for meeting with UUID {meeting_uuid}"}
        else:
            logger.error(f"Failed to send meeting reminder for meeting with UUID {meeting_uuid}")
            return {"status": "error", "message": f"Failed to send meeting reminder for meeting with UUID {meeting_uuid}"}



class GmailSender:
    SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

    def __init__(self):
        self.email_address = SENDER_EMAIL_ADDRESS
        self.google_creds = json.loads(base64.b64decode(os.getenv('GOOGLE_SERVICE_JSON')).decode('utf-8'))

    def authenticate_gmail(self, user_email):
        """Authenticate with Gmail API using stored credentials from google_creds table."""
        if not self.google_creds:
            raise Exception("Google credentials not found for user.")

        credentials = service_account.Credentials.from_service_account_info(self.google_creds, scopes=self.SCOPES)
        delegated_credentials = credentials.with_subject(self.email_address)

        # Build the Gmail service
        service = build('gmail', 'v1', credentials=delegated_credentials)

        return service

    def create_email(self, recipient, subject, body_html):
        message = MIMEText(body_html, "html")
        message["to"] = recipient
        message["from"] = self.email_address
        message["subject"] = subject
        message["Bcc"] = 'asaf@genieai.ai'

        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
        return {"raw": encoded_message}

    def send_email(self, user_email, recipient, subject, body_text):
        """Sends an email using Gmail API with credentials from google_creds table."""
        try:
            service = self.authenticate_gmail(user_email)
            email_msg = self.create_email(recipient, subject, body_text)
            result = service.users().messages().send(userId="me", body=email_msg).execute()
            logger.info(f"Email sent successfully to {recipient}, Message ID: {result['id']}")
            return result
        except Exception as e:
            logger.error(f"An error occurred while sending email: {e}")
            event = GenieEvent(
                topic=Topic.EMAIL_SENDING_FAILED,
                data=json.dumps({"recipient": recipient, "subject": subject, "error": str(e)}),
            )
            event.send()
            traceback.print_exc()

    def create_meeting_reminder_email_body(self, meeting: MeetingDTO, meeting_summary: dict, company: CompanyDTO, profiles: list):
        """Creates a polished, styled HTML meeting reminder email that matches Genie AI's website styling."""
        company_overview = meeting_summary.get("company_overview")
        attendees = meeting_summary.get("attendees")
        for attendee in attendees:
            for profile in profiles:
                if attendee["name"] == profile.name:
                    attendee["profile_picture"] = str(profile.picture_url)

        key_points = meeting_summary.get("key_points")
        meeting_subject = meeting.subject  # Assume 'subject' is a property of MeetingDTO

        # Format attendees
        attendees_str = "".join(
            f"""<li style="margin-bottom: 15px;">
                    <table style="width: 100%; margin: 0;">
                        <tr>
                            <td style="width: 60px; vertical-align: middle;">
                                {f'<img src="{attendee.get("profile_picture")}" style="width: 60px; height: 60px; border-radius: 50%;" />' if attendee.get("profile_picture") else ''}
                            </td>
                            <td style="vertical-align: middle;">
                                <strong style="font-size: 16px; color: #1A1A1A;">{attendee['name']}</strong><br>
                                <span style="font-size: 14px; color: #666666;">{attendee['summary']}</span>
                            </td>
                        </tr>
                    </table>
                </li>""" for attendee in attendees
        )

        # Format key points
        key_points_str = "".join(f"<li style='margin-bottom: 10px;'>{key_point}</li>" for key_point in key_points)

        # Email body with inline styling for Gmail compatibility
        body_html = f"""
        <html>
        <body style="font-family: 'Roboto', Arial, sans-serif; color: #333333; line-height: 1.6;">
            <div style="max-width: 600px; margin: auto; padding: 20px; border: 1px solid #e0e0e0; border-radius: 8px;">
                <p style="font-size: 18px; font-weight: bold; color: #333333;">
                    👋 Hello,
                </p>
                
                <p style="font-size: 16px; color: #444444;">
                    Just wanted to remind you that we have your back for your upcoming meeting! 🚀💼
                </p>
                <p style="font-size: 20px; font-weight: bold; color: #004080; text-align: center">
                    Subject: {meeting_subject}
                </p>
    
                <div style="margin-top: 20px;">
                    <h3 style="font-size: 22px; color: #004080; border-bottom: 2px solid #E0E0E0; padding-bottom: 8px; font-weight: 700;">Company Overview</h3>
                    <table style="width: 100%; margin-top: 10px; margin-left: 15px;">
                        <tr>
                            <td style="width: 60px; vertical-align: top;">
                                {f'<img src="{company.logo}" style="width: 60px; height: 60px; border-radius: 8px;" />' if company and company.logo else ''}
                            </td>
                            <td style="vertical-align: top;">
                                <p style="font-size: 15px; color: #666666; margin: 0;">{company_overview}</p>
                            </td>
                        </tr>
                    </table>
                </div>
    
                {f"""<div style="margin-top: 20px;">
                    <h3 style="font-size: 22px; color: #004080; border-bottom: 2px solid #E0E0E0; padding-bottom: 8px; font-weight: 700;">Attendees</h3>
                    <ul style="list-style-type: none; padding: 0; margin: 0; font-size: 14px; color: #333333;">
                        {attendees_str}
                    </ul>
                </div>""" if attendees else ""}
    
                {f"""
                <div style="margin-top: 20px;">
                    <h3 style="font-size: 22px; color: #004080; border-bottom: 2px solid #E0E0E0; padding-bottom: 8px; font-weight: 700;">Key Points</h3>
                    <ul style="padding-left: 20px; color: #333333; font-size: 14px; margin: 0;">
                        {"".join(
            f"<li style='background-color: {('#f9f9ff' if i % 2 == 0 else '#ffffff')}; padding: 8px; border-radius: 5px; margin-bottom: 5px; list-style: \"🔹\";'>{point}</li>"
            for i, point in enumerate(key_points)
        )}
                    </ul>
                </div>
                """ if key_points else ""}
    
                <div style="margin-top: 20px; text-align: center;">
                    <a href="{self.create_meeting_link(meeting)}" style="display: inline-block; padding: 12px 24px; font-size: 16px; color: #ffffff; background-color: #7A5CFA; border-radius: 5px; text-decoration: none; margin-top: 10px;">
                        View More Meeting Details
                    </a>
                </div>
    
                <table style="margin-top: 30px;">
                    <tr>
                        <td style="vertical-align: middle; padding-right: 10px;">
                            <img src="https://alpha.genieai.ai/images/image9.png" style="width: 40px; height: 40px;" alt="GenieAI logo">
                        </td>
                        <td style="vertical-align: middle;">
                            <p style="font-size: 16px; color: #333333; margin: 0;">
                                Best wishes,<br>
                                <strong style="color: #7A5CFA;">GenieAI</strong>
                            </p>
                        </td>
                    </tr>
                </table>
                
                <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #E0E0E0; text-align: center; color: #999999; font-size: 12px;">
                    <p style="margin: 0;">You are receiving this email because you have a scheduled meeting with GenieAI.</p>
                    <p style="margin: 5px 0;">
                        If you no longer wish to receive these reminders, you can <a href="{self.create_unsubscribe_link(meeting)}" style="color: #7A5CFA; text-decoration: none;">unsubscribe</a> here.
                    </p>
                </div>
    
            </div>
        </body>
        </html>
        """
        return body_html



    def create_meeting_link(self, meeting: MeetingDTO):
        """Creates a meeting link for a meeting."""
        encoded_subject = quote(meeting.subject) if meeting.subject else ""
        return f"{APP_URL}/meeting/{meeting.uuid}?name={encoded_subject}"

    def create_unsubscribe_link(self, meeting):
        return f"{APP_URL}/unsubscribe/{meeting.tenant_id}"


if __name__ == "__main__":
    email_manager = EmailManager()
    asyncio.run(email_manager.start())
