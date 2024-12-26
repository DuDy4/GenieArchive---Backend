import asyncio
import json
import os
import sys
import time
import traceback
from datetime import timedelta, datetime

from data.data_common.data_transfer_objects.person_dto import PersonDTO, PersonStatus
from data.data_common.data_transfer_objects.status_dto import StatusEnum
from data.data_common.repositories.tenants_repository import TenantsRepository
from data.data_common.repositories.statuses_repository import StatusesRepository

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from dotenv import load_dotenv

load_dotenv()
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.topics import Topic
from data.api_services.slack_bot import send_message

from common.utils import env_utils

from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.dependencies.dependencies import companies_repository, persons_repository, tenants_repository
from common.genie_logger import GenieLogger

logger = GenieLogger()
CONSUMER_GROUP = "slack_consumer_group"

TIME_PERIOD_TO_SENT_MESSAGE = int(
    env_utils.get("SLACK_TIME_PERIOD_TO_SEND_MESSAGE") or 30 * 60 * 60 * 24 * 7
)  # One month


class SlackConsumer(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[
                Topic.FAILED_TO_ENRICH_PERSON,
                Topic.FAILED_TO_ENRICH_EMAIL,
                Topic.FAILED_TO_GET_PROFILE_PICTURE,
                Topic.EMAIL_SENDING_FAILED,
                Topic.BUG_IN_TENANT_ID,
                Topic.PROFILE_ERROR
                # Should implement Topic.FAILED_TO_GET_COMPANY_DATA
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.company_repository: CompaniesRepository = companies_repository()
        self.persons_repository: PersonsRepository = persons_repository()
        self.tenants_repository: TenantsRepository = tenants_repository()
        self.statuses_repository: StatusesRepository = StatusesRepository()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        match topic:
            case Topic.FAILED_TO_ENRICH_PERSON:
                logger.info("Handling failed attempt to enrich person")
                await self.handle_failed_to_get_personal_data(event)
            case Topic.FAILED_TO_ENRICH_EMAIL:
                logger.info("Handling failed attempt to enrich email")
                await self.handle_failed_to_get_personal_data(event)
            case Topic.FAILED_TO_GET_PROFILE_PICTURE:
                logger.info("Handling failed attempt to get profile picture")
                await self.handle_failed_to_get_profile_picture(event)
            case Topic.EMAIL_SENDING_FAILED:
                logger.info("Handling failed email sending")
                await self.handle_email_sender_failed(event)
            case Topic.BUG_IN_TENANT_ID:
                logger.info("Handling bug in tenant id")
                await self.handle_bug_in_tenant_id(event)
            case Topic.PROFILE_ERROR:
                logger.info("Handling profile error")
                await self.handle_profile_error(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_failed_to_get_personal_data(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        email = event_body.get("email")
        person = event_body.get("person")
        if not email and person:
            person = PersonDTO.from_dict(person)
            email = person.email
            if not email:
                raise Exception("Could not find email in event body.")
        domain = email.split("@")[1] if (email and "@" in email) else None
        tenant_id = logger.get_tenant_id()
        last_message_sent_at = self.persons_repository.get_last_message_sent_at_by_email(email)
        if last_message_sent_at:
            time_period_delta = timedelta(seconds=TIME_PERIOD_TO_SENT_MESSAGE)

            if last_message_sent_at + time_period_delta > datetime.now():
                logger.info("Already sent message lately. Skipping...")
                return {"status": "skipped", "message": "Already sent message lately. Skipping..."}

        company = None
        if domain:
            company = self.company_repository.get_company_from_domain(email.split("@")[1])
        message = f"[CTX={logger.get_ctx_id()}] {f'[CTY={logger.clean_cty_id()}]'if logger.get_cty_id() else ''} failed to identify info for email: {email}."
        if tenant_id:
            tenant_email = self.tenants_repository.get_tenant_email(tenant_id)
            if tenant_email:
                message += f"""
                    Originating user: {tenant_email}.
                    """
        if company:
            message += f"""
            COMPANY= {company.name}.
            """
        send_message(message)
        self.persons_repository.update_status(email, PersonStatus.FAILED)
        self.persons_repository.update_last_message_sent_at_by_email(email)
        return {"status": "ok"}

    async def handle_failed_to_get_profile_picture(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person = event_body.get("person")
        person = PersonDTO.from_dict(person)
        if not person:
            message = f"[CTX={logger.get_ctx_id()}] failed to get profile picture - person not found."
            send_message(message)
            return {"status": "failed", "message": "Person not found."}
        if not person.linkedin:
            message = (f"[CTX={logger.get_ctx_id()}] failed to get profile picture - person linkedin not found."
                       f"Person: {person.name}, email: {person.email}.")
            send_message(message)
            return {"status": "failed", "message": "Person linkedin not found."}
        tenant_id = logger.get_tenant_id()

        last_message_sent_at = self.persons_repository.get_last_message_sent_at_by_email(person.email)
        if last_message_sent_at:
            time_period_delta = timedelta(seconds=TIME_PERIOD_TO_SENT_MESSAGE)

            if last_message_sent_at + time_period_delta > datetime.now():
                logger.info("Already sent message lately. Skipping...")
                return {"status": "skipped", "message": "Already sent message lately. Skipping..."}
        message = f"[CTX={logger.get_ctx_id()}] failed to get profile picture for person: {person.name} and linkedin: {person.linkedin}."
        if tenant_id:
            email = self.tenants_repository.get_tenant_email(tenant_id)
            if email:
                message += f"""
                    Originating user: {email}.
                    """
        send_message(message)
        self.persons_repository.update_last_message_sent_at_by_email(person.email)
        return {"status": "ok"}

    async def handle_email_sender_failed(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        error = event_body.get("error")
        recipient = event_body.get("recipient")
        subject = event_body.get("subject")
        tenant_id = logger.get_tenant_id()

        message = f"[CTX={logger.get_ctx_id()}] failed to send email to: {recipient} with subject: {subject}. Error: {error}."
        if tenant_id:
            email = self.tenants_repository.get_tenant_email(tenant_id)
            if email:
                message += f"""
                    Originating user: {email}.
                    """
        send_message(message, channel="bugs")
        return {"status": "ok"}

    async def handle_bug_in_tenant_id(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        tenant_id = event_body.get("tenant_id")
        logger_tenant_id = logger.get_tenant_id()
        message = f"[CTX={logger.get_ctx_id()}] found a bug in tenant_id: {tenant_id} and logger_tenant_id: {logger_tenant_id}."
        send_message(message, channel="bugs")
        return {"status": "ok"}

    async def handle_profile_error(self, event):
        """
        Should send a message to slack about the error
        """
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)

        logger_info = logger.get_extra()
        logger_info_str = ", ".join(f"{k}: {v}" for k, v in logger_info.items())
        error = event_body.get("error")
        traceback_logs = event_body.get("traceback")
        email = event_body.get("email")
        uuid = event_body.get("uuid")
        topic = event_body.get("topic")
        consumer_group = event_body.get("consumer_group")
        message = (f"{logger_info_str} error occurred in topic: {topic} and consumer_group: {consumer_group}."
                   f"\nWhile processing {email or uuid}."
                   f" \nError: {error}."
                   f" \nTraceback: {traceback_logs}.")
        send_message(message, channel="bugs")
        self.persons_repository.update_status(email, PersonStatus.FAILED)
        self.statuses_repository.update_status(uuid, email, topic, StatusEnum.FAILED)
        return {"status": "ok"}


if __name__ == "__main__":
    slack_consumer = SlackConsumer()
    try:
        asyncio.run(slack_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
