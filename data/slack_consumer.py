import asyncio
import json
import os
import sys
import time
import traceback
from datetime import timedelta, datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from dotenv import load_dotenv

load_dotenv()
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.topics import Topic
from data.api_services.slack_bot import send_message

from common.utils import env_utils

from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.dependencies.dependencies import companies_repository, persons_repository
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
                Topic.APOLLO_FAILED_TO_ENRICH_PERSON,
                Topic.APOLLO_FAILED_TO_ENRICH_EMAIL
                # Should implement Topic.FAILED_TO_GET_COMPANY_DATA
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.company_repository: CompaniesRepository = companies_repository()
        self.persons_repository: PersonsRepository = persons_repository()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        match topic:
            case Topic.APOLLO_FAILED_TO_ENRICH_PERSON:
                logger.info("Handling failed attempt to enrich person")
                await self.handle_failed_to_get_personal_data(event)
            case Topic.APOLLO_FAILED_TO_ENRICH_EMAIL:
                logger.info("Handling failed attempt to enrich email")
                await self.handle_failed_to_get_personal_data(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_failed_to_get_personal_data(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        email = event_body.get("email")
        domain = email.split("@")[1] if "@" in email else None

        last_message_sent_at = self.persons_repository.get_last_message_sent_at_by_email(email)
        if last_message_sent_at:
            time_period_delta = timedelta(seconds=TIME_PERIOD_TO_SENT_MESSAGE)

            if last_message_sent_at + time_period_delta > datetime.now():
                logger.info("Already sent message lately. Skipping...")
                return {"status": "skipped", "message": "Already sent message lately. Skipping..."}

        company = None
        if domain:
            company = self.company_repository.get_company_from_domain(email.split("@")[1])
        message = f"[CTX={logger.get_ctx_id()}] failed to identify info for email: {email}."
        if company:
            message += f"""
            COMPANY= {company.name}.
            """
        send_message(message)
        self.persons_repository.update_last_message_sent_at_by_email(email)
        return {"status": "ok"}


if __name__ == "__main__":
    slack_consumer = SlackConsumer()
    try:
        asyncio.run(slack_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
