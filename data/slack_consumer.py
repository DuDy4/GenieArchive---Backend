import asyncio
import json
import os
import sys
import traceback


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from dotenv import load_dotenv
load_dotenv()
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.api_services.slack_bot import send_message


from data.data_common.utils.str_utils import get_uuid4

from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.dependencies.dependencies import companies_repository
from common.genie_logger import GenieLogger

logger = GenieLogger()
CONSUMER_GROUP = "slack_consumer_group"


class SlackConsumer(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[
                Topic.FAILED_TO_GET_LINKEDIN_URL,
                Topic.FAILED_TO_GET_PERSONAL_DATA,
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.company_repository: CompaniesRepository = companies_repository()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        match topic:
            case Topic.FAILED_TO_GET_LINKEDIN_URL:
                logger.info("Handling failed attempt to get linkedin url")
                await self.handle_failed_to_get_linkedin_url(event)
            case Topic.FAILED_TO_GET_PERSONAL_DATA:
                logger.info("Handling failed attempt to enrich data")
                await self.handle_failed_to_get_personal_data(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_failed_to_get_linkedin_url(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        email = event_body.get("email")
        domain = email.split("@")[1] if "@" in email else None
        company = None
        if domain:
            company = self.company_repository.get_company_from_domain(email.split("@")[1])
        message = f"failed to identify info for email: {email}."
        if company:
            message += f"""
            We know that this domain is associated with a company {company.name}.

            {"Company size: " + company.size if company and company.size else ""}

            {company.overview if company and company.overview else ""}
            """
        send_message(message)

    async def handle_failed_to_get_personal_data(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person = event_body.get("person")
        message = f"""
        Failed to enrich data for person: {person}.
        """
        send_message(message)


if __name__ == "__main__":
    slack_consumer = SlackConsumer()
    try:
        asyncio.run(slack_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
