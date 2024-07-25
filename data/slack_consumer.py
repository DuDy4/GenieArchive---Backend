import json
import os
import sys

from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.slack.slack_bot import send_message


from data.data_common.utils.str_utils import get_uuid4

from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.dependencies.dependencies import companies_repository

CONSUMER_GROUP = "slack_consumer_group" + os.environ.get("CONSUMER_GROUP_NAME", "")


class SlackConsumer(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[
                Topic.FAILED_TO_GET_DOMAIN_INFO,
                Topic.FAILED_TO_ENRICH_DATA,
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.company_repository: CompaniesRepository = companies_repository()

    async def process_event(self, event):
        logger.info(f"PersonManager processing event: {event}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        match topic:
            case Topic.FAILED_TO_GET_DOMAIN_INFO:
                logger.info("Handling failed attempt to get domain info")
                await self.handle_failed_to_get_domain_info(event)
            case Topic.FAILED_TO_ENRICH_DATA:
                logger.info("Handling failed attempt to enrich data")
                await self.handle_failed_to_enrich_data(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_failed_to_get_domain_info(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        email = event_body.get("email")
        domain = self.company_repository.get_company_from_domain(email.split("@")[1])
        message = f"""
        failed to identify info for email: {email}.
        We know that this domain is associated with a company {domain["name"]}.
        {'Description: ' + domain["description"] if domain["description"] else ""}
        {'Technologies: ' + ', '.join(domain["technologies"]) if domain["technologies"] else ""}
        {'Known employees: ' + ', '.join([f'{employee["name"]} (Position: {employee["position"] if employee["position"] else "Unknown"})' for employee in domain["employees"]]) if domain["employees"] else ""}
        """
        send_message(message)

    async def handle_failed_to_enrich_data(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person = event_body.get("person")
        message = f"""
        Failed to enrich data for person: {person}.
        """
        send_message(message)
