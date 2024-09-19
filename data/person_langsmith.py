import json
import sys
import os
import asyncio

from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.utils import env_utils

from ai.langsmith.langsmith_loader import Langsmith
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.dependencies.dependencies import companies_repository, profiles_repository
from common.genie_logger import GenieLogger

logger = GenieLogger()
load_dotenv()

PERSON_PORT = env_utils.get("PERSON_PORT", 8005)

CONSUMER_GROUP_LANGSMITH = "langsmithconsumergroup"


class LangsmithConsumer(GenieConsumer):
    def __init__(self):
        super().__init__(
            topics=[Topic.NEW_PERSONAL_DATA],
            consumer_group=CONSUMER_GROUP_LANGSMITH,
        )
        self.langsmith = Langsmith()
        self.company_repository = companies_repository()
        self.profiles_repository = profiles_repository()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        logger.info(f"Processing event on topic {event.properties.get(b'topic').decode('utf-8')}")
        topic = event.properties.get(b"topic").decode("utf-8")

        match topic:
            case Topic.NEW_PERSONAL_DATA:
                logger.info("Handling new personal data to process")
                await self.handle_new_personal_data(event)

    async def handle_new_personal_data(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        personal_data = event_body.get("personal_data")
        person_data = {"personal_data": personal_data}
        person = event_body.get("person")
        email_address = person.get("email")
        profile = self.profiles_repository.get_profile_data_by_email(email_address)
        if profile:
            logger.info(f"Profile already exists: {profile}")
            return {"status": "success"}
        profile = self.profiles_repository.get_profile_data(person.get("uuid"))
        if profile:
            logger.info(f"Profile already exists: {profile}")
            return {"status": "success"}
        self.profiles_repository.insert_profile_without_strengths_and_get_to_know(person)
        logger.info(f"Person from NEW_PERSONAL_DATA event: {email_address}")
        company_data = None
        company_dict = {}

        if email_address and isinstance(email_address, str) and "@" in email_address:
            company_data = self.company_repository.get_company_from_domain(email_address.split("@")[1])
        if company_data:
            company_dict = company_data.to_dict()
            company_dict.pop("uuid")
            company_dict.pop("domain")
            company_dict.pop("employees")
            company_dict.pop("logo")

        response = await self.langsmith.get_profile(person_data, company_dict)
        logger.info(f"Response: {response.keys() if isinstance(response, dict) else response}")

        profile_strength_and_get_to_know = {
            "strengths": response.get("strengths"),
            "get_to_know": response.get("get_to_know"),
        }

        data_to_send = {"person": person, "profile": str(profile_strength_and_get_to_know)[:300]}

        logger.info(f"About to send event's data: {data_to_send}")

        event = GenieEvent(Topic.NEW_PROCESSED_PROFILE, data_to_send, "public")
        event.send()
        return {"status": "success"}


if __name__ == "__main__":
    langsmith_consumer = LangsmithConsumer()
    try:
        asyncio.run(langsmith_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
