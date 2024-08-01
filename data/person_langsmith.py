import json
import sys
import os

from dotenv import load_dotenv
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ai.langsmith.langsmith_loader import Langsmith
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.dependencies.dependencies import companies_repository

load_dotenv()


PERSON_PORT = os.environ.get("PERSON_PORT", 8005)

CONSUMER_GROUP_LANGSMITH = "langsmithconsumergroup"


class LangsmithConsumer(GenieConsumer):
    def __init__(self):
        super().__init__(
            topics=[Topic.NEW_PERSONAL_DATA, Topic.FAILED_TO_GET_DOMAIN_INFO],
            consumer_group=CONSUMER_GROUP_LANGSMITH,
        )
        self.langsmith = Langsmith()
        self.company_repository = companies_repository()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        logger.info(
            f"Processing event on topic {event.properties.get(b'topic').decode('utf-8')}"
        )
        topic = event.properties.get(b"topic").decode("utf-8")

        match topic:
            case Topic.NEW_PERSONAL_DATA:
                logger.info("Handling new personal data to process")
                await self.handle_new_personal_data(event)
            case Topic.FAILED_TO_ENRICH_DATA:
                logger.info("Handling failed attempt to enrich data")
                await self.handle_failed_to_enrich_data(event)
            case Topic.FAILED_TO_GET_DOMAIN_INFO:
                logger.info("Handling failed attempt to enrich email")
                await self.handle_failed_to_enrich_email(event)

    async def handle_new_personal_data(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        personal_data = event_body.get("personal_data")
        person_data = {"personal_data": personal_data}
        person = event_body.get("person")
        logger.info(f"Person: {person}")
        email_address = person.get("email")
        company_data = None
        company_dict = {}
        if email_address and isinstance(email_address, str) and "@" in email_address:
            company_data = self.company_repository.get_company_from_domain(
                email_address.split("@")[1]
            )
        if company_data:
            company_dict = company_data.to_dict()
            company_dict.pop("uuid")
            company_dict.pop("domain")
            company_dict.pop("employees")
        logger.info(f"Company data: {company_data}")

        response = await self.langsmith.get_profile(person_data, company_dict)
        logger.info(
            f"Response: {response.keys() if isinstance(response, dict) else response}"
        )

        profile = {
            "strengths": response.get("strengths"),
            "get_to_know": response.get("get_to_know"),
            "news": response.get("news"),
        }

        data_to_send = {"person": person, "profile": profile}

        logger.info(f"About to send event's data: {data_to_send}")

        event = GenieEvent(Topic.NEW_PROCESSED_PROFILE, data_to_send, "public")
        event.send()
        return {"status": "success"}

    async def handle_failed_to_enrich_data(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {event_body}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person = event_body.get("person")
        logger.info(f"Person: {person}")

        # Ask ChatGPT (through Langsmith) what can be told about this person
        response = self.langsmith.ask_chatgpt(
            f"What can you tell me about this person: {person}"
        )
        logger.info(f"Response from ChatGPT: {response}")

        data_to_send = {"person": person, "additional_info": response}
        event = GenieEvent(Topic.NEW_PROCESSED_PROFILE, data_to_send, "public")
        event.send()
        return {"status": "success"}

    async def handle_failed_to_enrich_email(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {event_body}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        email_address = event_body.get("email")
        logger.info(f"Email address: {email_address}")

        company = event_body.get("company")
        logger.debug(f"Company: {company}")

        # Ask ChatGPT (through Langsmith) to find the LinkedIn URL
        # response = self.langsmith.run_prompt_linkedin_url(email_address, company)

        response = None  # Need to implement the Chatgpt search better. got too many made up linkedin urls

        logger.info(f"Response from ChatGPT: {response}")

        # Verify the response and ensure it is a valid LinkedIn URL
        linkedin_url = (
            response.get("linkedin_url")
            if (response and isinstance(response, dict))
            else None
        )
        if linkedin_url and "linkedin.com" in linkedin_url:
            logger.info(f"Found LinkedIn URL: {linkedin_url}")
            data_to_send = {"email": email_address, "linkedin_url": linkedin_url}
            event = GenieEvent(Topic.NEW_LINKEDIN_URL, data_to_send, "public")
            event.send()
            return {"status": "success"}
        else:
            logger.warning("No valid LinkedIn URL found.")
            data_to_send = {
                "email": email_address,
                "error": "No valid LinkedIn URL found",
            }
            event = GenieEvent(Topic.FAILED_TO_GET_LINKEDIN_URL, data_to_send, "public")
            event.send()
            return {"status": "failed"}
        #
        # event = GenieEvent(Topic.NEW_PROCESSED_PROFILE, data_to_send, "public")
        # event.send()
        # return {"status": "success"}
