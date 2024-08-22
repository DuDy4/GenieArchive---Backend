import json
import sys
import os
import requests
import asyncio
from dotenv import load_dotenv

from common.utils import env_utils
from data.data_common.data_transfer_objects.company_dto import CompanyDTO

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.events.genie_consumer import GenieConsumer
from ai.langsmith.langsmith_loader import Langsmith

from data.data_common.utils.str_utils import get_uuid4

from data.data_common.data_transfer_objects.person_dto import PersonDTO

from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.dependencies.dependencies import (
    companies_repository,
    persons_repository,
)
from common.genie_logger import GenieLogger

logger = GenieLogger()
load_dotenv()


API_KEY = env_utils.get("HUNTER_API_KEY")

CONSUMER_GROUP_HUNTER_DOMAIN = "hunter_domain_consumer_group"


class HunterDomainConsumer(GenieConsumer):
    def __init__(self):
        super().__init__(
            topics=[Topic.NEW_EMAIL_TO_PROCESS_DOMAIN],
            consumer_group=CONSUMER_GROUP_HUNTER_DOMAIN,
        )
        self.company_repository = companies_repository()
        self.langsmith = Langsmith()
        self.person_repository = persons_repository()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        logger.info(f"Processing event on topic {event.properties.get(b'topic').decode('utf-8')}")
        topic = event.properties.get(b"topic").decode("utf-8")

        match topic:
            case Topic.NEW_EMAIL_TO_PROCESS_DOMAIN:
                logger.info("Handling new email to process domain")
                await self.handle_company_from_domain(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_company_from_domain(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {event_body}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        logger.info(f"Event body: {event_body}, type: {type(event_body)}")
        email_address = event_body.get("email")
        email_domain = get_domain_from_email(email_address)
        company = self.company_repository.get_company_from_domain(email_domain)
        if not company:
            response = await get_domain_info(str(email_address))
            if response.get("status") == "error":
                logger.info(f"Error: {response}")
                self.send_fail_event(email_address)
                return
            logger.info(f"Response: {response}")
            company = CompanyDTO.from_hunter_object(response["data"])
            self.company_repository.save_company_without_news(company)
        logger.info(f"Company: {company}")

        if not company.overview or not company.challenges:
            response = self.langsmith.run_prompt_company_overview_challenges(
                {"company_data": company.to_dict()}
            )
            logger.info(f"Response: {response}")
            overview = response.get("company_overview")
            challenges = response.get("challenges")
            logger.info(f"Overview: {overview}, Challenges: {challenges}")
            company.overview = overview
            company.challenges = challenges
            self.company_repository.save_company_without_news(company)

        event = GenieEvent(
            topic=Topic.NEW_COMPANY_DATA,
            data={"company_uuid": company.uuid, "company_name": company.name},
            scope="public",
        )
        event.send()

        return company, email_address


def get_domain_from_email(email: str) -> str:
    logger.debug(f"Email: {email}")
    return email.split("@")[1]


async def get_domain_info(email_address: str):
    """
    Get domain information from Hunter API
        Args:
            email_address (str): The email address to get domain information for.
        Returns:
            dict: The domain information.
    """
    domain = get_domain_from_email(email_address)
    response = requests.get(f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={API_KEY}")

    logger.info(f"Response: {response}")
    data = response.json()
    logger.info(f"Hunter data: {data}")

    return data


def find_employee_by_email(email: str, company: CompanyDTO):
    logger.info(f"Email: {email}, Company: {company}")
    if company.employees:
        for employee in company.employees:
            if employee.get("email") == email:
                return employee
    return None


if __name__ == "__main__":
    hunter_domain_consumer = HunterDomainConsumer()
    try:
        asyncio.run(hunter_domain_consumer.main())
    except KeyboardInterrupt:
        logger.info("Shutting down consumer")
        hunter_domain_consumer.close()
        sys.exit(0)
