import json
import sys
import os
import requests
import asyncio
from dotenv import load_dotenv
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.events.genie_consumer import GenieConsumer

from data.data_common.utils.str_utils import get_uuid4

from data.data_common.data_transfer_objects.person_dto import PersonDTO

from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.dependencies.dependencies import companies_repository

load_dotenv()

PERSON_PORT = os.environ.get("PERSON_PORT", 8000)
API_KEY = os.environ.get("HUNTER_API_KEY")

CONSUMER_GROUP_HUNTER_DOMAIN = "hunter_domain_consumer_group" + os.environ.get(
    "CONSUMER_GROUP_NAME", ""
)


class HunterDomainConsumer(GenieConsumer):
    def __init__(self):
        super().__init__(
            topics=[Topic.FAILED_TO_ENRICH_EMAIL],
            consumer_group=CONSUMER_GROUP_HUNTER_DOMAIN,
        )
        self.company_repository = companies_repository()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        logger.info(
            f"Processing event on topic {event.properties.get(b'topic').decode('utf-8')}"
        )
        topic = event.properties.get(b"topic").decode("utf-8")

        match topic:
            case Topic.FAILED_TO_ENRICH_EMAIL:
                logger.info("Handling failed attempt to enrich email")
                await self.handle_failed_to_enrich_email(event)

    async def handle_failed_to_enrich_email(self, event):
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
            company = response["data"]
        if not company.get("emails") and company.get("employees"):
            company["emails"] = company.get("employees")
        self.company_repository.save_company(company)
        employee = find_employee_by_email(email_address, company)
        if not employee:
            logger.info(f"Employee not found for email: {email_address}")
            self.send_fail_event(email_address, company)
            return
        if employee.get("name"):
            person = PersonDTO(
                uuid=get_uuid4(),
                name=employee.get("last_name"),
                email=employee.get("email"),
                position=employee.get("position"),
                company=company["organization"],
                linkedin=employee.get("linkedin"),
                timezone=company.get("timezone", ""),
            )
        elif employee.get("first_name"):
            person = PersonDTO.from_hunter_employee(employee, company["organization"])
        else:
            logger.error(f"Employee not found for email: {email_address}")
            return
        person_event = GenieEvent(
            topic=Topic.NEW_PERSON, data=person.to_json(), scope="public"
        )
        person_event.send()
        return {"status": "success"}

    def send_fail_event(self, email_address: str, company=None):
        event = GenieEvent(
            topic=Topic.FAILED_TO_GET_DOMAIN_INFO,
            data={"email": email_address, "company": company},
            scope="public",
        )
        event.send()


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

    logger.info(f"Domain: {domain}, API Key: {API_KEY}")

    response = requests.get(
        f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={API_KEY}"
    )

    logger.info(f"Response: {response}")
    data = response.json()
    logger.info(f"Data: {data}")

    return data


def find_employee_by_email(email: str, company: dict):
    logger.info(f"Email: {email}, Company: {company}")
    if company.get("employees"):
        for employee in company.get("employees"):
            if employee.get("email") == email:
                return employee
    if company.get("emails"):
        for employee in company.get("emails"):
            if employee.get("value") == email:
                return employee
    return None
