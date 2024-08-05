import json
import sys
import os
import requests
import asyncio
from dotenv import load_dotenv
from loguru import logger

from data.data_common.data_transfer_objects.company_dto import CompanyDTO

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.events.genie_consumer import GenieConsumer
from ai.langsmith.langsmith_loader import Langsmith

from data.data_common.utils.str_utils import get_uuid4

from data.data_common.data_transfer_objects.person_dto import PersonDTO

from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.dependencies.dependencies import companies_repository

load_dotenv()

PERSON_PORT = os.environ.get("PERSON_PORT", 8000)
API_KEY = os.environ.get("HUNTER_API_KEY")

CONSUMER_GROUP_HUNTER_DOMAIN = "hunter_domain_consumer_group"


class HunterDomainConsumer(GenieConsumer):
    def __init__(self):
        super().__init__(
            topics=[Topic.FAILED_TO_ENRICH_EMAIL, Topic.NEW_EMAIL_TO_PROCESS_DOMAIN],
            consumer_group=CONSUMER_GROUP_HUNTER_DOMAIN,
        )
        self.company_repository = companies_repository()
        self.langsmith = Langsmith()

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
            case Topic.NEW_EMAIL_TO_PROCESS_DOMAIN:
                logger.info("Handling new email to process domain")
                await self.handle_company_from_domain(event)

    async def handle_failed_to_enrich_email(self, event):
        company, email_address = await self.handle_company_from_domain(event)
        employee = find_employee_by_email(email_address, company)
        if not employee:
            logger.info(f"Employee not found for email: {email_address}")
            self.send_fail_event(email_address, company)
            return
        if employee.get("name") and "None " not in employee.get("name"):
            person = PersonDTO(
                uuid=get_uuid4(),
                name=employee.get("last_name"),
                email=employee.get("email"),
                position=employee.get("position"),
                company=company.name,
                linkedin=employee.get("linkedin"),
                timezone="",
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

    def send_fail_event(self, email_address: str, company: CompanyDTO = None):
        event = GenieEvent(
            topic=Topic.FAILED_TO_GET_DOMAIN_INFO,
            data={"email": email_address, "company": company.to_dict()},
            scope="public",
        )
        event.send()

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
            self.company_repository.save_company(company)
        logger.info(f"Company: {company}")
        # event = GenieEvent(
        #     topic=Topic.NEW_COMPANY_DATA,
        #     data=company.to_dict(),
        #     scope="public",
        # )
        # event.send()
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
            self.company_repository.save_company(company)

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

    logger.info(f"Domain: {domain}, API Key: {API_KEY}")

    response = requests.get(
        f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={API_KEY}"
    )

    logger.info(f"Response: {response}")
    data = response.json()
    logger.info(f"Data: {data}")

    return data


def find_employee_by_email(email: str, company: CompanyDTO):
    logger.info(f"Email: {email}, Company: {company}")
    if company.employees:
        for employee in company.employees:
            if employee.get("email") == email:
                return employee
    return None


#
# data = asyncio.run(get_domain_info("alon@hanacovc.com"))
# print(data)
