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
from data.data_common.dependencies.dependencies import companies_repository
from common.genie_logger import GenieLogger

logger = GenieLogger()
load_dotenv()


API_KEY = env_utils.get("HUNTER_API_KEY")


class HunterClient:
    def __init__(self):
        self.company_repository = companies_repository()

    async def handle_company_from_domain(self, email_domain: str):
        response = await self.get_domain_info(email_domain)
        if response.get("status") == "error":
            logger.info(f"Error: {response}")
            return None
        logger.info(f"Response: {response}")
        company = CompanyDTO.from_hunter_object(response["data"])
        self.company_repository.save_company_without_news(company)
        logger.info(f"Company: {company}")
        return company

    @staticmethod
    async def get_domain_info(domain: str):
        """
        Get domain information from Hunter API
            Args:
                domain (str): The domain to search for.
            Returns:
                dict: The domain information.
        """
        response = requests.get(f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={API_KEY}")

        logger.info(f"Hunter response: {response}")
        data = response.json()
        logger.info(f"Hunter data: {data}")

        return data
