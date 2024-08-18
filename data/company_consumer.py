import asyncio

import json
import os
import sys
import traceback


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from datetime import datetime
from common.utils.json_utils import clean_json
from data.api_services.tavily_client import Tavily
from dotenv import load_dotenv
load_dotenv()
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.dependencies.dependencies import companies_repository
from common.genie_logger import GenieLogger


logger = GenieLogger()
CONSUMER_GROUP = "company_consumer_group"
COMPANY_LAST_UPDATE_INTERVAL_SECONDS = 30 * 24 * 60 * 60  # 30 Days

class CompanyConsumer(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[
                Topic.NEW_COMPANY_DATA,
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.companies_repository: CompaniesRepository = companies_repository()

    async def process_event(self, event):
        logger.info(f"Company consumer processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Company consumer - Processing event on topic {topic}")
        match topic:
            case Topic.NEW_COMPANY_DATA:
                logger.info("Handling news fething for company")
                await self.handle_fetch_company_news(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_fetch_company_news(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        company_uuid = event_body.get("company_uuid")
        if company_uuid:
            compant_dto = self.companies_repository.get_company(company_uuid)
            if not compant_dto:
                logger.error(f"Company not found for uuid: {company_uuid}")
                return
            company_name = compant_dto.name
            news_last_update = self.companies_repository.get_news_last_updated(company_uuid)
            if news_last_update and (datetime.now() - news_last_update).total_seconds() < COMPANY_LAST_UPDATE_INTERVAL_SECONDS :
                logger.info(f"Company news for {company_name} is up to date")
                return {"status": "success"}
            tavily_client = Tavily()
            logger.info(f"Fetching new for company {company_name}")
            news_list = tavily_client.get_news(company_name)
            logger.info(f"Fetched {len(news_list)} news for company {company_name}")
            self.companies_repository.save_news(company_uuid, news_list)
            logger.info(f"Company news for {company_name} updated")
            return {"status": "success"}


if __name__ == "__main__":
    company_consumer = CompanyConsumer()
    try:
        asyncio.run(company_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
