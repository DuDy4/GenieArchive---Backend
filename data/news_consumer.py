import asyncio
import json
import os
import sys
import traceback

from loguru import logger

from data.data_common.data_transfer_objects.company_dto import CompanyDTO

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.news_scrapper.news_scraper import NewsScrapper

from data.data_common.utils.str_utils import get_uuid4

from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.dependencies.dependencies import companies_repository

CONSUMER_GROUP = "news_consumer_group"


class NewsConsumer(GenieConsumer):
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
        self.news_scrapper = NewsScrapper()

    async def process_event(self, event):
        logger.info(f"News consumer processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        match topic:
            case Topic.NEW_COMPANY_DATA:
                logger.info("Handling failed attempt to get linkedin url")
                await self.handle_new_company_data(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_new_company_data(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        logger.info(f"Event body: {str(event_body)[:300]}")
        company_uuid = event_body.get("company_uuid")
        logger.info(f"Company UUID: {company_uuid}")
        company = self.compannies_repository.get_company(company_uuid)
        if isinstance(company, tuple):
            company = CompanyDTO.from_tuple(company)
        if isinstance(company, dict):
            company = CompanyDTO.from_dict(company)
        logger.info(f"Company: {str(company)[:300]}")

        if not company:
            logger.error(f"Company not found for UUID: {company_uuid}")
            return {"error": "Company not found"}

        # If already has news, check if it's outdated. if not - skip news fetching
        if company.news:
            logger.info(f"Company already has news: {company.news}. Checking if it's outdated")
            outdated = await self.news_scrapper.is_news_outdated(company.news)
            if not outdated:
                logger.info(f"News is not outdated. Skipping news update")
                event = GenieEvent(
                    topic=Topic.COMPANY_NEWS_UP_TO_DATE,
                    data={"company_uuid": company.uuid},
                    scope="public",
                )
                event.send()
                return
            logger.info(f"News is outdated. Updating news")
        news = await self.news_scrapper.get_news(company.name)
        logger.info(f"Got news for company: {company.name}. News: {news}")

        if news and news.get("error"):
            logger.info(f"No news found for company: {company.name}")
            event = GenieEvent(
                topic=Topic.FAILED_TO_GET_COMPANY_NEWS,
                data={"company_uuid": company.uuid},
                scope="public",
            )
            event.send()
            logger.info(f"Sent event for failed news update: {company.name}")
            return
        else:
            # If news is a dict, extract the news list
            if isinstance(news, dict):
                if news.get("news"):
                    news = news.get("news")
            self.companies_repository.save_news(company.uuid, news)
            company.news = news
            logger.info(f"Saved news for company: {company.name}. News: {news}")
            event = GenieEvent(
                topic=Topic.COMPANY_NEWS_UPDATED,
                data={"company_uuid": company.uuid},
                scope="public",
            )
            event.send()
            logger.info(f"Sent event for company news update: {company.name}")


if __name__ == "__main__":
    news_consumer = NewsConsumer()
    try:
        asyncio.run(news_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
