import asyncio

import json
import os
import sys
import traceback


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from datetime import datetime
from data.api_services.tavily_client import Tavily
from data.api_services.apollo import ApolloClient
from data.api_services.hunter import HunterClient
from ai.langsmith.langsmith_loader import Langsmith
from dotenv import load_dotenv

from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.data_transfer_objects.company_dto import CompanyDTO

from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.dependencies.dependencies import companies_repository
from common.genie_logger import GenieLogger

load_dotenv()


logger = GenieLogger()
CONSUMER_GROUP = "company_consumer_group"
COMPANY_LAST_UPDATE_INTERVAL_SECONDS = 30 * 24 * 60 * 60  # 30 Days


class CompanyConsumer(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[Topic.NEW_COMPANY_DATA, Topic.NEW_EMAIL_TO_PROCESS_DOMAIN],
            consumer_group=CONSUMER_GROUP,
        )
        self.apollo_client: ApolloClient = ApolloClient()
        self.hunter_client: HunterClient = HunterClient()
        self.langsmith = Langsmith()
        self.tavily_client = Tavily()
        self.companies_repository: CompaniesRepository = companies_repository()

    async def process_event(self, event):
        logger.info(f"Company consumer processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Company consumer - Processing event on topic {topic}")
        match topic:
            case Topic.NEW_COMPANY_DATA:
                logger.info("Handling news fething for company")
                await self.handle_fetch_company_news(event)
            case Topic.NEW_EMAIL_TO_PROCESS_DOMAIN:
                logger.info("Handling new email to process domain")
                await self.handle_company_from_domain(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_fetch_company_news(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        company_uuid = event_body.get("company_uuid")
        if not company_uuid:
            logger.error(f"Company uuid not found in event: {event_body}")
            return
        company_dto = self.companies_repository.get_company(company_uuid)
        logger.info(f"Company: {company_dto}")
        if not company_dto:
            logger.error(f"Company not found for uuid: {company_uuid}")
            return
        news_last_update = self.companies_repository.get_news_last_updated(company_uuid)
        if (
            news_last_update
            and (datetime.now() - news_last_update).total_seconds() < COMPANY_LAST_UPDATE_INTERVAL_SECONDS
        ):
            logger.info(f"Company news for {company_dto.name} is up to date")
            return {"status": "success"}
        logger.info(f"Fetching news for company {company_dto.name}")
        news = self.fetched_news(company_dto.uuid, company_dto.name)
        company_dto.challenges = None
        company_dto.news = news
        updated_challenges = await self.langsmith.get_company_challenges_with_news(company_dto)
        logger.info(f"Updated challenges: {updated_challenges}")
        company_dto.challenges = updated_challenges
        self.companies_repository.save_company_without_news(company_dto)
        event = GenieEvent(topic=Topic.COMPANY_NEWS_UPDATED, data={"company_uuid": company_dto.uuid})
        event.send()

        return {"status": "success"}

    async def handle_company_from_domain(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        email_address = event_body.get("email")
        if not email_address:
            logger.error(f"Email address not found in event: {event_body}")
            return
        email_domain = email_address.split("@")[-1]
        company = self.companies_repository.get_company_from_domain(email_domain)
        if not company:
            logger.info(f"Company not found in database for domain: {email_domain}. Fetching company data")
            company = await self.fetch_company_data(email_domain)
            if not company:
                logger.error(f"Company not found for domain: {email_domain}")
                event = GenieEvent(topic=Topic.FAILED_TO_GET_COMPANY_DATA, data={"email": email_address})
                event.send()
                return
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
            self.companies_repository.save_company_without_news(company)
        news_last_update = self.companies_repository.get_news_last_updated(company.uuid)
        if (
            news_last_update
            and (datetime.now() - news_last_update).total_seconds() < COMPANY_LAST_UPDATE_INTERVAL_SECONDS
        ):
            logger.info(f"Company news for {company.name} is up to date")
            event = GenieEvent(topic=Topic.COMPANY_NEWS_UP_TO_DATE, data={"company_uuid": company.uuid})
            event.send()
            return {"status": "success"}
        logger.info(f"Fetching news for company {company.name}")
        self.fetched_news(company.uuid, company.name)
        event = GenieEvent(topic=Topic.COMPANY_NEWS_UPDATED, data={"company_uuid": company.uuid})
        event.send()
        return {"status": "success"}

    async def fetch_company_data(self, email_domain):
        company_data = await self.apollo_client.enrich_company(email_domain)
        if company_data:
            company = CompanyDTO.from_apollo_object(company_data.get("organization") or company_data)
            company = self.validate_company_data(company, email_domain)
            company = self.fix_company_description(company)
            logger.info(f"Company data fetched from Apollo: {str(company)[:300]}")
            self.companies_repository.save_company_without_news(company)
            return company
        logger.warning(f"Apollo couldn't find company data for domain: {email_domain}")
        company_data = await self.hunter_client.get_domain_info(email_domain)
        company = CompanyDTO.from_hunter_object(company_data["data"])
        company = self.validate_company_data(company, email_domain)
        company = self.fix_company_description(company)
        logger.info(f"Company data fetched from Hunter: {str(company)[:300]}")
        self.companies_repository.save_company_without_news(company)
        return company

    def fetched_news(self, company_uuid, company_name):
        news_list = self.tavily_client.get_news(company_name)
        if news_list and len(news_list) > 0:
            logger.info(f"Fetched {len(news_list)} news for company {company_name}")
            self.companies_repository.save_news(company_uuid, news_list)
            logger.info(f"Company news for {company_name} updated")
            return news_list
        else:
            logger.info(f"No news found for company {company_name}")

    def validate_company_data(self, company_dto: CompanyDTO, email_domain: str):
        if company_dto.domain != email_domain:
            logger.error(f"Domain mismatch: {company_dto.domain} != {email_domain}")
            company_dto.domain = email_domain
        if company_dto.size == "0" or company_dto.size == 0:
            logger.error(f"Invalid company size: {company_dto.size}")
            company_dto.size = None
        return company_dto
    
    def fix_company_description(self, company_dto: CompanyDTO):
        description = company_dto.description
        if not description or len(description) > 300:
            description_summary = self.langsmith.get_summary(description)
            if description_summary:
                company_dto.description = description_summary
        return company_dto


if __name__ == "__main__":
    company_consumer = CompanyConsumer()
    try:
        asyncio.run(company_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
