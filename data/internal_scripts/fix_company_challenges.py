import asyncio
import sys
import os
from typing import List

from data.data_common.data_transfer_objects.company_dto import CompanyDTO
from ai.langsmith.langsmith_loader import Langsmith
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from common.genie_logger import GenieLogger

from data.data_common.dependencies.dependencies import (
    companies_repository,
)

logger = GenieLogger()

companies_repository = companies_repository()
langsmith = Langsmith()


async def fix_company_challenges(limit: int = 1):
    all_companies_without_challenges = companies_repository.get_all_companies_without_challenges()
    logger.info(f"companies without challenges: {len(all_companies_without_challenges)}")
    for company in all_companies_without_challenges[:limit]:
        logger.info(f"company name: {company.name}, company challenges: {company.challenges}")
        if company.news:
            # logger.info(f"Company news: {company.news}")
            company.challenges = None
            updated_challenges = await langsmith.get_company_challenges_with_news(company)
            logger.info(f"Updated challenges: {updated_challenges}")
            company.challenges = updated_challenges
            companies_repository.save_company_without_news(company)
        else:
            response = langsmith.run_prompt_company_overview_challenges({"company_data": company.to_dict()})
            logger.info(f"Response: {response}")
            overview = response.get("company_overview")
            challenges = response.get("challenges")
            logger.info(f"Overview: {overview}, Challenges: {challenges}")
            company.overview = overview
            company.challenges = challenges
            companies_repository.save_company_without_news(company)
            event = GenieEvent(topic=Topic.NEW_COMPANY_DATA, data={"company_uuid": company.uuid})
            event.send()


asyncio.run(fix_company_challenges(100))
