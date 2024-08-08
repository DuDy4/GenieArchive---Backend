import os
import sys
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.repositories.companies_repository import CompaniesRepository
from data.data_common.data_transfer_objects.company_dto import CompanyDTO, NewsData
from data.data_common.dependencies.dependencies import (
    get_db_connection,
)

companies_repository = CompaniesRepository(conn=get_db_connection())

companies_repository.create_table_if_not_exists()

news = [
    {
        "date": "2024-07-27",
        "link": "https://news.com",
        "media": "News",
        "title": "GenieAI raises $1M",
        "summary": "GenieAI raises $1M in seed funding.",
    },
    {
        "date": "2024-07-28",
        "link": "https://news2.com",
        "media": "News2",
        "title": "GenieAI raises $2M",
        "summary": "GenieAI raises $2M in seed funding.",
    },
]

news = [NewsData.from_dict(new_data) for new_data in news]

company = CompanyDTO(
    uuid="e27be55a-d2fd-4f85-bb37-d7d65f09d8a1",
    name="GenieAI-MOCK",
    domain="genieai.ai",
    size="11-50",
    description="Genie is an AI company that provides AI solutions for businesses.",
    overview="GenieAI is a company that provides AI solutions for businesses.",
    challenges=[
        {"title": "Challenge 1", "description": "Challenge 1 description"},
        {"title": "Challenge 2", "description": "Challenge 2 description"},
    ],
    technologies=["AI", "ML", "NLP"],
    employees=[],
)


companies_repository.save_company(company)

assert companies_repository.exists(company.uuid)
logger.info("Companies save test passed")


company = companies_repository.get_company(company.uuid)

assert company == company
logger.info("Companies get test passed")

companies_repository.save_news(company.uuid, news)
news_in_database = companies_repository.get_news(company.uuid)
logger.debug(f"News in database: {news_in_database}")
assert news_in_database == news
logger.info("Companies save news test passed")

wrong_news = [
    {
        "date": "2024-07-27",
        "link": "https://news.com",
        "title": "GenieAI raises $1M",
        "summary": "GenieAI raises $1M in seed funding.",
    },
    {
        "date": "2024-07-28",
        "media": "News2",
        "title": "GenieAI raises $2M",
        "summary": "GenieAI raises $2M in seed funding.",
    },
    {
        "link": "https://news3.com",
        "media": "News3",
        "title": "GenieAI raises $3M",
        "summary": "GenieAI raises $3M in seed funding.",
    },
    {
        "date": "2024-07-29",
        "link": "https://news3.com",
        "media": "News3",
        "summary": "GenieAI raises $3M in seed funding.",
    },
    {
        "date": "2024-07-29",
        "link": "https://news3.com",
        "media": "News3",
        "title": "GenieAI raises $3M",
    },
]
try:
    companies_repository.save_news(company.uuid, wrong_news)
except Exception as e:
    logger.error(f"Error: {e}")
    logger.info("Companies save news test passed")
finally:
    news_in_database = companies_repository.get_news(company.uuid)
    logger.debug(f"News: {news}")
    assert news == news_in_database
    logger.info("Companies get news test passed")


companies_repository.delete_company(company.uuid)

assert not companies_repository.exists(company.uuid)
logger.info("Companies delete test passed")
