import asyncio
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from common.genie_logger import GenieLogger

from data.data_common.data_transfer_objects.company_dto import CompanyDTO
from data.data_common.dependencies.dependencies import companies_repository
from data.company_consumer import CompanyConsumer
from data.api_services.apollo import ApolloClient

logger = GenieLogger()

company_consumer = CompanyConsumer()

companies_repository = companies_repository()
apollo_client = ApolloClient()


def get_all_companies_with_missing_attributes():
    all_companies = companies_repository.get_all_companies_without_attributes()
    company = companies_repository.get_company_from_domain("@")
    return [company]
    # return all_companies


async def update_companies_with_missing_attributes(companies: list[CompanyDTO]):
    new_companies = []
    tasks = []
    for company in companies[:5]:
        if not company.domain:
            continue
        tasks.append(apollo_client.enrich_company(company.domain))

    results = await asyncio.gather(*tasks)

    for i, new_company in enumerate(results):
        if not new_company:
            logger.warning(f"Failed to get data for company {companies[i].domain}")
            continue
        company = companies[i]
        new_company = new_company.get("organization", new_company)
        new_company["uuid"] = company.uuid
        logger.info(f"Data from apollo: {new_company}")
        company_dto = CompanyDTO.from_apollo_object(new_company)
        logger.info(f"Company from apollo: {company_dto}")
        if company_dto.domain != company.domain:
            logger.error(f"Domain mismatch: {company_dto.domain} != {company_dto.domain}")
            company_dto.domain = company.domain
        if company_dto.size == "0" or company_dto.size == 0:
            logger.error(f"Invalid company size: {company_dto.size}")
        company_dto = await company_consumer.fix_company_description(company_dto)
        companies_repository.save_company_without_news(company_dto)

        new_companies.append(company_dto)
    return new_companies


# company_data = asyncio.run(apollo_client.enrich_company("fiverr.com"))
# logger.info(f"Company data fetched from Apollo: {company_data}")

async def check_and_fix_descriptions():
    companies = companies_repository.get_all_companies()
    for company in companies:
        if company.description and len(company.description) > 300:
            logger.info(f"Company description too long: {company.name}: {len(company.description)}")
            company.description = await company_consumer.langsmith.get_summary(company.description)
            logger.info(f"Updated description: {len(company.description)}")
            companies_repository.save_company_without_news(company)

# if __name__ == "__main__":
#     asyncio.run(check_and_fix_descriptions())