import sys
import os
from typing import List

from data.data_common.data_transfer_objects.company_dto import CompanyDTO
from ai.langsmith.langsmith_loader import Langsmith

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from common.genie_logger import GenieLogger

from data.data_common.dependencies.dependencies import (
    get_db_connection,
    companies_repository,
)

from data.data_common.utils.persons_utils import (
    create_person_from_pdl_personal_data,
    create_person_from_apollo_personal_data,
)

logger = GenieLogger()

companies_repository = companies_repository()
langsmith = Langsmith()


def get_all_companies_without_challenges():
    all_companies_without_challenges = companies_repository.get_all_companies_without_challenges()
    return all_companies_without_challenges


def fix_company_challenges(companies: List[CompanyDTO]):
    logger.info(f"companies without challenges: {len(companies)}")
    for company in companies[:10]:
        logger.info(f"company name: {company.name}, company challenges: {company.challenges}")
        if not company.challenges:
            response = langsmith.run_prompt_company_overview_challenges({"company_data": company.to_dict()})
            logger.info(f"Response: {response}")
            overview = response.get("company_overview")
            challenges = response.get("challenges")
            logger.info(f"Overview: {overview}, Challenges: {challenges}")
            company.overview = overview
            company.challenges = challenges
            companies_repository.save_company_without_news(company)


companies_without = get_all_companies_without_challenges()
logger.info(f"companies without challenges: {len(companies_without)}")
# fix_company_challenges(companies_without)
