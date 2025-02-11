import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from common.genie_logger import GenieLogger
from data.api.api_services_classes.admin_api_services import AdminApiService
from data.data_common.services.person_builder_service import (
    get_company_and_position_from_pdl_experience,
    get_company_and_position_from_apollo_personal_data,
)
from data.data_common.dependencies.dependencies import (
    profiles_repository,
    persons_repository,
    companies_repository,
    personal_data_repository,
)

admin_api_service = AdminApiService()

logger = GenieLogger()

profiles_repository = profiles_repository()
persons_repository = persons_repository()
companies_repository = companies_repository()
personal_data_repository = personal_data_repository()


def get_all_persons_with_personal_data_but_no_profile():
    profiles_uuid = profiles_repository.get_missing_profiles()
    logger.info(f"Found {len(profiles_uuid)} profiles without profile")
    for profile_uuid in profiles_uuid:
        logger.info(f"Syncing profile for {profile_uuid}")
        result = admin_api_service.sync_profile(profile_uuid)
        if result.get("error"):
            logger.error(f"Error syncing profile: {result}")
            email = persons_repository.get_person(profile_uuid).email
            result = admin_api_service.sync_email(profile_uuid)
            if result.get("error"):
                logger.error(f"Error syncing email: {result}")
            else:
                logger.info(f"Email synced for {email}")


def fix_company_name_in_profiles():
    profiles = profiles_repository.get_all_profiles_without_company_name()
    logger.info(f"Found {len(profiles)} profiles without company name")
    for profile in profiles:
        logger.info(f"Starting to fix company name for profile {profile.uuid}")
        if profile.company:
            logger.info(f"Profile {profile.uuid} already has company name: {profile.company}")
            continue
        person = persons_repository.get_person(profile.uuid)
        if not person:
            logger.error(f"Person with uuid {profile.uuid} not found")
            continue
        if person.company:
            logger.info(f"Person {person.email} has company name: {person.company}")
            profile.company = person.company
            profiles_repository.save_profile(profile)
            continue

        company_domain = person.email.split("@")[-1] if "@" in person.email else None
        if company_domain:
            company_from_domain = companies_repository.get_company_from_domain(company_domain)
            if company_from_domain:
                logger.info(f"Company found from domain: {company_from_domain.name}")
                profile.company = company_from_domain.name
                profiles_repository.save_profile(profile)
                continue
        logger.error(f"Could not find company for {person.email}")
        pdl_personal_data = personal_data_repository.get_pdl_personal_data(person.uuid)
        if pdl_personal_data:
            company, position = get_company_and_position_from_pdl_experience(
                pdl_personal_data.get("experience")
            )
            if company:
                logger.info(f"Company found from PDL: {company}")
                profile.company = company
                if not profile.position:
                    profile.position = position
                profiles_repository.save_profile(profile)
                continue
        logger.error(f"Could not find company from PDL for {person.email}")
        apollo_personal_data = personal_data_repository.get_apollo_personal_data(person.uuid)
        if apollo_personal_data:
            company, position = get_company_and_position_from_apollo_personal_data(apollo_personal_data)
            if company:
                logger.info(f"Company found from Apollo: {company}")
                profile.company = company
                if not profile.position:
                    profile.position = position
                profiles_repository.save_profile(profile)
                continue
        logger.error(f"Could not find company from any resource for {person.email}")


# fix_company_name_in_profiles()
