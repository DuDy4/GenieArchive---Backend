import sys
import os

from common.utils import email_utils
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.genie_logger import GenieLogger

from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.pdl_consumer import PDLConsumer
from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.repositories.personal_data_repository import PersonalDataRepository
from data.data_common.dependencies.dependencies import (
    get_db_connection,
    persons_repository,
    personal_data_repository,
    companies_repository,
)

logger = GenieLogger()

persons_repository = persons_repository()
personal_data_repository = personal_data_repository()
companies_repository = companies_repository()


def get_all_persons_without_linkedin_url():
    all_persons = persons_repository.get_all_persons_without_linkedin_url()
    return all_persons


def create_person_from_pdl_personal_data(person: PersonDTO):
    row_dict = personal_data_repository.get_personal_data_row(person.uuid)
    if not row_dict or row_dict.get("pdl_status") == personal_data_repository.TRIED_BUT_FAILED:
        return None
    personal_data = row_dict.get("pdl_personal_data")
    if not personal_data:
        logger.error(f"Personal data not found for {person.uuid}")
        return None
    logger.info(f"Personal data: {personal_data}")
    personal_experience = personal_data.get("experience")
    position = ""
    company = ""
    linkedin_url = row_dict.get("linkedin_url")
    if not linkedin_url:
        linkedin_url = personal_data.get("linkedin_url")
        if not linkedin_url:
            profiles = personal_data.get("profiles")
            for profile in profiles:
                if profile.get("network") == "linkedin":
                    linkedin_url = profile.get("url")
                    break
    logger.debug(f"Linkedin URL: {linkedin_url}")
    if personal_experience and isinstance(personal_experience, list):
        personal_experience = personal_experience[0]

    if personal_experience and isinstance(personal_experience, dict):
        title_object = personal_experience.get("title")
        if title_object and isinstance(title_object, dict):
            position = title_object.get("name")
        company_object = personal_experience.get("company")
        if company_object and isinstance(company_object, dict):
            company = company_object.get("name")

    person_name = row_dict.get("name", "") or personal_data.get("full_name")
    logger.info(
        f"Position: {position}, Company: {company}, Person Name: {person_name}, Person Email: {person_email}"
    )

    person = PersonDTO(
        uuid=person.uuid,
        name=person.name or person_name,
        company=person.company or company,
        email=person.email,
        linkedin=person.linkedin or linkedin_url,
        position=person.position or position,
        timezone="",
    )
    company_domain = (
        person.email.split("@")[1] if isinstance(person.email, str) and "@" in person.email else ""
    )
    if company_domain:
        company = companies_repository.get_company_from_domain(company_domain)
        if company:
            person.company = company.name
    logger.info(f"Person: {person}")
    return person


def create_person_from_apollo_personal_data(person: PersonDTO):
    row_dict = personal_data_repository.get_personal_data_row(person.uuid)
    if not row_dict or row_dict.get("apollo_status") == personal_data_repository.TRIED_BUT_FAILED:
        return None
    personal_data = row_dict.get("apollo_personal_data")
    if not personal_data:
        logger.error(f"Personal data not found for {person.uuid}")
        return None
    logger.info(f"Personal data: {personal_data}")
    personal_experience = personal_data.get("employment_history")
    position = personal_data.get("title", "")
    company = personal_data.get("organization", "")
    if company:
        company = company.get("name")
    linkedin_url = row_dict.get("linkedin_url")
    if not linkedin_url:
        linkedin_url = personal_data.get("linkedin_url")
        if not linkedin_url:
            logger.error(f"LinkedIn URL not found for {person.uuid}")
            return None
    logger.debug(f"Linkedin URL: {linkedin_url}")
    if personal_experience and isinstance(personal_experience, list):
        personal_experience = personal_experience[0]
    if not position:
        position = personal_experience.get("title")
    if not company:
        company = personal_experience.get("organization_name")
    person_name = personal_data.get("name", "") or personal_data.get("first_name") + " " + personal_data.get(
        "last_name"
    )
    logger.info(
        f"Position: {position}, Company: {company}, Person Name: {person_name}, Person Email: {person.email}"
    )

    person = PersonDTO(
        uuid=person.uuid,
        name=person.name if (person.name and person.name != " ") else person_name,
        company=person.company if (person.company and person.company != " ") else company,
        email=person.email,
        linkedin=person.linkedin if (person.linkedin and person.linkedin != " ") else linkedin_url,
        position=person.position if (person.position and person.position != " ") else position,
        timezone="",
    )
    company_domain = (
        person.email.split("@")[1] if isinstance(person.email, str) and "@" in person.email else ""
    )
    if company_domain:
        company = companies_repository.get_company_from_domain(company_domain)
        if company:
            person.company = company.name
    logger.info(f"Person: {person}")
    return person


def update_persons_with_linkedin_url(persons: list[PersonDTO]):
    new_persons = []
    for person in persons:
        new_person = create_person_from_pdl_personal_data(person)
        logger.info(f"Person from pdl: {new_person}")
        if not new_person:
            new_person = create_person_from_apollo_personal_data(person)
            logger.info(f"Person from apollo: {new_person}")
        if new_person:
            new_persons.append(new_person)
            persons_repository.save_person(new_person)
            personal_data_repository.update_name_in_personal_data(new_person.uuid, new_person.name)
            personal_data_repository.update_linkedin_url(new_person.uuid, new_person.linkedin)
            logger.info(f"Updated LinkedIn URL for {new_person.email} to {new_person.linkedin}")
    return new_persons


persons = get_all_persons_without_linkedin_url()
logger.info(f"Persons without LinkedIn URL: {persons}")
persons = update_persons_with_linkedin_url(persons)
logger.info(f"Persons without LinkedIn URL: {persons}")
