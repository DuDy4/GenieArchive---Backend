from common.genie_logger import GenieLogger

from data.data_common.data_transfer_objects.person_dto import PersonDTO

from data.data_common.dependencies.dependencies import (
    persons_repository,
    personal_data_repository,
    companies_repository,
)

persons_repository = persons_repository()
personal_data_repository = personal_data_repository()
companies_repository = companies_repository()

logger = GenieLogger()


def fix_linkedin_url(linkedin_url: str) -> str:
    """
    Converts a full LinkedIn URL to a shortened URL.

    Args:
        linkedin_url (str): The full LinkedIn URL.

    Returns:
        str: The shortened URL.
    """

    if not linkedin_url:
        logger.error(f"Trying to fix Linkedin URL, but it is None or empty: {linkedin_url}")
        return ""

    linkedin_url = linkedin_url.replace("http://www.linkedin.com/in/", "linkedin.com/in/")
    linkedin_url = linkedin_url.replace("https://www.linkedin.com/in/", "linkedin.com/in/")
    linkedin_url = linkedin_url.replace("http://linkedin.com/in/", "linkedin.com/in/")
    linkedin_url = linkedin_url.replace("https://linkedin.com/in/", "linkedin.com/in/")

    if linkedin_url and linkedin_url[-1] == "/":
        linkedin_url = linkedin_url[:-1:]
    return linkedin_url


def get_company_name_from_domain(email: str):
    company_domain = email.split("@")[1] if isinstance(email, str) and "@" in email else None
    if company_domain:
        company = companies_repository.get_company_from_domain(company_domain)
        if company:
            return company.name
    return None


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
    company = get_company_name_from_domain(person.email)

    linkedin_url = row_dict.get("linkedin_url")
    if not linkedin_url:
        linkedin_url = personal_data.get("linkedin_url")
        if not linkedin_url:
            profiles = personal_data.get("profiles")
            for profile in profiles:
                if profile.get("network") == "linkedin":
                    linkedin_url = profile.get("url")
                    break
    linkedin_url = fix_linkedin_url(linkedin_url)
    logger.debug(f"Linkedin URL: {linkedin_url}")
    if personal_experience and isinstance(personal_experience, list):
        personal_experience = personal_experience[0]

    if personal_experience and isinstance(personal_experience, dict):
        title_object = personal_experience.get("title")
        if title_object and isinstance(title_object, dict):
            position = title_object.get("name")
        if not company:
            company_object = personal_experience.get("company")
            if company_object and isinstance(company_object, dict):
                company = company_object.get("name")

    person_name = row_dict.get("name", "") or personal_data.get("full_name")
    logger.info(
        f"Position: {position}, Company: {company}, Person Name: {person_name}, Person Email: {person.email}"
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
    company = get_company_name_from_domain(person.email)
    if not company:
        company = personal_data.get("organization", "")
        if company:
            company = company.get("name")
    linkedin_url = row_dict.get("linkedin_url")
    if not linkedin_url:
        linkedin_url = personal_data.get("linkedin_url")
        if not linkedin_url:
            logger.error(f"LinkedIn URL not found for {person.uuid}")
            return None
    linkedin_url = fix_linkedin_url(linkedin_url)
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
    logger.info(f"Person: {person}")
    return person
