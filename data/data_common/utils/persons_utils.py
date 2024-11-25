from common.genie_logger import GenieLogger

from data.data_common.data_transfer_objects.person_dto import PersonDTO

from data.data_common.data_transfer_objects.profile_dto import ProfileCategory, ProfileDTO, SalesCriteriaType, SalesCriteria
from data.data_common.dependencies.dependencies import (
    persons_repository,
    personal_data_repository,
    companies_repository,
)

persons_repository = persons_repository()
personal_data_repository = personal_data_repository()
companies_repository = companies_repository()

logger = GenieLogger()

profiles = ["The Analytical", "The Amiable", "The Driver", "The Expressive", "The Skeptic", "The Pragmatist", "The Curious"]


sales_criteria_mapping = {
    profiles[0]: [
        SalesCriteria(criteria=SalesCriteriaType.BUDGET, score=0, target_score=10), 
        SalesCriteria(criteria=SalesCriteriaType.TECHNICAL_FIT, score=0, target_score=30), 
        SalesCriteria(criteria=SalesCriteriaType.BUSINESS_FIT, score=0, target_score=30),
        SalesCriteria(criteria=SalesCriteriaType.VALUE_PROPOSITION, score=0, target_score=20),
        SalesCriteria(criteria=SalesCriteriaType.LONG_TERM_PROFESSIONAL_ADVISOR, score=0, target_score=10),],
    profiles[1]: [
        SalesCriteria(criteria=SalesCriteriaType.BUDGET, score=0, target_score=10), 
        SalesCriteria(criteria=SalesCriteriaType.TRUST, score=0, target_score=40), 
        SalesCriteria(criteria=SalesCriteriaType.BUSINESS_FIT, score=0, target_score=10),
        SalesCriteria(criteria=SalesCriteriaType.REPUTATION, score=0, target_score=10),
        SalesCriteria(criteria=SalesCriteriaType.LONG_TERM_PROFESSIONAL_ADVISOR, score=0, target_score=30),],
    profiles[2]: [
        SalesCriteria(criteria=SalesCriteriaType.BUDGET, score=0, target_score=15), 
        SalesCriteria(criteria=SalesCriteriaType.BUSINESS_FIT, score=0, target_score=30), 
        SalesCriteria(criteria=SalesCriteriaType.VALUE_PROPOSITION, score=0, target_score=30),
        SalesCriteria(criteria=SalesCriteriaType.INNOVATION, score=0, target_score=15),
        SalesCriteria(criteria=SalesCriteriaType.RESPONSIVENESS, score=0, target_score=10),],
    profiles[3]: [
        SalesCriteria(criteria=SalesCriteriaType.BUDGET, score=0, target_score=10), 
        SalesCriteria(criteria=SalesCriteriaType.BUSINESS_FIT, score=0, target_score=20), 
        SalesCriteria(criteria=SalesCriteriaType.VALUE_PROPOSITION, score=0, target_score=20),
        SalesCriteria(criteria=SalesCriteriaType.INNOVATION, score=0, target_score=40),
        SalesCriteria(criteria=SalesCriteriaType.LONG_TERM_PROFESSIONAL_ADVISOR, score=0, target_score=10),],
    profiles[4]: [
        SalesCriteria(criteria=SalesCriteriaType.BUDGET, score=0, target_score=15), 
        SalesCriteria(criteria=SalesCriteriaType.TECHNICAL_FIT, score=0, target_score=10), 
        SalesCriteria(criteria=SalesCriteriaType.BUSINESS_FIT, score=0, target_score=15),
        SalesCriteria(criteria=SalesCriteriaType.VALUE_PROPOSITION, score=0, target_score=20),
        SalesCriteria(criteria=SalesCriteriaType.REPUTATION, score=0, target_score=40),],
    profiles[5]: [
        SalesCriteria(criteria=SalesCriteriaType.BUDGET, score=0, target_score=20), 
        SalesCriteria(criteria=SalesCriteriaType.BUSINESS_FIT, score=0, target_score=40), 
        SalesCriteria(criteria=SalesCriteriaType.VALUE_PROPOSITION, score=0, target_score=20),
        SalesCriteria(criteria=SalesCriteriaType.REPUTATION, score=0, target_score=10),
        SalesCriteria(criteria=SalesCriteriaType.RESPONSIVENESS, score=0, target_score=10),],
    profiles[6]: [
        SalesCriteria(criteria=SalesCriteriaType.BUDGET, score=0, target_score=10), 
        SalesCriteria(criteria=SalesCriteriaType.TECHNICAL_FIT, score=0, target_score=20), 
        SalesCriteria(criteria=SalesCriteriaType.BUSINESS_FIT, score=0, target_score=25),
        SalesCriteria(criteria=SalesCriteriaType.INNOVATION, score=0, target_score=35),
        SalesCriteria(criteria=SalesCriteriaType.LONG_TERM_PROFESSIONAL_ADVISOR, score=0, target_score=10),],
}

strengths_mapping = {
    # "Achiever": [4, 6, 1, 3, 7, 2, 5],
    "Achiever": [7, 7, 7, 7, 7, 7, 7],
    "Activator": [5, 7, 2, 1, 6, 3, 4],
    "Adaptability": [6, 2, 5, 1, 7, 4, 3],
    "Analytical": [1, 10, 4, 6, 2, 5, 3],
    "Arranger": [5, 4, 3, 7, 1, 6, 2],
    "Belief": [7, 1, 6, 2, 4, 3, 5],
    "Command": [6, 3, 2, 5, 7, 1, 4],
    "Communication": [6, 1, 5, 2, 10, 4, 3],
    "Competition": [5, 7, 2, 4, 10, 1, 3],
    "Connectedness": [7, 5, 4, 1, 6, 3, 2],
    "Consistency": [2, 1, 3, 10, 6, 4, 5],
    "Context": [2, 10, 1, 10, 3, 5, 4],
    "Deliberative": [2, 6, 3, 7, 1, 4, 5],
    "Developer": [10, 1, 4, 2, 3, 10, 10],
    "Discipline": [4, 10, 2, 6, 5, 1, 3],
    "Empathy": [10, 1, 10, 2, 10, 10, 10],
    "Focus": [3, 10, 2, 10, 5, 1, 4],
    "Futuristic": [10, 3, 10, 1, 10, 6, 2],
    "Harmony": [10, 2, 10, 10, 1, 10, 10],
    "Ideation": [10, 10, 3, 1, 10, 10, 2],
    "Includer": [10, 1, 10, 10, 10, 2, 10],
    "Individualization": [3, 1, 2, 10, 10, 5, 4],
    "Input": [1, 7, 5, 3, 4, 6, 2],
    "Intellection": [4, 3, 5, 2, 7, 6, 1],
    "Learner": [3, 7, 4, 2, 5, 6, 1],
    "Maximizer": [4, 1, 3, 10, 10, 2, 5],
    "Positivity": [10, 1, 10, 2, 10, 10, 10],
    "Relator": [10, 1, 10, 10, 10, 10, 10],
    "Responsibility": [3, 6, 1, 10, 4, 2, 5],
    "Restorative": [5, 7, 3, 2, 4, 6, 1],
    "Self-Assurance": [3, 10, 2, 10, 7, 1, 10],
    "Significance": [6, 7, 4, 3, 5, 1, 2],
    "Strategic": [6, 7, 3, 4, 5, 2, 1],
    "Woo": [5, 1, 4, 3, 10, 2, 10],
}

def determing_deal_sales_criteria(company_uuid: list[str]) -> str:
    company_profiles = companies_repository.get_company_profiles(company_uuid)
    list_of_profiles_strengths = [profile.strengths for profile in company_profiles]
    if not list_of_profiles_strengths:
        return None
    for profile_strengths in list_of_profiles_strengths:
        if not profile_strengths:
            continue
        profile_strengths = [strength for strength in profile_strengths if strength.strength_name in strengths_mapping]
        if not profile_strengths:
            continue
        profile_category = determine_profile_category(profile_strengths)
        if profile_category:
            return profile_category.category

# Function to calculate the best profile
def determine_profile_category(strengths_scores):
    total_strength_score = 0
    for strength in strengths_scores:
        total_strength_score += strength['score']
    normalized_strengths = {}
    for strength  in strengths_scores:
        normalized_strengths[strength['strength_name']] = strength['score'] / total_strength_score            

    # Initialize profile scores
    profile_scores = {profile: 0 for profile in profiles}

    for strength, normalized_weight in normalized_strengths.items():
        if strength in strengths_mapping:
            for i, profile in enumerate(profiles):
                score = strengths_mapping[strength][i]
                if score != 10:  # Ignore irrelevant scores
                    profile_scores[profile] += normalized_weight * score

    # Select the profile with the lowest total score
    best_profile = min(profile_scores, key=profile_scores.get)
    return ProfileCategory(category=best_profile, scores=profile_scores)

def get_default_individual_sales_criteria(profile_category: str) -> list[SalesCriteria]:
    return sales_criteria_mapping[profile_category]


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
        logger.info(f"Personal data not found for {person.uuid}")
        return None
    personal_data = row_dict.get("pdl_personal_data")
    if not personal_data:
        logger.error(f"Personal data not found for {person.uuid}")
        return None
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
    pdl_company, pdl_position = get_company_and_position_from_pdl_experience(personal_experience)
    position = pdl_position
    if not company:
        company = pdl_company

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


def get_company_and_position_from_apollo_personal_data(personal_data):
    position = personal_data.get("title", "")
    company = personal_data.get("organization", "")
    if company:
        company = company.get("name")
    experience_list = personal_data.get("employment_history")
    if experience_list and isinstance(experience_list, list):
        last_workplace = experience_list[0]
        if not position:
            position = last_workplace.get("title")
        if not company:
            company = last_workplace.get("organization_name")
    return company, position


def create_person_from_apollo_personal_data(person: PersonDTO):
    row_dict = personal_data_repository.get_personal_data_row(person.uuid)
    if not row_dict or row_dict.get("apollo_status") == personal_data_repository.TRIED_BUT_FAILED:
        return None
    personal_data = row_dict.get("apollo_personal_data")
    if not personal_data:
        logger.error(f"Personal data not found for {person.uuid}")
        return None
    apollo_company, position = get_company_and_position_from_apollo_personal_data(personal_data)
    company = get_company_name_from_domain(person.email)
    if not company:
        company = apollo_company
    linkedin_url = row_dict.get("linkedin_url")
    if not linkedin_url:
        logger.error(f"LinkedIn URL not found for {person.uuid}")
    else:
        linkedin_url = fix_linkedin_url(linkedin_url)
    logger.debug(f"Linkedin URL: {linkedin_url}")
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


def get_company_and_position_from_pdl_experience(personal_experience: dict) -> (str, str):
    """
    Get the company name from the experience section of the PDL personal data.

    Args:
        experience (dict): The experience section of the PDL personal data.

    Returns:
        str: The company name.
    """
    if personal_experience and isinstance(personal_experience, list):
        personal_experience = personal_experience[0]
    company = ""
    position = ""

    if personal_experience and isinstance(personal_experience, dict):
        title_object = personal_experience.get("title")
        if title_object and isinstance(title_object, dict):
            position = title_object.get("name")
            company_object = personal_experience.get("company")
            if company_object and isinstance(company_object, dict):
                company = company_object.get("name")
    return company, position
