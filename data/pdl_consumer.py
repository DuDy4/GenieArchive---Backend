"""Module for interacting with the People Data Labs API."""
import json
import os
import sys
import asyncio
import traceback
from datetime import datetime, timedelta
from dotenv import load_dotenv
from peopledatalabs import PDLPY

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.utils import env_utils
from data.data_common.utils.str_utils import get_uuid4, to_custom_title_case
from data.data_common.dependencies.dependencies import (
    personal_data_repository,
    companies_repository,
)
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.repositories.personal_data_repository import (
    PersonalDataRepository,
)
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from common.genie_logger import GenieLogger

logger = GenieLogger()
load_dotenv()
PDL_API_KEY = env_utils.get("PDL_API_KEY")
CONSUMER_GROUP = "pdlconsumergroup"
MIN_INTERVAL_TO_FETCH_PROFILES = int(
    env_utils.get("MIN_INTERVAL_TO_FETCH_PROFILES", 60 * 60 * 24 * 60)
)  # Default: 24 hours


class PDLConsumer(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[Topic.PDL_NEW_PERSON_TO_ENRICH, Topic.PDL_NEW_EMAIL_ADDRESS_TO_ENRICH],
            consumer_group=CONSUMER_GROUP,
        )
        self.personal_data_repository = personal_data_repository()
        self.pdl_client = create_pdl_client(self.personal_data_repository)
        self.company_repository = companies_repository()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        # Should use Topic class

        match topic:
            case Topic.PDL_NEW_PERSON_TO_ENRICH:
                logger.info("Handling new person to enrich")
                await self.enrich_person(event)
            case Topic.PDL_NEW_EMAIL_ADDRESS_TO_ENRICH:
                logger.info("Handling new interaction")
                await self.enrich_email_address(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def enrich_person(self, event):
        event_body = event.body_as_str()
        if isinstance(event_body, str):
            try:
                logger.info(f"Event body is string")
                event_body = json.loads(event_body)
                if isinstance(event_body, str):
                    event_body = json.loads(event_body)
                logger.debug(f"Event body: {str(event_body)[:300]}")
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON: {event_body}")
                return {"error": "Invalid JSON"}
        if event_body.get("person"):
            person = PersonDTO.from_dict(event_body.get("person"))
        else:
            person = PersonDTO.from_dict(event_body)
        logger.info(f"Person: {person}")
        if not person:
            logger.error(f"Failed to create person from event body")
            return {"status": "failed"}

        person.linkedin = self.pdl_client.fix_linkedin_url(person.linkedin)
        if person.linkedin and self.personal_data_repository.exists_linkedin_url(person.linkedin):
            logger.info(
                f"Personal data for {person.name if person.name else person.uuid} already exists in the database."
            )
            if self.pdl_client.is_up_to_date(person.uuid):
                # Check status if fetched or failed before
                pdl_status = self.personal_data_repository.get_pdl_status(person.uuid)
                if pdl_status == self.personal_data_repository.TRIED_BUT_FAILED or not pdl_status:
                    # If already tried but failed before, check if apollo has new information since last fetch
                    if self.pdl_client.has_other_new_data(person.uuid):
                        logger.warning(f"Profile for {person.uuid} has other new data.")
                        if person.linkedin:
                            personal_data = self.pdl_client.fetch_profile(person)
                            result = self.pdl_client.handle_fetched_profile(
                                person.email, personal_data, person
                            )
                            return result
                        else:
                            logger.error(f"LinkedIn URL not found for {person.uuid}")
                            self.personal_data_repository.update_pdl_status(
                                person.uuid, self.personal_data_repository.TRIED_BUT_FAILED
                            )

                            event = GenieEvent(
                                Topic.PDL_FAILED_TO_ENRICH_PERSON,
                                {"person": person.to_dict(), "email": person.email},
                                "public",
                            )
                            event.send()
                            return {"status": "failed"}
                    # If already tried but failed before, and no new data from apollo, send up-to-date event
                    apollo_last_updated = self.personal_data_repository.get_apollo_last_updated(person.uuid)
                    if not apollo_last_updated:
                        logger.error(f"Last updated timestamp not found for {person.uuid}")
                        event = GenieEvent(
                            Topic.PDL_FAILED_TO_ENRICH_PERSON,
                            {"person": person.to_dict()},
                            "public",
                        )
                        event.send()
                        return {"status": "failed"}
                    logger.info(f"Already tried but failed for {person.uuid}, skipping...")
                    data_to_transfer = {
                        "person": person.to_dict() if person else None,
                    }
                    event = GenieEvent(Topic.ALREADY_PDL_FAILED_TO_ENRICH_PERSON, data_to_transfer, "public")
                    event.send()
                    logger.info(f"Sending event to {Topic.PDL_UP_TO_DATE_ENRICHED_DATA}")
                    return

                # If already fetched and up-to-date, send up-to-date event
                logger.info(f"Personal data is up-to-date. Skipping update for {person.uuid}.")
                personal_data = self.personal_data_repository.get_pdl_personal_data(person.uuid)
                logger.debug(f"Person: {person}")
                logger.debug(f"Personal data: {str(personal_data)[:300]}")
                if not personal_data:
                    logger.error(f"Unexpected error: Personal data fetched but not found for {person.uuid}")
                else:
                    person = self.verify_person(person, personal_data)
                logger.debug(f"Person: {person}")
                if not person:
                    logger.error(f"Failed to create person from personal data")
                    return {"status": "failed"}

                data_to_transfer = {
                    "person": person.to_dict(),
                }
                event = GenieEvent(Topic.PDL_UP_TO_DATE_ENRICHED_DATA, data_to_transfer, "public")
                event.send()
                logger.info(f"Sending event to {Topic.PDL_UP_TO_DATE_ENRICHED_DATA}")
                return
            else:
                logger.info(f"Personal data for {person.name} is outdated. Fetching new data")
                personal_data = self.pdl_client.fetch_profile(person)
                result = self.pdl_client.handle_fetched_profile(person.email, personal_data, person)
                return result
        else:
            logger.info(f"Person {person.name} not found in the database. Fetching new personal data")
            profile = self.pdl_client.fetch_profile(person)
            result = self.pdl_client.handle_fetched_profile(person.email, profile, person)
            return result
            # data_to_transfer = {"person": person.to_dict(), "personal_data": profile}
            # event = GenieEvent(Topic.PDL_UPDATED_ENRICHED_DATA, data_to_transfer, "public")
            # event.send()
            # logger.info(f"Sending event to {Topic.PDL_UPDATED_ENRICHED_DATA}")

        return {"status": "success"}

    async def enrich_email_address(self, event):
        """
        Enriches the personal data for a given email address.
        1. Checks for existing personal data.
        2. If exists, checks for last_updated timestamp. If still up-to-date, skips.
        3. If not up-to-date, or not exists - fetches the personal data from PDL.
        4. Saves the personal data to the database.
        5. Create a PersonDTO object.
        6. Sends an event to the event queue with the PersonDTO and personal data.
        """
        event_body = event.body_as_str()
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            try:
                logger.info(f"Event body is string")
                event_body = json.loads(event_body)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON: {event_body}")
                return {"error": "Invalid JSON"}

        email = event_body.get("email")
        uuid = event_body.get("uuid")
        tenant_id = event_body.get("tenant_id")
        logger.info(f"Email: {email}, uuid: {uuid}, tenant_id: {tenant_id}")

        # Check if the email already exists in the database - should return uuid of the existing record
        existing_uuid = self.personal_data_repository.get_personal_uuid_by_email(email)
        if existing_uuid:
            # If personal data exists in database for the email
            personal_data_in_repo = self.personal_data_repository.get_pdl_personal_data(existing_uuid)
            personal_data = ""

            # If personal data exists in database and is up-to-date, skip
            if self.pdl_client.is_up_to_date(existing_uuid):
                # If already tried but failed before, skip
                pdl_status = self.personal_data_repository.get_pdl_status(existing_uuid)
                logger.debug(f"Status: {pdl_status}")
                if pdl_status == self.personal_data_repository.TRIED_BUT_FAILED or not pdl_status:
                    if self.pdl_client.has_other_new_data(existing_uuid):
                        logger.warning(f"Profile for {existing_uuid} has other new data.")
                        person = PersonDTO(
                            uuid=existing_uuid,
                            name="",
                            email=email,
                            linkedin="",
                            company="",
                            position="",
                            timezone="",
                        )
                        personal_data = self.pdl_client.fetch_profile(person)
                        if personal_data:
                            person = self.pdl_client.create_person_from_personal_data(existing_uuid)
                        result = self.pdl_client.handle_fetched_profile(person.email, personal_data, person)
                    logger.info(f"Already tried but failed for {email}, skipping...")
                    apollo_personal_data = self.personal_data_repository.get_apollo_personal_data(
                        existing_uuid
                    )
                    if not apollo_personal_data:
                        event = GenieEvent(
                            Topic.ALREADY_PDL_FAILED_TO_ENRICH_EMAIL,
                            {"email": email, "tenant_id": tenant_id},
                            "public",
                        )
                        event.send()
                        logger.info(f"Sending event to {Topic.ALREADY_PDL_FAILED_TO_ENRICH_EMAIL}")
                        return
                    logger.info(f"Already tried but failed for {email}, but has apollo data.")
                    event = GenieEvent(
                        Topic.PDL_FAILED_TO_ENRICH_EMAIL,
                        {"email": email, "tenant_id": tenant_id},
                        "public",
                    )
                    event.send()
                    return {"status": "failed"}

                # If it has up-to-date personal data, send event
                person = self.pdl_client.create_person_from_personal_data(existing_uuid)
                logger.info(f"Person: {person}")
                if not person:
                    logger.error(f"Failed to create person from personal data")
                    return {"status": "failed"}
                personal_data = personal_data_in_repo
                data_to_transfer = {
                    "person": person.to_dict(),
                }
                event = GenieEvent(Topic.PDL_UP_TO_DATE_ENRICHED_DATA, data_to_transfer, "public")
                event.send()
                logger.info(f"Sending event to {Topic.PDL_UP_TO_DATE_ENRICHED_DATA}")

            # If passed long time since last pdl fetch, fetch again
            else:
                personal_data = self.pdl_client.get_single_profile_from_email_address(email)
                if not personal_data:
                    self.personal_data_repository.save_pdl_personal_data(
                        person=person,
                        personal_data=None,
                        status=self.personal_data_repository.TRIED_BUT_FAILED,
                    )
                    logger.error(f"Failed to fetch personal data for {email}")
                    event = GenieEvent(
                        Topic.PDL_FAILED_TO_ENRICH_EMAIL,
                        {"email": email, "tenant_id": tenant_id},
                        "public",
                    )
                    event.send()
                    return {"status": "failed"}
                logger.info(f"Fetched Personal data from PDL: {personal_data}")
                experience: list = personal_data.get("experience")
                if experience:
                    personal_data["experience"] = self.pdl_client.fix_and_sort_experience(experience)
            personal_data = self.merge_personal_data(personal_data_in_repo, personal_data)
            if personal_data:
                if personal_data != personal_data_in_repo:
                    self.personal_data_repository.update_pdl_personal_data(
                        uuid=existing_uuid, personal_data=personal_data
                    )
                person = self.pdl_client.create_person_from_personal_data(existing_uuid)
                self.send_event(person, personal_data, tenant_id)
                return {"status": "success"}
            else:
                logger.info(f"Failed to fetch personal data for {email}")
                return

        # If no personal data exists in database for the email
        logger.info(f"No personal data found in database for {email}")
        personal_data = self.pdl_client.get_single_profile_from_email_address(email)
        if personal_data:
            experience: list = personal_data.get("experience")
            if experience:
                personal_data["experience"] = self.pdl_client.fix_and_sort_experience(experience)

        logger.info(f"Personal data: {personal_data}")
        if personal_data:
            linkedin_url = ""
            social_profiles = personal_data.get("profiles", {})
            for social_profile in social_profiles:
                if social_profile.get("network") == "linkedin":
                    linkedin_url = social_profile.get("url")
                    break
            if not uuid:
                uuid = get_uuid4()
            person = self.pdl_client.create_person_from_personal_data(uuid)
            logger.info(f"Created person from personal data: {person}")
            self.personal_data_repository.save_pdl_personal_data(
                person=person, email=email, personal_data=personal_data
            )

            person = self.pdl_client.create_person_from_personal_data(uuid)
            logger.info(f"Created person from personal data: {person}")
            if person:
                self.send_event(person, personal_data, tenant_id)
                return {"status": "success"}
            else:
                logger.error(f"Failed to create person from personal data")
                return {"status": "failed"}
        # If no personal data exists in PDL for the email
        else:
            logger.info(f"Failed to fetch personal data for {email}")
            if not uuid:
                uuid = get_uuid4()
            person = PersonDTO(
                uuid=uuid,
                name="",
                email=email,
                linkedin="",
                company="",
                position="",
                timezone="",
            )
            self.personal_data_repository.save_pdl_personal_data(
                person=person,
                personal_data=personal_data,
                status=self.personal_data_repository.TRIED_BUT_FAILED,
            )
            event = GenieEvent(
                Topic.PDL_FAILED_TO_ENRICH_EMAIL,
                {"email": email, "tenant_id": tenant_id},
                "public",
            )
            event.send()

            return {"status": "failed"}

    def send_event(self, person: PersonDTO, personal_data: dict, tenant_id: str):
        data_to_transfer = {
            "person": person.to_dict(),
            "personal_data": personal_data,
            "tenant_id": tenant_id,
        }
        event = GenieEvent(Topic.PDL_UPDATED_ENRICHED_DATA, data_to_transfer, "public")
        event.send()
        logger.info(f"Sending event to {Topic.PDL_UPDATED_ENRICHED_DATA}")

    def verify_person(self, person, personal_data):
        if not person:
            PersonDTO(
                uuid=personal_data.get("uuid"),
                name=personal_data.get("full_name"),
                email=personal_data.get("email"),
                linkedin=personal_data.get("linkedin_url"),
                company="",
                position="",
                timezone="",
            )
            return person
        if not personal_data:
            return person
        personal_experience = personal_data.get("experience")
        if personal_experience and isinstance(personal_experience, list):
            personal_experience = personal_experience[0]

        if personal_experience and isinstance(personal_experience, dict):
            title_object = personal_experience.get("title")
            if title_object and isinstance(title_object, dict):
                position = title_object.get("name")
            else:
                position = personal_experience.get["job_title"]
            company_object = personal_experience.get("company")
            if company_object and isinstance(company_object, dict):
                company = company_object.get("name")
            person.position = position
            person.company = company
        return person

    def merge_personal_data(self, personal_data_in_repo, personal_data):
        """
        Needs to be implemented after deciding how to merge the data
        """
        logger.warning("Needs to be implemented after deciding how to merge the data")
        if personal_data:
            return personal_data
        else:
            return personal_data_in_repo


class PDLClient:
    """Class for interacting with the People Data Labs API."""

    def __init__(self, api_key: str, personal_data_repository: PersonalDataRepository):
        """
        Initializes the PDLClient with the given API key and profiles repository.

        Args:
            api_key (str): The API key for the People Data Labs API.
            personal_data_repository (PersonalDataRepository): The repository for storing profiles.
        """
        self.personal_data_repository = personal_data_repository

        self._client = PDLPY(api_key=api_key)
        self._tried_but_failed = set()
        self._fetched_profiles = set()
        self.company_repository = companies_repository()

    def fetch_profile(self, person):
        if person.linkedin:
            personal_data = self.get_single_profile(person.linkedin)
        elif person.email:
            personal_data = self.get_single_profile_from_email_address(person.email)
        else:
            logger.warning(f"No LinkedIn or email for {person.uuid}")
            return
        logger.debug(f"Personal data: {str(personal_data)[:300]}")
        status = (
            self.personal_data_repository.FETCHED
            if personal_data
            else self.personal_data_repository.TRIED_BUT_FAILED
        )
        logger.debug(f"Status: {status}")
        self.personal_data_repository.save_pdl_personal_data(
            person=person, personal_data=personal_data, status=status
        )
        if not person.name and personal_data:
            person.name = personal_data.get("full_name", "")
            person.position = personal_data.get("job_title", "")

        return personal_data

    def identify_person(self, email, first_name, last_name, company) -> list[dict] | None:
        params = {
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "company": company,
            "pretty": True,
        }

        # Pass the parameters object to the Person Identify API
        response = self._client.person.identify(**params).json()

        # Check for successful response
        if response["status"] == 200:
            # Create a list of matches
            identities: list[dict] = response["matches"]

            # Print the matches in JSON format
            logger.info(f"Found {len(identities)} identities!")
            logger.info(identities)
            return identities

        else:
            logger.info("Identify unsuccessful. See error and try again.")
            logger.info("error:", response)

    def get_single_profile(self, linkedin_profile_url: str) -> dict[str, dict] | None:
        existing_profile = self.personal_data_repository.get_pdl_personal_data_by_linkedin(
            linkedin_profile_url
        )
        if existing_profile:
            if not self.does_need_update(existing_profile[0]):
                return existing_profile

        linkedin_profile_url = self.fix_linkedin_url(linkedin_profile_url)
        params = {"profile": [linkedin_profile_url]}

        # Pass the parameters object to the Person Enrichment API
        response = self._client.person.enrichment(**params).json()
        if response["status"] == 404:
            logger.warning(f"Cannot find profiles for {linkedin_profile_url}")
            return
        if response["status"] == 402:
            logger.warning(f"Need Payment")
            return
        else:
            logger.info(
                f"Got profile for {linkedin_profile_url} from PDL. Data: {str(response['data'])[:200]}"
            )
            return response["data"]

    def get_single_profile_from_email_address(self, email_address: str) -> dict[str, dict] | None:
        existing_uuid = self.personal_data_repository.get_personal_uuid_by_email(email_address)
        existing_profile = self.personal_data_repository.get_pdl_personal_data_by_email(email_address)
        if existing_profile:
            if not self.does_need_update(existing_uuid):
                return json.loads(existing_profile) if isinstance(existing_profile, str) else existing_profile

        params = {"email": email_address}

        # Pass the parameters object to the Person Enrichment API
        response = self._client.person.enrichment(**params).json()
        if response["status"] == 404:
            logger.warning(f"Cannot find profiles for {email_address}")
            return
        if response["status"] == 402:
            logger.warning(f"Need Payment")
            return
        else:
            logger.info(f"Got profile for {email_address} from PDL")
            return response["data"]

    def fix_linkedin_url(self, linkedin_url: str) -> str:
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

    def create_person_from_personal_data(self, uuid):
        row_dict = self.personal_data_repository.get_personal_data_row(uuid)
        if not row_dict or row_dict.get("status") == self.personal_data_repository.TRIED_BUT_FAILED:
            return None
        if not row_dict.get("personal_data"):
            logger.error(f"Personal data not found for {uuid} - in circumstances it should not happen")
            return None
        personal_data = row_dict.get("personal_data")
        logger.info(f"Personal data: {str(personal_data)[:300]}")
        personal_experience = personal_data.get("experience")
        logger.info(f"Personal experience: {str(personal_experience)[:300]}")
        position = ""
        company = ""
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
        person_email = row_dict.get("email", "")
        logger.info(
            f"Position: {position}, Company: {company}, Person Name: {person_name}, Person Email: {person_email}"
        )

        person = PersonDTO(
            uuid=uuid,
            name=person_name,
            company=company,
            email=person_email,
            linkedin=row_dict.get("linkedin_url", ""),
            position=position,
            timezone="",
        )
        company_domain = (
            person_email.split("@")[1] if isinstance(person_email, str) and "@" in person_email else ""
        )
        if company_domain:
            company = self.company_repository.get_company_from_domain(company_domain)
            if company:
                person.company = company.name
        logger.info(f"Person: {person}")
        return person

    def get_company_profile(self, company_website: str) -> dict[str, dict] | None:
        # linkedin_profile_url = self.profiles_repository._fix_linkedin_url(linkedin_profile_url)
        # existing_company_summary = self.profiles_repository.get_profile_data(linkedin_profile_url)
        # if existing_company_summary:
        #     return existing_company_summary
        logger.info("About to request PDL for company data")
        params = {"website": company_website}

        # Pass the parameters object to the Person Enrichment API
        response = self._client.company.enrichment(**params).json()
        if response["status"] == 404:
            logger.warning(f"Cannot find data for {company_website}")
            # self.profiles_repository.insert_tried_but_failed_profiles([linkedin_profile_url])
            return None
        else:
            # self.profiles_repository.insert_fetched_profiles({linkedin_profile_url: response["data"]})
            return response["summary"]

    def does_need_update(self, uuid):
        last_updated_timestamp = self.personal_data_repository.get_pdl_last_updated(uuid)
        if last_updated_timestamp:
            logger.debug(f"Last updated: {last_updated_timestamp}")
            time_since_last_update = datetime.now() - last_updated_timestamp
            if time_since_last_update < timedelta(seconds=MIN_INTERVAL_TO_FETCH_PROFILES):
                logger.info(f"Skipping update for uuid: {uuid}, last updated {time_since_last_update} ago.")
                # add to daily queue?
                return False
            else:
                return True

    def handle_fetched_profile(self, email_address: str, personal_data: dict, person: PersonDTO = None):
        if not person:
            person = self.create_person_from_personal_data(personal_data.get("uuid"))
        if not personal_data:
            logger.error(f"Failed to fetch personal data for {person.name}")
            self.personal_data_repository.save_pdl_personal_data(
                person=person,
                status=self.personal_data_repository.TRIED_BUT_FAILED,
                personal_data=personal_data,
            )
            logger.info(f"Updated timestamp for failing to get personal data for {person.name}")
            event = GenieEvent(
                Topic.PDL_FAILED_TO_ENRICH_PERSON,
                {
                    "person": person.to_dict() if person else None,
                    "email": person.email if not person else None,
                },
                "public",
            )
            event.send()
            return {"status": "failed"}
        if not person:
            logger.error(f"Unexpected error: Got personal data but not personfor email: {email_address}")
        data_to_transfer = {
            "person": person.to_dict() if person else None,
            "personal_data": personal_data,
        }
        event = GenieEvent(Topic.PDL_UPDATED_ENRICHED_DATA, data_to_transfer, "public")
        event.send()
        logger.info(f"Sending event to {Topic.PDL_UPDATED_ENRICHED_DATA}")
        return {"status": "success"}

    @staticmethod
    def fix_and_sort_experience(experience):
        try:
            for exp in experience:
                exp["end_date"] = exp.get("end_date") or "9999-12-31"  # Treat ongoing as future date
                exp["start_date"] = exp.get("start_date") or "0000-01-01"

                # Sort experience
            sorted_experience = sorted(
                experience, key=lambda x: (x["end_date"], x["start_date"]), reverse=True
            )
        except:
            logger.error(f"Error fixing and sorting experience: {experience}")
            sorted_experience = experience
        for exp in sorted_experience:
            try:
                if exp.get("end_date") == "9999-12-31":
                    exp["end_date"] = None
                if exp.get("start_date") == "0000-01-01":
                    exp["start_date"] = None
                title = exp.get("title")
                if title and isinstance(title, dict):
                    name = title.get("name")
                    titleize_name = to_custom_title_case(name)
                    exp["title"]["name"] = titleize_name
                company = exp.get("company")
                if company and isinstance(company, dict):
                    name = company.get("name")
                    titleize_name = to_custom_title_case(name)
                    exp["company"]["name"] = titleize_name
            except Exception as e:
                logger.error(f"Error: {e}")
                traceback.print_exc()
                continue
        return to_custom_title_case(sorted_experience)

    def is_up_to_date(self, existing_uuid):
        last_updated_timestamp = self.personal_data_repository.get_pdl_last_updated(existing_uuid)
        if last_updated_timestamp:
            logger.debug(f"Last updated: {last_updated_timestamp}")
            time_since_last_update = datetime.now() - last_updated_timestamp
            if time_since_last_update < timedelta(seconds=MIN_INTERVAL_TO_FETCH_PROFILES):
                logger.info(f"{existing_uuid} is up-to-date")
                return True
        logger.info(f"{existing_uuid} is not up-to-date")
        return False

    def has_other_new_data(self, existing_uuid):
        last_pdl_updated_timestamp = self.personal_data_repository.get_pdl_last_updated(existing_uuid)
        last_apollo_updated_timestamp = self.personal_data_repository.get_apollo_last_updated(existing_uuid)
        if not last_apollo_updated_timestamp:
            logger.warning(f"No Apollo last timestamp for {existing_uuid}")
            return False
        if last_pdl_updated_timestamp and last_apollo_updated_timestamp:
            if last_pdl_updated_timestamp <= last_apollo_updated_timestamp:
                logger.info(f"There is new personal data for {existing_uuid}")
                return True
        return False


def create_pdl_client(personal_data_repository: PersonalDataRepository) -> PDLClient:
    """
    Factory method to create a PDLClient object.

    Args:
        personal_data_repository (PersonalDataRepository): The repository for storing profiles.

    Returns:
        PDLClient: The PDLClient object.
    """
    return PDLClient(api_key=PDL_API_KEY, personal_data_repository=personal_data_repository)


if __name__ == "__main__":
    pdl_consumer = PDLConsumer()
    try:
        asyncio.run(pdl_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
