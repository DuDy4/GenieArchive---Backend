"""Module for interacting with the People Data Labs API."""
import json
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from loguru import logger
from peopledatalabs import PDLPY

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.utils.str_utils import get_uuid4
from data.data_common.dependencies.dependencies import personal_data_repository
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.repositories.personal_data_repository import (
    PersonalDataRepository,
)
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.data_transfer_objects.person_dto import PersonDTO

load_dotenv()
PDL_API_KEY = os.environ.get("PDL_API_KEY")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP_PDL", "pdlconsumergroup")
MIN_INTERVAL_TO_FETCH_PROFILES = int(
    os.environ.get("MIN_INTERVAL_TO_FETCH_PROFILES", 60 * 60 * 24)
)  # Default: 24 hours


class PDLConsumer(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[Topic.NEW_CONTACT_TO_ENRICH, Topic.NEW_EMAIL_ADDRESS_TO_ENRICH],
            consumer_group=CONSUMER_GROUP,
        )
        self.personal_data_repository = personal_data_repository()
        self.pdl_client = create_pdl_client(self.personal_data_repository)

    async def process_event(self, event):
        logger.info(f"PersonManager processing event: {event}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        # Should use Topic class

        match topic:
            case Topic.NEW_CONTACT_TO_ENRICH:
                logger.info("Handling new contact")
                await self.enrich_contact(event)
            case Topic.NEW_EMAIL_ADDRESS_TO_ENRICH:
                logger.info("Handling new interaction")
                await self.enrich_email_address(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def enrich_contact(self, event):
        event_body = event.body_as_str()
        if isinstance(event_body, str):
            try:
                logger.info(f"Event body is string")
                event_body = json.loads(event_body)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON: {event_body}")
                return {"error": "Invalid JSON"}
        person = PersonDTO.from_json(event_body)
        logger.info(f"Person: {person}")

        person.linkedin = self.pdl_client.fix_linkedin_url(person.linkedin)
        if self.personal_data_repository.exists_linkedin_url(person.linkedin):
            logger.info(
                f"Profile for {person.linkedin} already exists in the database."
            )
            last_updated_timestamp = self.personal_data_repository.get_last_updated(
                person.uuid
            )
            if last_updated_timestamp:
                logger.debug(f"Last updated: {last_updated_timestamp}")
                time_since_last_update = datetime.now() - last_updated_timestamp
                if time_since_last_update < timedelta(
                    seconds=MIN_INTERVAL_TO_FETCH_PROFILES
                ):
                    logger.info(
                        f"Skipping update for {person.uuid}, last updated {time_since_last_update} ago."
                    )
                    personal_data = self.personal_data_repository.get_personal_data(
                        person.uuid
                    )

                    data_to_transfer = {
                        "person": person.to_dict(),
                        "personal_data": personal_data,
                    }
                    event = GenieEvent(
                        Topic.UP_TO_DATE_ENRICHED_DATA, data_to_transfer, "public"
                    )
                    event.send()
                    logger.info(f"Sending event to {Topic.UP_TO_DATE_ENRICHED_DATA}")
                    return
                else:
                    personal_data = self.pdl_client.fetch_profile(person)
                    data_to_transfer = {
                        "person": person.to_dict(),
                        "personal_data": personal_data,
                    }
                    event = GenieEvent(
                        Topic.UPDATED_ENRICHED_DATA, data_to_transfer, "public"
                    )
                    event.send()
                    logger.info(f"Sending event to {Topic.UPDATED_ENRICHED_DATA}")
        else:

            profile = self.pdl_client.fetch_profile(person)
            data_to_transfer = {"person": person.to_json(), "personal_data": profile}
            event = GenieEvent(Topic.UPDATED_ENRICHED_DATA, data_to_transfer, "public")
            event.send()
            logger.info(f"Sending event to {Topic.UPDATED_ENRICHED_DATA}")

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
        logger.info(f"Email: {email}")

        # Check if the email already exists in the database - should return uuid of the existing record
        existing_uuid = self.personal_data_repository.get_personal_data_by_email(email)
        if existing_uuid:
            if not self.pdl_client.does_need_last_updated(existing_uuid):
                logger.info(
                    f"Personal data for {email} already exists in the database. And is up to date"
                )
                return
            else:
                # Assuming that we have personal data, but we need to check if it is up-to-date
                personal_data = self.pdl_client.get_single_profile_from_email_address(
                    email
                )
                logger.info(f"Personal data: {personal_data}")
                personal_data_in_repo = self.personal_data_repository.get_personal_data(
                    existing_uuid
                )
                if personal_data_in_repo != personal_data:
                    personal_data = self.merge_personal_data(
                        personal_data_in_repo, personal_data
                    )
                    self.personal_data_repository.save_personal_data(
                        existing_uuid, personal_data
                    )
                    person = self.create_person(existing_uuid)
                    self.send_event(person, personal_data)
                    return {"status": "success"}
                else:
                    logger.info(
                        f"Personal data for {email} already exists in the database. And is up to date"
                    )
                    return {"status": "success"}

        # Assuming that we have personal data, but we need to check if it is up-to-date
        personal_data = self.pdl_client.get_single_profile_from_email_address(email)
        logger.info(f"Personal data: {personal_data}")
        if personal_data:
            linkedin_url = ""
            social_profiles = personal_data.get("profiles", {})
            for social_profile in social_profiles:
                if social_profile.get("network") == "linkedin":
                    linkedin_url = social_profile.get("url")
                    break
            uuid = get_uuid4()
            self.personal_data_repository.insert(
                uuid=uuid,
                name=personal_data.get("full_name").title()
                if personal_data.get("full_name")
                else "",
                email=email,
                linkedin_url=linkedin_url,
                personal_data=json.dumps(personal_data),
            )
            logger.info(f"Saved personal data for {email}")
            person = self.create_person(uuid)
            self.send_event(person, personal_data)
            return {"status": "success"}
        else:
            logger.info(f"Failed to fetch personal data for {email}")
            self.personal_data_repository.insert(
                uuid=get_uuid4(),
                name="",
                email=email,
                linkedin_url="",
                personal_data=personal_data,
                status=self.personal_data_repository.TRIED_BUT_FAILED,
            )
            return {"status": "failed"}

    def send_event(self, person: PersonDTO, personal_data: dict):
        data_to_transfer = {
            "person": person,
            "personal_data": personal_data,
        }
        event = GenieEvent(Topic.UPDATED_ENRICHED_DATA, data_to_transfer, "public")
        event.send()
        logger.info(f"Sending event to {Topic.UPDATED_ENRICHED_DATA}")

    def create_person(self, uuid):
        row_dict = self.personal_data_repository.get_personal_data_row(uuid)
        if not row_dict:
            return None
        personal_data = row_dict["personal_data"]
        position = personal_data.get("experience").get("title").get("name")
        person = PersonDTO(
            uuid=uuid,
            name=row_dict.get("name", "") or personal_data.get("full_name"),
            company=personal_data.get("experience").get("company").get("name"),
            email=row_dict.get("email", ""),
            linkedin=row_dict.get("linkedin_url", ""),
            position=position,
            timezone="",
        )
        return person

    def merge_personal_data(self, personal_data_in_repo, personal_data):
        """
        Needs to be implemented after deciding how to merge the data
        """
        return personal_data


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

    def fetch_profile(self, person):
        profile = None
        if person.linkedin:
            profile = self.get_single_profile(person.linkedin)
        elif person.email:
            profile = self.get_single_profile_from_email_address(person.email)
        else:
            logger.warning(f"No LinkedIn or email for {person.uuid}")
            return
        status = (
            self.personal_data_repository.FETCHED
            if profile
            else self.personal_data_repository.TRIED_BUT_FAILED
        )
        self.personal_data_repository.insert(
            uuid=person.uuid,
            name=person.name,
            email=person.email,
            linkedin_url=person.linkedin,
            personal_data=json.dumps(profile),
            status=status,
        )
        return profile

    def identify_person(
        self, email, first_name, last_name, company
    ) -> list[dict] | None:
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
        existing_profile = self.personal_data_repository.get_personal_data_by_linkedin(
            linkedin_profile_url
        )
        if existing_profile:
            if not self.does_need_last_updated(existing_profile[0]):
                return existing_profile

        linkedin_profile_url = self.fix_linkedin_url(linkedin_profile_url)
        params = {"profile": [linkedin_profile_url]}

        # Pass the parameters object to the Person Enrichment API
        response = self._client.person.enrichment(**params).json()
        logger.debug(f" response: {response}")
        if response["status"] == 404:
            logger.warning(f"Cannot find profiles for {linkedin_profile_url}")
            return
        if response["status"] == 402:
            logger.warning(f"Need Payment")
            return
        else:
            logger.info(f"Got profile for {linkedin_profile_url} from PDL")
            return response["data"]

    def get_single_profile_from_email_address(
        self, email_address: str
    ) -> dict[str, dict] | None:
        existing_profile = self.personal_data_repository.get_personal_data_by_email(
            email_address
        )
        if existing_profile:
            if not self.does_need_last_updated(existing_profile[0]):
                return (
                    json.loads(existing_profile[4])
                    if isinstance(existing_profile[4], str)
                    else existing_profile[4]
                )

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
        linkedin_url = linkedin_url.replace(
            "http://www.linkedin.com/in/", "linkedin.com/in/"
        )
        linkedin_url = linkedin_url.replace(
            "https://www.linkedin.com/in/", "linkedin.com/in/"
        )
        linkedin_url = linkedin_url.replace(
            "http://linkedin.com/in/", "linkedin.com/in/"
        )
        linkedin_url = linkedin_url.replace(
            "https://linkedin.com/in/", "linkedin.com/in/"
        )

        if linkedin_url[-1] == "/":
            linkedin_url = linkedin_url[:-1:]
        return linkedin_url

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

    def does_need_last_updated(self, uuid):
        last_updated_timestamp = self.personal_data_repository.get_last_updated(uuid)
        if last_updated_timestamp:
            logger.debug(f"Last updated: {last_updated_timestamp}")
            time_since_last_update = datetime.now() - last_updated_timestamp
            if time_since_last_update < timedelta(
                seconds=MIN_INTERVAL_TO_FETCH_PROFILES
            ):
                logger.info(
                    f"Skipping update for uuid: {uuid}, last updated {time_since_last_update} ago."
                )
                # add to daily queue?
                return False
            else:
                return True


def create_pdl_client(personal_data_repository: PersonalDataRepository) -> PDLClient:
    """
    Factory method to create a PDLClient object.

    Args:
        personal_data_repository (PersonalDataRepository): The repository for storing profiles.

    Returns:
        PDLClient: The PDLClient object.
    """
    return PDLClient(
        api_key=PDL_API_KEY, personal_data_repository=personal_data_repository
    )


if __name__ == "__main__":
    pdl_consumer = PDLConsumer()
    # uvicorn.run(
    #     "person:app",
    #     host="0.0.0.0",
    #     port=PERSON_PORT,
    #     ssl_keyfile="../key.pem",
    #     ssl_certfile="../cert.pem",
    # )
    # print("Running person service")
    pdl_consumer.run()
