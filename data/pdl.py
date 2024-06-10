"""Module for interacting with the People Data Labs API."""
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from loguru import logger
from peopledatalabs import PDLPY

from common.dependencies.dependencies import personal_data_repository
from common.events.genie_event import GenieEvent
from common.events.topics import Topic
from common.repositories.personal_data_repository import PersonalDataRepository
from common.events.genie_consumer import GenieConsumer
from common.data_transfer_objects.person_dto import PersonDTO as DTOPerson

load_dotenv()
PDL_API_KEY = os.environ.get("PDL_API_KEY")
MIN_INTERVAL_TO_FETCH_PROFILES = int(
    os.environ.get("MIN_INTERVAL_TO_FETCH_PROFILES", 60 * 60 * 24)
)  # Default: 24 hours


class PDLConsumer(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(topics=[Topic.NEW_CONTACT_TO_ENRICH])
        self.personal_data_repository = personal_data_repository()
        self.pdl_client = create_pdl_client(self.personal_data_repository)

    async def process_event(self, event):
        logger.info(f"Processing event on topic {self.topics}")
        event_body = event.body_as_str()
        if isinstance(event_body, str):
            try:
                logger.info(f"Event body is string")
                event_body = json.loads(event_body)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON: {event_body}")
                return
        person = DTOPerson.from_json(event_body)
        logger.info(f"Person: {person}")

        # """
        # The flow is as follows:
        # 1. Check if the person exists in the database.
        #     2. If the person exists, check the status and the last updated timestamp.
        #     3. decide if we want to send a request.
        #     4. if the result is good, save it to the database.
        # 5. if does not exists in database, send a request.
        # 6. if the result is good, save it to the database.
        # 7. if the result is bad, save it to the database.
        # """
        person.linkedin = self.pdl_client.fix_linkedin_url(person.linkedin)
        if self.personal_data_repository.exists_linkedin_url(person.linkedin):
            last_updated_timestamp = self.personal_data_repository.get_last_updated(
                person.uuid
            )
            if last_updated_timestamp:
                time_since_last_update = datetime.now() - last_updated_timestamp
                if time_since_last_update < timedelta(
                    seconds=MIN_INTERVAL_TO_FETCH_PROFILES
                ):
                    logger.info(
                        f"Skipping update for {person.uuid}, last updated {time_since_last_update} ago."
                    )
                    # add to daily queue?
                    return
                else:
                    personal_data = self.pdl_client.fetch_profile(person)
                    data_to_transfer = {
                        "person": person.to_dict(),
                        "personal_data": personal_data,
                    }
                    logger.debug(f"Profile type: {type(personal_data)}")
                    event = GenieEvent(
                        Topic.UPDATED_ENRICHED_DATA, data_to_transfer, "public"
                    )
                    event.send()
                    logger.info(f"Sending event to {Topic.UPDATED_ENRICHED_DATA}")
        else:
            profile = self.pdl_client.fetch_profile(person)
            logger.debug(f"Profile type: {type(profile)}")
            data_to_transfer = {"person": person.to_json(), "profile": profile}
            event = GenieEvent(Topic.UPDATED_ENRICHED_DATA, data_to_transfer, "public")
            event.send()
            logger.info(f"Sending event to {Topic.UPDATED_ENRICHED_DATA}")


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
        profile = self.get_single_profile(person.linkedin)
        status = (
            self.personal_data_repository.FETCHED
            if profile
            else self.personal_data_repository.TRIED_BUT_FAILED
        )
        self.personal_data_repository.insert(
            uuid=person.uuid,
            name=person.name,
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
        linkedin_profile_url = self.fix_linkedin_url(linkedin_profile_url)
        params = {"profile": [linkedin_profile_url]}

        # Pass the parameters object to the Person Enrichment API
        response = self._client.person.enrichment(**params).json()
        if response["status"] == 404:
            logger.warning(f"Cannot find profiles for {linkedin_profile_url}")
            return
        else:
            logger.info(f"Got profile for {linkedin_profile_url} from PDL")
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
