"""Module for interacting with the People Data Labs API."""
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from loguru import logger
from peopledatalabs import PDLPY

from common.dependencies.dependencies import personal_data_repository
from common.events.topics import Topic
from common.repositories.personal_data_repository import PersonalDataRepository
from common.events.genie_consumer import GenieConsumer
from common.data_transfer_objects.person import PersonDTO as DTOPerson

load_dotenv()
PDL_API_KEY = os.environ.get("PDL_API_KEY")
MIN_INTERVAL_TO_FETCH_PROFILES = int(
    os.environ.get("MIN_INTERVAL_TO_FETCH_PROFILES", 60 * 60 * 24)
)  # Default: 24 hours


class PDLConsumer(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(topics=[Topic.PDL_WITH_LINKEDIN, Topic.PDL_WITHOUT_LINKEDIN])
        self.personal_data_repository = personal_data_repository()
        self.pdl_client = create_pdl_client(self.personal_data_repository)

    async def process_event(self, event):
        logger.info(f"Processing event on topic {Topic.PDL}")
        event_body = event.body_as_str()
        logger.info(f"Event body: {event_body}")
        person = DTOPerson.from_json(event_body)
        logger.info(f"Person: {person}")

        if self.personal_data_repository.exists(person.uuid):
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
                    profiles = self.pdl_client.identify_person(
                        person.email,
                        person.first_name,
                        person.last_name,
                        person.company,
                    )
            else:
                logger.info(
                    f"No last updated timestamp for {person.uuid}, proceeding with update."
                )
        logger.info(f"Fetching profiles for {person.uuid}")

    # this is not finished - need to decide what to do if time_since_last_update < MIN_INTERVAL_TO_FETCH_PROFILES.


class PDLClient:
    """Class for interacting with the People Data Labs API."""

    def __init__(self, api_key: str, profiles_repository: PersonalDataRepository):
        """
        Initializes the PDLClient with the given API key and profiles repository.

        Args:
            api_key (str): The API key for the People Data Labs API.
            profiles_repository (PersonalDataRepository): The repository for storing profiles.
        """
        self.profiles_repository = profiles_repository

        self._client = PDLPY(api_key=api_key)
        self._tried_but_failed = set()
        self._fetched_profiles = set()

    def _filter_out_profile_urls(self, linkedin_profile_urls: list[str]) -> list[str]:
        """
        Filters out profile URLs that are already in the database.

        Args:
            linkedin_profile_urls (list[str]): A list of LinkedIn profile URLs.

        Returns:
            list[str]: A list of filtered profile URLs.
        """
        already_fetched_profile_urls = (
            self.profiles_repository.get_fetched_profile_urls()
        )
        already_tried_but_failed_profile_urls = (
            self.profiles_repository.get_tried_but_failed_profile_urls()
        )

        logger.warning(
            f"Already fetched {len([url for url in already_fetched_profile_urls])} profile urls"
        )
        logger.warning(
            f"Already tried but failed {len([url for url in already_tried_but_failed_profile_urls])} profile urls"
        )

        return [
            url
            for url in linkedin_profile_urls
            if url not in already_fetched_profile_urls
            and url not in already_tried_but_failed_profile_urls
        ]

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
        linkedin_profile_url = self.profiles_repository._fix_linkedin_url(
            linkedin_profile_url
        )
        existing_profile = self.profiles_repository.get_profile_data(
            linkedin_profile_url
        )
        if existing_profile:
            return existing_profile

        params = {"profile": [linkedin_profile_url]}

        # Pass the parameters object to the Person Enrichment API
        response = self._client.person.enrichment(**params).json()
        if response["status"] == 404:
            logger.warning(f"Cannot find profiles for {linkedin_profile_url}")
            self.profiles_repository.insert_tried_but_failed_profiles(
                [linkedin_profile_url]
            )
            return
        else:
            self.profiles_repository.insert_fetched_profiles(
                {linkedin_profile_url: response["data"]}
            )
            return response["data"]

    def get_profiles(self, linkedin_profile_urls: list[str]) -> dict[str, dict] | None:
        """
        Retrieves profiles from the People Data Labs API for the given LinkedIn profile URLs.

        Args:
            linkedin_profile_urls (list[str]): A list of LinkedIn profile URLs.

        Returns:
            dict[str, dict] | None: A mapping of profile URLs to profile data, or None if an error occurred.
        """
        linkedin_profile_urls = list(
            map(self.profiles_repository._fix_linkedin_url, linkedin_profile_urls)
        )
        linkedin_profile_urls = self._filter_out_profile_urls(linkedin_profile_urls)
        batches = [
            linkedin_profile_urls[i : i + 100]
            for i in range(0, len(linkedin_profile_urls), 100)
        ]
        profiles = {}
        logger.info(
            f"Retrieving profiles from People Data Labs for {len(linkedin_profile_urls)} URL(s)"
        )
        for i, batch in enumerate(batches, start=1):
            logger.info(
                f"Retrieving profiles from People Data Labs for batch #{i} of {len(batches)}"
            )
            fetched_profiles = self._get_profiles(linkedin_profile_urls=batch)
            if fetched_profiles:
                profiles.update(fetched_profiles)

        self.profiles_repository.create_table_if_not_exists()
        logger.debug("Updating database... Saving fetched profiles")
        self.profiles_repository.insert_fetched_profiles(profiles)
        logger.debug("Updating database... Saving tried but failed profiles")
        self.profiles_repository.insert_tried_but_failed_profiles(
            linkedin_profile_urls=list(self._tried_but_failed)
        )
        # Reset the fetched and tried but failed sets
        self._fetched_profiles = set()
        self._tried_but_failed = set()
        return profiles

    def _get_profiles(self, linkedin_profile_urls: list[str]) -> dict[str, dict] | None:
        linkedin_profile_urls = [
            lpu for lpu in linkedin_profile_urls if lpu not in self._fetched_profiles
        ]
        if not linkedin_profile_urls:
            logger.warning("No new profiles to fetch")
            return None

        linkedin_profile_urls_query = ", ".join(
            f"'{url}'" for url in linkedin_profile_urls
        )
        sql_query = f"SELECT * FROM person WHERE (linkedin_url in ({linkedin_profile_urls_query}))"
        params = {
            "dataset": "resume",
            "sql": sql_query,
            "size": len(linkedin_profile_urls),
        }
        response = self._client.person.search(**params).json()
        if response["status"] == 200:
            logger.debug(
                f"Data retrieved from People Data Labs ({len(response['data'])} records) from sinlge batch of {len(linkedin_profile_urls)} URL(s)"
            )
            profiles = {}
            for d in response["data"]:
                profiles[d["linkedin_url"]] = d
                self._fetched_profiles.add(d["linkedin_url"])

            for url in linkedin_profile_urls:
                if url not in profiles:
                    logger.warning(f"Profile not found for {url}")
                    self._tried_but_failed.add(url)
            return profiles
        elif response["status"] == 404:
            logger.warning(f"Cannot find profiles for {linkedin_profile_urls}")
            self._tried_but_failed.update(linkedin_profile_urls)
        else:
            logger.error(f"Error retrieving profiles from People Data Labs: {response}")
            return None

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


def create_pdl_client(profiles_repository: PersonalDataRepository) -> PDLClient:
    """
    Factory method to create a PDLClient object.

    Args:
        profiles_repository (PersonalDataRepository): The repository for storing profiles.

    Returns:
        PDLClient: The PDLClient object.
    """
    return PDLClient(api_key=PDL_API_KEY, profiles_repository=profiles_repository)
