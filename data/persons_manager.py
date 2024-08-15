import json
import os
import sys
import asyncio

import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse
from pydantic import ValidationError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.repositories.personal_data_repository import PersonalDataRepository
from data.data_common.repositories.profiles_repository import ProfilesRepository
from data.data_common.repositories.ownerships_repository import OwnershipsRepository
from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.repositories.companies_repository import CompaniesRepository

from data.data_common.dependencies.dependencies import (
    persons_repository,
    personal_data_repository,
    profiles_repository,
    ownerships_repository,
    meetings_repository,
    companies_repository,
)

from data.data_common.utils.str_utils import get_uuid4
from common.genie_logger import GenieLogger

logger = GenieLogger()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

CONSUMER_GROUP = "personmanagerconsumergroup"


class PersonManager(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[
                Topic.NEW_CONTACT,
                Topic.NEW_PERSON,
                Topic.PDL_UPDATED_ENRICHED_DATA,
                Topic.APOLLO_UPDATED_ENRICHED_DATA,
                Topic.PDL_FAILED_TO_ENRICH_PERSON,
                Topic.PDL_FAILED_TO_ENRICH_EMAIL,
                Topic.NEW_PROCESSED_PROFILE,
                Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                Topic.PDL_UP_TO_DATE_ENRICHED_DATA,
                Topic.APOLLO_UP_TO_DATE_ENRICHED_DATA,
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.persons_repository = persons_repository()
        self.personal_data_repository = personal_data_repository()
        self.profiles_repository = profiles_repository()
        self.ownerships_repository = ownerships_repository()
        self.meetings_repository = meetings_repository()
        self.companies_repository = companies_repository()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        # Should use Topic class

        match topic:
            case Topic.NEW_CONTACT:
                logger.info("Handling new salesforce contact")
                await self.handle_new_salesforce_contact(event)
            case Topic.NEW_PERSON:
                logger.info("Handling new person")
                await self.handle_new_person(event)
            case Topic.NEW_EMAIL_ADDRESS_TO_PROCESS:
                logger.info("Handling email address")
                await self.handle_email_address(event)
            case Topic.PDL_UPDATED_ENRICHED_DATA:
                logger.info("Handling updated enriched data")
                await self.handle_pdl_updated_enriched_data(event)
            case Topic.APOLLO_UPDATED_ENRICHED_DATA:
                logger.info("Handling updated enriched data")
                await self.handle_apollo_updated_enriched_data(event)
            case Topic.PDL_UP_TO_DATE_ENRICHED_DATA:
                logger.info("Personal data is up to date, Checking for existing profile")
                await self.check_profile_data(event)
            case Topic.APOLLO_UP_TO_DATE_ENRICHED_DATA:
                logger.info("Personal data is up to date, Checking for existing profile")
                await self.check_profile_data(event)
            case Topic.PDL_FAILED_TO_ENRICH_PERSON:
                logger.info("Handling failed attempt to enrich person")
                await self.handle_pdl_failed_to_enrich_person(event)
            case Topic.PDL_FAILED_TO_ENRICH_EMAIL:
                logger.info("Handling failed attempt to enrich email")
                await self.handle_pdl_failed_to_enrich_email(event)
            case Topic.NEW_PROCESSED_PROFILE:
                logger.info("Handling new processed data")
                await self.handle_new_processed_profile(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_new_salesforce_contact(self, event):
        # Assuming the event body contains a JSON string with the contact data
        logger.info("Handling new salesforce contact")
        contact_data_str = event.body_as_str()
        contact_data = json.loads(contact_data_str)
        if isinstance(contact_data, str):
            contact_data = json.loads(contact_data)
        tenant_id = contact_data.get("tenant_id")
        new_person = PersonDTO.from_dict(contact_data)
        uuid = self.persons_repository.save_person(new_person)

        new_person.uuid = uuid
        self.ownerships_repository.save_ownership(new_person.uuid, tenant_id)

        # Send "pdl" event to the event queue
        person_json = new_person.to_json()
        event = GenieEvent(Topic.PDL_NEW_PERSON_TO_ENRICH, person_json, "public")
        event.send()
        logger.info("Sent 'pdl' event to the event queue")
        return {"status": "success"}

    async def handle_new_person(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person_dict = event_body.get("person")
        if isinstance(person_dict, str):
            person_dict = json.loads(person_dict)
        logger.debug(f"Person: {person_dict}")
        person = PersonDTO.from_dict(person_dict)
        person.uuid = self.persons_repository.save_person(person)

        person_json = person.to_json()
        event = GenieEvent(Topic.PDL_NEW_PERSON_TO_ENRICH, person_json, "public")
        event.send()
        logger.info("Sent 'pdl' event to the event queue")
        return {"status": "success"}

    async def handle_email_address(self, event):
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
        tenant_id = event_body.get("tenant_id")
        logger.info(f"Got data from event: Tenant: {tenant_id}, Email: {email}")

        person = self.persons_repository.find_person_by_email(email)
        # If person is found in the database,
        # check that it is not an empty person (only email and uuid) and handle it accordingly
        if person:
            pdl_personal_data = self.personal_data_repository.get_pdl_personal_data(person.uuid)
            apollo_personal_data = self.personal_data_repository.get_apollo_personal_data(person.uuid)
            if pdl_personal_data or apollo_personal_data:
                logger.info(f"Person already has pdl personal data: {person}")
                check_profile = await self.check_profile_data_from_person(person)
                return {"status": "success"}

            logger.info(f"Person found: {person}")
            event = GenieEvent(
                Topic.PDL_NEW_PERSON_TO_ENRICH,
                person.to_json(),
                "public",
            )
            event.send()
            self.ownerships_repository.save_ownership(person.uuid, tenant_id)
            logger.info("Sent 'pdl' event to the event queue")
            return {"status": "success"}
        else:
            logger.info("Person not found in database")

            # In case only person was deleted from database
            person_uuid = self.personal_data_repository.get_personal_uuid_by_email(email)
            if not person_uuid:
                person_uuid = get_uuid4()
            person = PersonDTO(
                uuid=person_uuid,
                name="",
                company="",
                email=email,
                linkedin="",
                position="",
                timezone="",
            )
            if not self.persons_repository.exist_email(email):
                self.persons_repository.insert(person)
            self.ownerships_repository.save_ownership(person_uuid, tenant_id)
            logger.info(f"Saved new person: {person} to persons repository and ownerships repository")
            event = GenieEvent(
                Topic.PDL_NEW_EMAIL_ADDRESS_TO_ENRICH,
                json.dumps({"uuid": person_uuid, "email": email, "tenant_id": tenant_id}),
                "public",
            )
            event.send()
            return {"status": "success"}

        event = GenieEvent(Topic.NEW_EMAIL_TO_PROCESS_DOMAIN, person.to_json(), "public")
        event.send()
        return {"status": "success"}

    async def handle_pdl_updated_enriched_data(self, event):
        # Assuming the event body contains an uuid and a JSON string with the personal data
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person_dict = event_body.get("person")
        tenant_id = event_body.get("tenant_id")
        if not person_dict:
            logger.error("No person data received in event")
            return {"error": "No person data received in event"}

        if isinstance(person_dict, str):
            person_dict = json.loads(person_dict)
        person: PersonDTO = PersonDTO.from_dict(person_dict)
        personal_data = self.personal_data_repository.get_pdl_personal_data(person.uuid)
        if not personal_data:
            logger.error("No personal data received in event")
            return {"error": "No personal data received in event"}
        person = self.verify_person_with_pdl_data(person)

        if tenant_id:
            has_ownership = self.ownerships_repository.check_ownership(tenant_id, person.uuid)
            if not has_ownership:
                self.ownerships_repository.save_ownership(person.uuid, tenant_id)
        if not person or not personal_data:
            logger.error("No person or personal data found")
            return {"error": "No person or personal data found"}
        data_to_send = {"person": person.to_dict(), "personal_data": personal_data}
        # Send "new_personal_data" event to the event queue
        event = GenieEvent(Topic.NEW_PERSONAL_DATA, data_to_send, "public")
        event.send()
        logger.info("Sent 'new_personal_data' event to the event queue")
        return {"status": "success"}

    async def handle_pdl_failed_to_enrich_person(self, event):
        """
        This function checks if already tried to get apollo personal data.
        If not, it will send apollo an event to get the apollo data.
        """
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person_dict = event_body.get("person")
        if isinstance(person_dict, str):
            person_dict = json.loads(person_dict)
        person = PersonDTO.from_dict(person_dict)
        if not person:
            logger.error(f"Person not found in event body: {event_body}")
            return {"error": "Person not found in event body"}
        apollo_personal_data = self.personal_data_repository.get_apollo_personal_data(person.uuid)
        if apollo_personal_data:
            logger.info(f"Person already has apollo personal data: {person}")
            person = self.verify_person_with_apollo_data(person)
            self.persons_repository.save_person(person)
            return {"status": "success"}
        event = GenieEvent(
            Topic.APOLLO_NEW_PERSON_TO_ENRICH,
            data={"person": person.to_dict()},
            scope="public",
        )
        event.send()
        logger.info(f"Sent 'apollo' event to the event queue")
        return {"status": "success"}

    async def handle_pdl_failed_to_enrich_email(self, event):
        """
        This function checks if already tried to get apollo personal data.
        If not, it will send apollo an event to get the apollo data.
        """
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        email = event_body.get("email")
        if not email:
            logger.error(f"Email not found in event body: {event_body}")
            return {"error": "Email not found in event body"}
        person = self.persons_repository.find_person_by_email(email)
        if not person:
            logger.warning(f"Person not found for email: {email}")
        if not person.linkedin:
            logger.warning(f"Person has no linkedin: {person}")
        apollo_personal_data = self.personal_data_repository.get_apollo_personal_data(person.uuid)
        if apollo_personal_data:
            logger.info(f"Person already has apollo personal data: {person}")
            return {"status": "success"}
        event = GenieEvent(
            Topic.APOLLO_NEW_PERSON_TO_ENRICH,
            data={"person": person.to_dict()},
            scope="public",
        )
        event.send()
        logger.info(f"Sent 'apollo' event to the event queue")
        return {"status": "success"}

    async def handle_apollo_updated_enriched_data(self, event):
        """
        Handle the event when apollo has updated enriched data

        This is the main function that handles the event when apollo has updated enriched data.
        It will check the pdl last updated timestamp and the apollo last updated timestamp.
        If the apollo timestamp is newer than the pdl timestamp, it will send a new personal data event - so the
        pdl data will be .


        :param event: The event to handle
        :return: A dictionary with the status of the operation

        """

        # Assuming the event body contains an uuid and a JSON string with the personal data
        def send_new_person_event(person):
            event = GenieEvent(
                topic=Topic.PDL_NEW_PERSON_TO_ENRICH,
                data={"person": person.to_dict()},
                scope="public",
            )
            event.send()
            return {"error": "Failed to get personal data"}

        def update_profile_picture_url(person):
            profile = self.profiles_repository.get_profile_data(person.uuid)
            if not profile:
                logger.warning(f"Profile does not exist in database for person: {person}")
                return {"error": "Profile does not exist in database"}
            if not profile.picture_url:
                profile.picture_url = self.personal_data_repository.get_profile_picture(person.uuid)
                self.profiles_repository.save_profile(profile)
                logger.info(f"Saved profile with picture url: {profile.picture_url}")
            return {"status": "success"}

        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person_dict = event_body.get("person")
        if not person_dict:
            logger.error("No person data received in event")
            return {"error": "No person data received in event"}

        if isinstance(person_dict, str):
            person_dict = json.loads(person_dict)
        person: PersonDTO = PersonDTO.from_dict(person_dict)
        person = self.verify_person_with_apollo_data(person)

        pdl_last_updated = self.personal_data_repository.get_pdl_last_updated(person.uuid)
        apollo_last_updated = self.personal_data_repository.get_apollo_last_updated(person.uuid)
        if not pdl_last_updated:
            logger.error(f"Failed to get pdl last updated for person: {person}")
            send_new_person_event(person)
            return {"error": "No last updated timestamp in pdl database"}
        if not apollo_last_updated:
            logger.error(f"Failed to get apollo last updated for person: {person}")
            send_new_person_event(person)
            return {"error": "No last updated timestamp in apollo database"}
        if apollo_last_updated > pdl_last_updated:
            pdl_status = self.personal_data_repository.get_pdl_status(person.uuid)
            if pdl_status == self.personal_data_repository.FETCHED:
                update_profile_picture_url(person)
                return {"status": "success"}
            else:
                logger.info(f"PDL data is up to date for person: {person}")
                if person.linkedin:
                    send_new_person_event(person)
                    return {"status": "success"}
                else:
                    logger.info(f"Person has no linkedin, no need to try PDL again")
                apollo_personal_data = self.personal_data_repository.get_apollo_personal_data(person.uuid)
                logger.debug(f"Apollo personal data: {str(apollo_personal_data)[:300]}")
                if not apollo_personal_data:
                    logger.error(f"Failed to get personal data for person: {person}")
                    return {"error": "Failed to get personal data"}
                logger.debug(f"Person before verification: {person}")
                person = self.verify_person_with_apollo_data(person)
                logger.debug(f"Person after verification: {person}")
                self.persons_repository.save_person(person)
                event = GenieEvent(
                    topic=Topic.NEW_PERSONAL_DATA,
                    data={"person": person.to_dict(), "personal_data": apollo_personal_data},
                    scope="public",
                )
                event.send()
                return {"status": "success"}
            logger.warning(f"Should not have reached this point. Expect missing data or unexpected behavior")
            return {"status": "warning"}
        else:
            pdl_status = self.personal_data_repository.get_pdl_status(person.uuid)
            if pdl_status == self.personal_data_repository.TRIED_BUT_FAILED:
                apollo_personal_data = self.personal_data_repository.get_apollo_personal_data(person.uuid)
                if not apollo_personal_data:
                    logger.error(f"Failed to get personal data for person: {person}")
                    return {"error": "Failed to get personal data"}
                logger.debug(f"Person before verification: {person}")
                person = self.verify_person_with_apollo_data(person)
                logger.debug(f"Person after verification: {person}")
                self.persons_repository.save_person(person)
                event = GenieEvent(
                    topic=Topic.NEW_PERSONAL_DATA,
                    data={"person": person.to_dict(), "personal_data": apollo_personal_data},
                    scope="public",
                )
                event.send()
                logger.info("Sent 'new_personal_data' event to the event queue")
                return {"status": "success"}
            else:
                update_profile_picture_url(person)
                logger.info(f"PDL data is up to date for person: {person}")
                return {"status": "success"}
            logger.warning(f"Should not have reached this point. Expect missing data or unexpected behavior")
            return {"status": "warning"}
        logger.warning(f"Should not have reached this point. Expect missing data or unexpected behavior")
        return {"status": "warning"}

    async def handle_new_processed_profile(self, event):
        # Assuming the event body contains a JSON string with the processed data
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person_dict = event_body.get("person")
        if isinstance(person_dict, str):
            person_dict = json.loads(person_dict)
        profile = event_body.get("profile")
        if isinstance(profile, str):
            profile = json.loads(profile)
        logger.debug(f"Person: {person_dict},\n Profile: {str(profile)}")

        person = PersonDTO.from_dict(person_dict)
        if not person.company:
            logger.info("Person has no company, setting it from profile")
            email_domain = person.email.split("@")[1]
            company_data = self.companies_repository.get_company_from_domain(email_domain)
            if company_data:
                person.company = company_data.name

        self.persons_repository.save_person(person)

        uuid = person_dict.get("uuid") if person_dict.get("uuid") else get_uuid4()

        # This is a test to get profile picture from social media links
        social_media_links = self.personal_data_repository.get_social_media_links(uuid)
        picture_url = self.personal_data_repository.get_profile_picture_url(uuid)
        profile["picture_url"] = picture_url
        logger.debug(f"Picture url: {picture_url}")
        if not picture_url:
            picture_urls = get_picture_from_social_links_list(social_media_links)
            logger.debug(f"Picture urls: {picture_urls}")

            if picture_urls:
                profile["picture_url"] = picture_urls[0]

        if not profile.get("picture_url"):
            profile["picture_url"] = "https://monomousumi.com/wp-content/uploads/anonymous-user-8.png"

        if profile.get("strengths") and isinstance(profile["strengths"], dict):
            logger.warning("Strengths is a dict again...")
            profile["strengths"] = profile.get("strengths")

        profile_person = ProfileDTO.from_dict(
            {
                "uuid": uuid,
                "name": person_dict.get("name"),
                "company": person_dict.get("company"),
                "position": person_dict.get("position")
                if person_dict.get("position")
                else profile.get("job_title", ""),
                "strengths": profile.get("strengths", []),
                "hobbies": profile.get("hobbies", []),
                "connections": profile.get("connections", []),
                "get_to_know": profile.get("get_to_know", {}),
                "summary": profile.get("summary", ""),
                "picture_url": profile.get("picture_url", ""),
            }
        )
        profile_details = "\n".join(
            [f"{k}: {len(v) if isinstance(v, list) else v}" for k, v in profile_person.__dict__.items()]
        )
        logger.debug(f"Profile person: {profile_details}")
        self.profiles_repository.save_profile(profile_person)
        json_profile = profile_person.to_json()
        event = GenieEvent(Topic.FINISHED_NEW_PROFILE, json_profile, "public")
        event.send()
        logger.info("Saved new processed data to profiles_repository")
        return {"status": "success"}

    async def check_profile_data(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person_dict = event_body.get("person")
        if isinstance(person_dict, str):
            person_dict = json.loads(person_dict)
        person = PersonDTO.from_dict(person_dict)
        result = await self.check_profile_data_from_person(person)
        logger.info(f"Result: {result}")
        return {"status": "success"}

    async def check_profile_data_from_person(self, person: PersonDTO):
        if not person:
            logger.error(f"Invalid person data: {person}")
            return {"error": "Invalid person data"}
        logger.debug(f"Person: {person}")
        pdl_personal_data = self.personal_data_repository.get_pdl_personal_data(person.uuid)
        apollo_personal_data = self.personal_data_repository.get_apollo_personal_data(person.uuid)
        fetched_personal_data = None
        if pdl_personal_data:
            fetched_personal_data = pdl_personal_data
            person = self.verify_person_with_pdl_data(person)
        elif apollo_personal_data:
            fetched_personal_data = apollo_personal_data
            person = self.verify_person_with_apollo_data(person)
        logger.debug(f"Person after verification: {person}")
        self.persons_repository.save_person(person)
        profile_exists = self.profiles_repository.exists(person.uuid)
        if not profile_exists:
            logger.warning("Profile does not exist in database")
            # Need to implement a call to langsmith, but ensure there is no one in process
            logger.warning(
                "Need to implement a call to langsmith,"
                " but need to think about a way to do it only if there is no langsmith in progress"
            )
            self.profiles_repository.save_new_profile_from_person(person)
        try:
            profile = self.profiles_repository.get_profile_data(person.uuid)
            if not profile.picture_url:
                profile.picture_url = self.personal_data_repository.get_profile_picture(person.uuid)
                logger.info(f"Updated profile picture url: {profile.picture_url}")
            if not profile.strengths and fetched_personal_data:
                logger.info(f"Profile does not have strengths, sending event to langsmith. Email: {person.email}")
                data_to_send = {"person": person.to_dict(), "personal_data": fetched_personal_data}    
                GenieEvent(Topic.NEW_PERSONAL_DATA, data_to_send, "public").send()
            return {"status": "success"}
        except ValidationError as e:
            person = self.verify_person_with_apollo_data(person)
            logger.error(f"Profile data is invalid: {e}")
            profile.name = person.name
            profile.company = person.company
            profile.position = person.position
            profile.picture_url = self.personal_data_repository.get_profile_picture(person.uuid)
            logger.info(f"Profile: {profile}")
            self.profiles_repository.save_profile(profile)
            return {"status": "success"}

        logger.info("Profile exists in database. Skipping profile building")

    def verify_person_with_pdl_data(self, person: PersonDTO):
        person_in_database = self.persons_repository.find_person_by_email(person.email)
        personal_data = self.personal_data_repository.get_pdl_personal_data(person.uuid)
        if person.name != person_in_database.name:
            person.name = personal_data.get("full_name", "")
        if person.position != person_in_database.position:
            person.position = personal_data.get("job_title", "")
        if person.company != person_in_database.company:
            email_domain = person.email.split("@")[1]
            company = self.companies_repository.get_company_from_domain(email_domain)
            person.company = company.name if company else person.company
        return person

    def verify_person_with_apollo_data(self, person: PersonDTO):
        person_in_database = self.persons_repository.find_person_by_email(person.email)
        personal_data = self.personal_data_repository.get_apollo_personal_data(person.uuid)
        if person.name != person_in_database.name:
            person.name = personal_data.get("name", "")
        if person.position != person_in_database.position:
            person.position = personal_data.get("title", "")
        if person.linkedin != person_in_database.linkedin or not person.linkedin:
            person.linkedin = personal_data.get("linkedin_url", "")
        if person.company != person_in_database.company:
            email_domain = person.email.split("@")[1]
            company = self.companies_repository.get_company_from_domain(email_domain)
            person.company = company.name if company else person.company
        return person


def get_profile_picture(url, platform):
    headers = {"User-Agent": "Mozilla/5.0"}

    # Ensure the URL has the scheme
    parsed_url = urlparse(url)
    if not parsed_url.scheme:
        if parsed_url.netloc:
            url = urlunparse(("https", parsed_url.netloc, parsed_url.path, "", "", ""))
        else:
            url = urlunparse(("https", parsed_url.path, "", "", "", ""))

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        match platform.lower():
            case "linkedin":
                profile_picture = soup.find("img", {"class": "profile-photo"})
            case "facebook":
                profile_picture = soup.find("img", {"class": "profilePic"})
            case "twitter":
                profile_picture = soup.find("img", {"class": "ProfileAvatar-image"})
            case _:
                return None
        if profile_picture:
            return profile_picture["src"]

    return None


def get_picture_from_social_links_list(links: list[dict]):
    for entry in links:
        url = entry.get("url")
        network = entry.get("network")
        if url and network:
            picture_url = get_profile_picture(url, network)
            if picture_url:
                return picture_url
    return None


if __name__ == "__main__":
    person_consumer = PersonManager()
    try:
        asyncio.run(person_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
