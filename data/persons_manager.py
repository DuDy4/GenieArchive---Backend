import json
import os
import sys
import asyncio

import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urlunparse
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from data.data_common.dependencies.dependencies import (
    persons_repository,
    personal_data_repository,
    profiles_repository,
    interactions_repository,
    ownerships_repository,
    meetings_repository,
)

from data.data_common.utils.str_utils import get_uuid4

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
                Topic.NEW_INTERACTION,
                Topic.UPDATED_ENRICHED_DATA,
                Topic.NEW_PROCESSED_PROFILE,
                Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                Topic.UP_TO_DATE_ENRICHED_DATA,
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.persons_repository = persons_repository()
        self.personal_data_repository = personal_data_repository()
        self.profiles_repository = profiles_repository()
        self.interactions_repository = interactions_repository()
        self.ownerships_repository = ownerships_repository()
        self.meetings_repository = meetings_repository()

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
            case Topic.NEW_INTERACTION:
                logger.info("Handling new interaction")
                await self.handle_new_interaction(event)
            case Topic.UPDATED_ENRICHED_DATA:
                logger.info("Handling updated enriched data")
                await self.handle_updated_enriched_data(event)
            case Topic.UP_TO_DATE_ENRICHED_DATA:
                logger.info("Personal data is up to date, Checking for existing profile")
                await self.check_profile_data(event)
            case Topic.NEW_PROCESSED_PROFILE:
                logger.info("Handling new processed data")
                await self.handle_new_processed_profile(event)
            case Topic.NEW_EMAIL_ADDRESS_TO_PROCESS:
                logger.info("Handling email address")
                await self.handle_email_address(event)
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
        event = GenieEvent(Topic.NEW_CONTACT_TO_ENRICH, person_json, "public")
        event.send()
        logger.info("Sent 'pdl' event to the event queue")
        return {"status": "success"}

    async def handle_new_interaction(self, event):
        # Assuming the event body contains a JSON string with the contact data
        interaction_data = event.body_as_str()
        # should gather all of the interactions of this person, and the personal data - then send to langsmith
        self.interactions_repository.save_interaction(interaction_data)
        logger.info("Saved interaction to interactions_repository")

        # Here we should implement whatever we want to do with the interaction data
        # event = GenieEvent(Topic., interaction_data, "public")
        # event.send()
        return {"status": "success"}

    async def handle_updated_enriched_data(self, event):
        # Assuming the event body contains an uuid and a JSON string with the personal data
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        personal_data = event_body.get("personal_data")
        person_dict = event_body.get("person")
        tenant_id = event_body.get("tenant_id")
        if not person_dict:
            logger.error("No person data received in event")
            return {"error": "No person data received in event"}

        if isinstance(person_dict, str):
            person_dict = json.loads(person_dict)
        person: PersonDTO = PersonDTO.from_dict(person_dict)
        personal_data_in_database = self.personal_data_repository.get_personal_data(person.uuid)
        if not personal_data:
            personal_data = personal_data_in_database
            if not personal_data:
                logger.error("No personal data received in event")
                return {"error": "No personal data received in event"}
        if not person.position:
            logger.info("Person has no position, setting it from personal data")
            logger.debug(f"Position: {personal_data.get('job_title', '')}")
            person.position = personal_data.get("job_title", "")
        if not person.name:
            logger.info("Person has no name, setting it from personal data")
            logger.debug(f"Name: {personal_data.get('full_name', '')}")
            person.name = personal_data.get("full_name", "")
        if personal_data_in_database != personal_data:
            logger.error("Personal data in database does not match the one received from event")
        person_in_database = self.persons_repository.find_person_by_email(person.email)
        if not person_in_database:
            logger.error("Person not found in database")
            self.persons_repository.save_person(person)
        else:
            self.personal_data_repository.update_uuid(person.uuid, person_in_database.uuid)
            person.uuid = person_in_database.uuid

        # If person is found in the database, check that it is not an empty person (only email and uuid)
        if person_in_database and (not person_in_database.name or not person_in_database.linkedin):
            self.persons_repository.update(person)
            logger.info("Updated person in database")

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
            company_data = self.personal_data_repository.get_company_from_domain(email_domain)
            if company_data:
                person.company = company_data.name

        self.persons_repository.save_person(person)

        uuid = person_dict.get("uuid") if person_dict.get("uuid") else get_uuid4()

        # This is a test to get profile picture from social media links
        social_media_links = self.personal_data_repository.get_social_media_links(uuid)
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
                "position": person_dict.get("position") if person_dict.get("position") else profile.get("job_title", ""),
                "strengths": profile.get("strengths", []),
                "hobbies": profile.get("hobbies", []),
                "connections": profile.get("connections", []),
                "news": profile.get("news", []),
                "get_to_know": profile.get("get_to_know", {}),
                "summary": profile.get("summary", ""),
                "picture_url": profile.get("picture_url", ""),
            }
        )
        profile_details = "\n".join([f"{k}: {len(v) if isinstance(v, list) else v}" for k, v in profile_person.__dict__.items()])
        logger.debug(f"Profile person: {profile_details}")
        self.profiles_repository.save_profile(profile_person)
        json_profile = profile_person.to_json()
        event = GenieEvent(Topic.FINISHED_NEW_PROFILE, json_profile, "public")
        event.send()
        logger.info("Saved new processed data to profiles_repository")
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
        if person and person.linkedin:
            logger.info(f"Person found: {person}")
            event = GenieEvent(
                Topic.NEW_CONTACT_TO_ENRICH,
                person.to_json(),
                "public",
            )
            event.send()
            self.ownerships_repository.save_ownership(person.uuid, tenant_id)
            logger.info("Sent 'pdl' event to the event queue")
        else:
            logger.info("Person not found in database")
            uuid = person.uuid if person else get_uuid4()
            person = PersonDTO(
                uuid=uuid,
                name="",
                company="",
                email=email,
                linkedin="",
                position="",
                timezone="",
            )
            if not self.persons_repository.exist_email(email):
                self.persons_repository.insert(person)
            self.ownerships_repository.save_ownership(uuid, tenant_id)
            logger.info(f"Saved new person: {person} to persons repository and ownerships repository")
            event = GenieEvent(
                Topic.NEW_EMAIL_ADDRESS_TO_ENRICH,
                json.dumps({"uuid": uuid, "email": email, "tenant_id": tenant_id}),
                "public",
            )
            event.send()

        event = GenieEvent(Topic.NEW_EMAIL_TO_PROCESS_DOMAIN, person.to_json(), "public")
        event.send()
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
        profile = self.profiles_repository.exists(person.uuid)
        if not profile:
            logger.info("Profile does not exist in database")
            await self.handle_updated_enriched_data(event)
            return
        logger.info("Profile exists in database. Skipping profile building")

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
        event = GenieEvent(Topic.NEW_CONTACT_TO_ENRICH, person_json, "public")
        event.send()
        logger.info("Sent 'pdl' event to the event queue")
        return {"status": "success"}


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
