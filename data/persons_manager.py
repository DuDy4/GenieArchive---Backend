import json
import os
import sys
import asyncio

from linkedin_scrape import HandleLinkedinScrape
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
from data.data_common.utils.persons_utils import (
    create_person_from_pdl_personal_data,
    create_person_from_apollo_personal_data,
)

from data.data_common.dependencies.dependencies import (
    persons_repository,
    personal_data_repository,
    profiles_repository,
    ownerships_repository,
    meetings_repository,
    companies_repository,
)

from data.importers.profile_pictures import get_profile_picture
from data.data_common.repositories.profiles_repository import DEFAULT_PROFILE_PICTURE
from data.data_common.utils.str_utils import get_uuid4
from common.genie_logger import GenieLogger

logger = GenieLogger()
linkedin_scrapper = HandleLinkedinScrape()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

CONSUMER_GROUP = "personmanagerconsumergroup"


class PersonManager(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[
                Topic.NEW_PERSON,
                Topic.PDL_UPDATED_ENRICHED_DATA,
                Topic.APOLLO_UPDATED_ENRICHED_DATA,
                Topic.PDL_FAILED_TO_ENRICH_PERSON,
                Topic.PDL_FAILED_TO_ENRICH_EMAIL,
                Topic.NEW_PROCESSED_PROFILE,
                Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                Topic.PDL_UP_TO_DATE_ENRICHED_DATA,
                Topic.APOLLO_UP_TO_DATE_ENRICHED_DATA,
                Topic.ALREADY_PDL_FAILED_TO_ENRICH_PERSON,
                Topic.NEW_PERSONAL_DATA,
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
            case Topic.ALREADY_PDL_FAILED_TO_ENRICH_PERSON:
                logger.info("Handling already failed to enrich person")
                await self.handle_pdl_already_failed_to_enrich_person(event)
            case Topic.NEW_PERSONAL_DATA:
                logger.info("Handling linkedin scrape")
                await self.handle_linkedin_scrape(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

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
                self.ownerships_repository.save_ownership(person.uuid, tenant_id)
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
        person = create_person_from_pdl_personal_data(person)
        self.persons_repository.save_person(person)
        self.personal_data_repository.update_name_in_personal_data(person.uuid, person.name)
        self.personal_data_repository.update_linkedin_url(person.uuid, person.linkedin)

        if tenant_id:
            has_ownership = self.ownerships_repository.check_ownership(tenant_id, person.uuid)
            if not has_ownership:
                self.ownerships_repository.save_ownership(person.uuid, tenant_id)
        if not person or not personal_data:
            logger.error("No person or personal data found")
            return {"error": "No person or personal data found"}
        data_to_send = {"person": person.to_dict(), "personal_data": personal_data, "tenant_id": tenant_id}
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
            logger.info(f"Person already has apollo personal data: {person.email}")
            person = create_person_from_apollo_personal_data(person)
            self.persons_repository.save_person(person)
            return {"status": "success"}
        event = GenieEvent(
            topic=Topic.APOLLO_NEW_PERSON_TO_ENRICH,
            data={"person": person.to_dict()},
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
            logger.info(f"Person already has apollo personal data: {person.email}")
            self.check_profile_data_from_person(person)
            return {"status": "success"}
        event = GenieEvent(
            Topic.APOLLO_NEW_PERSON_TO_ENRICH,
            data={"person": person.to_dict()},
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
            )
            event.send()
            return {"error": "Failed to get personal data"}

        def update_profile_picture_url(person: PersonDTO):
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
        person = create_person_from_apollo_personal_data(person)

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
                logger.info(f"PDL data is out of date for person: {person}")
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
                person = create_person_from_apollo_personal_data(person)
                logger.debug(f"Person after verification: {person}")
                self.persons_repository.save_person(person)
                event = GenieEvent(
                    topic=Topic.NEW_PERSONAL_DATA,
                    data={"person": person.to_dict(), "personal_data": apollo_personal_data},
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
                person = create_person_from_apollo_personal_data(person)
                logger.debug(f"Person after verification: {person}")
                self.persons_repository.save_person(person)
                event = GenieEvent(
                    topic=Topic.NEW_PERSONAL_DATA,
                    data={"person": person.to_dict(), "personal_data": apollo_personal_data},
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
        logger.info(f"Person: {person_dict},\n Profile: {str(profile)}")

        person = PersonDTO.from_dict(person_dict)
        # person = self.validate_person(person)

        self.persons_repository.save_person(person)

        uuid = person_dict.get("uuid") if person_dict.get("uuid") else get_uuid4()

        # This is a test to get profile picture from social media links
        social_media_links = self.personal_data_repository.get_social_media_links(uuid)
        picture_url = self.personal_data_repository.get_profile_picture_url(uuid)
        profile["picture_url"] = picture_url if picture_url else DEFAULT_PROFILE_PICTURE

        if profile.get("strengths") and isinstance(profile["strengths"], dict):
            logger.warning("Strengths is a dict again...")
            profile["strengths"] = profile.get("strengths")

        logger.info(f"Person: {person}, Profile: {profile}")
        if not person.name:
            logger.error("Got person with no name")
        profile_dto = ProfileDTO.from_dict(
            {
                "uuid": uuid,
                "name": person.name if person.name else "",
                "company": person.company if person.company else "",
                "position": person.position if person.position else profile.get("job_title", ""),
                "strengths": profile.get("strengths", []),
                "hobbies": profile.get("hobbies", []),
                "connections": profile.get("connections", []),
                "get_to_know": profile.get("get_to_know", {}),
                "summary": profile.get("summary", ""),
                "picture_url": profile.get("picture_url", ""),
            }
        )
        profile_details = "\n".join(
            [f"{k}: {len(v) if isinstance(v, list) else v}" for k, v in profile_dto.__dict__.items()]
        )
        logger.debug(f"Profile person: {profile_details}")
        self.profiles_repository.save_profile(profile_dto)
        logger.info(f"About to fetch profile picture for {person.email}")
        if not profile_dto.picture_url or profile_dto.picture_url == DEFAULT_PROFILE_PICTURE:
            profile_dto.picture_url = get_profile_picture(person, social_media_links)
            self.profiles_repository.update_profile_picture(str(profile_dto.uuid), profile_dto.picture_url)
        logger.info(f"Profile picture url: {profile_dto.picture_url}")
        json_profile = profile_dto.to_json()
        event = GenieEvent(
            Topic.FETCH_NEWS,
            data={
                "uuid": uuid,
                "person": person.to_dict(),
                "linkedin_profile": social_media_links.get("linkedin", ""),
            },
        )
        event.send()
        logger.info(f"FETCH_NEWS event sent for person: {person.name}")
        event = GenieEvent(Topic.FINISHED_NEW_PROFILE, json_profile, "public")
        event.send()
        self.persons_repository.remove_last_sent_message(person.uuid)
        logger.info("Saved new processed data to profiles_repository")
        return {"status": "success"}

    async def handle_pdl_already_failed_to_enrich_person(self, event):
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
            logger.info(f"Person already has apollo personal data: {person.email}")
            person = create_person_from_apollo_personal_data(person)
            self.persons_repository.save_person(person)
            return await self.check_profile_data_from_person(person)
        apollo_status = self.personal_data_repository.get_apollo_status(person.uuid)
        if apollo_status == self.personal_data_repository.TRIED_BUT_FAILED:
            logger.info(f"Person already tried to get apollo data: {person.email}")
            return {"status": "failure"}
        elif apollo_status == self.personal_data_repository.FETCHED:
            logger.info(f"Person already has apollo data: {person.email}")
            return {"status": "success"}

        elif not apollo_status:
            logger.info(f"Person has not tried to get apollo data: {person.email}")
            event = GenieEvent(
                Topic.APOLLO_NEW_PERSON_TO_ENRICH,
                data={"person": person.to_dict()},
            )
            event.send()
            logger.info(f"Sent 'apollo' event to the event queue")
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
        tenant_id = event_body.get("tenant_id")
        logger.debug(f"Person: {person}, Tenant: {tenant_id}")
        self.ownerships_repository.save_ownership(person.uuid, tenant_id)

        result = await self.check_profile_data_from_person(person)
        logger.info(f"Result: {result}")
        return {"status": "success"}

    async def handle_linkedin_scrape(self, event):
        logger.info(f"Handling LinkedIn scrape event: {event}")
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)

        linkedin = event_body.get("linkedin")
        uuid = event_body.get("uuid")

        logger.debug(f"Extracted UUID: {uuid}")
        if not uuid:
            logger.error("UUID is missing in the event data")
            return {"error": "UUID is missing"}

        if not linkedin:
            logger.error(f"No LinkedIn URL found in event body, skipping this part: {event_body}")
            return {"error": "No LinkedIn URL found in event body"}

        logger.info(f"Calling LinkedIn scraper for URL: {linkedin}")

        if self.personal_data_repository.should_do_linkedin_posts_lookup(uuid):
            scraped_posts = linkedin_scrapper.fetch_and_process_posts(linkedin)

            if not scraped_posts:
                logger.error(f"No posts found or an error occurred while scraping {linkedin}")
                self.personal_data_repository.update_news_to_db(
                    uuid, None, PersonalDataRepository.TRIED_BUT_FAILED
                )
                return {"error": "No posts found or an error occurred"}

            logger.info(f"Successfully scraped {len(scraped_posts)} posts from LinkedIn URL: {linkedin}")
            news_data_objects = []
            for post in scraped_posts:
                post_json = post.to_dict()
                if post_json.get("image_urls"):
                    post_json["images"] = post_json["image_urls"]
                news_data_objects.append(post_json)
                self.personal_data_repository.update_news_to_db(
                    uuid, post_json, PersonalDataRepository.FETCHED
                )
            return {"posts": news_data_objects}
        else:
            logger.info(f"No need to scrape LinkedIn posts for {uuid} as it was scraped recently or never")
            return {"error": "No need to scrape LinkedIn posts"}

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
            new_person = create_person_from_pdl_personal_data(person)
            person = new_person if new_person else person
        elif apollo_personal_data:
            fetched_personal_data = apollo_personal_data
            new_person = create_person_from_apollo_personal_data(person)
            person = new_person if new_person else person
        logger.debug(f"Person after verification: {person}")
        self.persons_repository.save_person(person)
        profile_exists = self.profiles_repository.exists(person.uuid)
        if not profile_exists:
            logger.warning("Profile does not exist in database")
            event = GenieEvent(
                Topic.NEW_PERSONAL_DATA,
                data={"person": person.to_dict(), "personal_data": fetched_personal_data},
            )
            event.send()
            # Need to implement a call to langsmith, but ensure there is no one in process
            logger.warning(
                "Need to implement a call to langsmith,"
                " but need to think about a way to do it only if there is no langsmith in progress"
            )
            # self.profiles_repository.save_new_profile_from_person(person)
            return {"status": "success"}

        try:
            profile = self.profiles_repository.get_profile_data(person.uuid)
            if not profile.picture_url:
                profile.picture_url = self.personal_data_repository.get_profile_picture(person.uuid)
                logger.info(f"Updated profile picture url: {profile.picture_url}")
            if not profile.strengths and fetched_personal_data:
                logger.info(
                    f"Profile does not have strengths, sending event to langsmith. Email: {person.email}"
                )
                data_to_send = {"person": person.to_dict(), "personal_data": fetched_personal_data}
                GenieEvent(Topic.NEW_PERSONAL_DATA, data_to_send, "public").send()
            return {"status": "success"}
        except ValidationError as e:
            person = create_person_from_apollo_personal_data(person)
            if not person:
                logger.error(f"Failed to create person from apollo personal data: {person}")
                return {"error": "Failed to create person from apollo personal data"}
            self.persons_repository.save_person(person)
            profile.name = person.name
            profile.company = person.company
            profile.position = person.position
            profile.picture_url = self.personal_data_repository.get_profile_picture_url(person.uuid)
            logger.info(f"Profile: {profile}")
            self.profiles_repository.save_profile(profile)
            return {"status": "success"}

    def validate_person(self, person: PersonDTO) -> PersonDTO:
        """
        This function validates the person data:
        1. If the person has no name, position or LinkedIn, it will update it from personal data
        2. It checks the email domain and validate that the company name is correct. If not, it will update it.

        """
        if not person.name or not person.position or not person.linkedin:
            pdl_personal_data = self.personal_data_repository.get_pdl_personal_data(person.uuid)
            if pdl_personal_data:
                person = create_person_from_pdl_personal_data(person)
                logger.info(f"Updated person with pdl data: {person}")

            else:
                apollo_personal_data = self.personal_data_repository.get_apollo_personal_data(person.uuid)
                if apollo_personal_data:
                    person = create_person_from_apollo_personal_data(person)
                    logger.info(f"Updated person with apollo data: {person}")

        # Validate the company name match the domain
        email_domain = (
            person.email.split("@")[1] if isinstance(person.email, str) and "@" in person.email else None
        )
        if email_domain:
            company = self.companies_repository.get_company_from_domain(email_domain)
            if company:
                person.company = company.name

        return person


if __name__ == "__main__":
    person_consumer = PersonManager()
    try:
        asyncio.run(person_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
