import json
import os
import sys
import asyncio

from data.api_services.linkedin_scrape import HandleLinkedinScrape
from pydantic import ValidationError

from data.data_common.data_transfer_objects.news_data_dto import NewsData, SocialMediaPost
from data.data_common.data_transfer_objects.status_dto import StatusDTO, StatusEnum
from data.data_common.repositories.personal_data_repository import PersonalDataRepository
from data.data_common.repositories.user_profiles_repository import UserProfilesRepository

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.data_transfer_objects.person_dto import PersonDTO, PersonStatus
from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from data.internal_services.azure_storage_picture_uploader import AzureProfilePictureUploader
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
    tenant_profiles_repository,
)

from data.importers.profile_pictures import get_profile_picture
from data.data_common.repositories.profiles_repository import DEFAULT_PROFILE_PICTURE
from data.data_common.utils.str_utils import get_uuid4
from common.genie_logger import GenieLogger, user_id

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
                Topic.APOLLO_FAILED_TO_ENRICH_PERSON,
                Topic.APOLLO_FAILED_TO_ENRICH_EMAIL,
                Topic.NEW_PROCESSED_PROFILE,
                Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                Topic.PDL_UP_TO_DATE_ENRICHED_DATA,
                Topic.APOLLO_UP_TO_DATE_ENRICHED_DATA,
                Topic.ALREADY_PDL_FAILED_TO_ENRICH_PERSON,
                Topic.NEW_PERSONAL_DATA,
                Topic.FINISHED_NEW_PROFILE,
                Topic.PROFILE_ERROR,
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.persons_repository = persons_repository()
        self.personal_data_repository = personal_data_repository()
        self.profiles_repository = profiles_repository()
        self.ownerships_repository = ownerships_repository()
        self.meetings_repository = meetings_repository()
        self.companies_repository = companies_repository()
        # self.tenant_profiles_repository = tenant_profiles_repository()
        self.user_profiles_repository = UserProfilesRepository()
        self.azure_profile_picture_uploader = AzureProfilePictureUploader()

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
            case Topic.APOLLO_FAILED_TO_ENRICH_PERSON:
                logger.info("Handling failed attempt to enrich person")
                await self.handle_apollo_failed_to_enrich_person(event)
            case Topic.PDL_FAILED_TO_ENRICH_EMAIL:
                logger.info("Handling failed attempt to enrich email")
                await self.handle_pdl_failed_to_enrich_email(event)
            case Topic.APOLLO_FAILED_TO_ENRICH_EMAIL:
                logger.info("Handling failed attempt to enrich email")
                await self.handle_apollo_failed_to_enrich_email(event)
            case Topic.NEW_PROCESSED_PROFILE:
                logger.info("Handling new processed data")
                await self.handle_new_processed_profile(event)
            case Topic.ALREADY_PDL_FAILED_TO_ENRICH_PERSON:
                logger.info("Handling already failed to enrich person")
                await self.handle_pdl_already_failed_to_enrich_person(event)
            case Topic.NEW_PERSONAL_DATA:
                logger.info("Handling linkedin scrape")
                await self.handle_linkedin_scrape(event)
            case Topic.FINISHED_NEW_PROFILE:
                logger.info("Handling finished new profile")
                await self.handle_finished_new_profile(event)
            case Topic.PROFILE_ERROR:
                logger.info("Handling profile error")
                await self.handle_profile_error(event)
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
        person = PersonDTO.from_dict(person_dict)
        person.uuid = self.persons_repository.save_person(person)
        logger.bind_y_context()
        event = GenieEvent(Topic.PDL_NEW_PERSON_TO_ENRICH, {"person": person.to_dict()})
        event.send()
        return {"status": "success"}

    async def handle_email_address(self, event):
        event_body = event.body_as_str()
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            try:
                event_body = json.loads(event_body)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON: {event_body}")
                return {"error": "Invalid JSON"}
        email = event_body.get("email")
        tenant_id = event_body.get("tenant_id") or logger.get_tenant_id()
        #
        # checked_tenant_id = logger.get_tenant_id()
        # if tenant_id != checked_tenant_id:
        #     logger.error(f"Tenant id mismatch: {tenant_id} != {checked_tenant_id}")
        #     event = GenieEvent(
        #         Topic.BUG_IN_TENANT_ID,
        #         {"email": email, "tenant_id": tenant_id},
        #         "public",
        #     )
        #     event.send()
        user_id = event_body.get("user_id") or logger.get_user_id()
        logger.bind_y_context()


        person = self.persons_repository.find_person_by_email(email)
        # If person is found in the database,
        # check that it is not an empty person (only email and uuid) and handle it accordingly
        if person:
            pdl_personal_data = self.personal_data_repository.get_pdl_personal_data(person.uuid)
            apollo_personal_data = self.personal_data_repository.get_apollo_personal_data(person.uuid)
            if pdl_personal_data or apollo_personal_data:
                logger.info(f"Person already has personal data: {person}")
                self.ownerships_repository.save_ownership(uuid=person.uuid, user_id=user_id, tenant_id=tenant_id)
                check_profile = await self.check_profile_data_from_person(person)
                return {"status": "success"}

            logger.info(f"Person found: {person}")
            event = GenieEvent(
                Topic.PDL_NEW_PERSON_TO_ENRICH,
                {"person": person.to_dict(), "tenant_id": tenant_id, "user_id": user_id},
                "public",
            )
            event.send()
            self.ownerships_repository.save_ownership(uuid=person.uuid, user_id=user_id, tenant_id=tenant_id)
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
            self.ownerships_repository.save_ownership(uuid=person_uuid, user_id=user_id, tenant_id=tenant_id)
            logger.info(f"Saved new person: {person} to persons repository and ownerships repository")
            event = GenieEvent(
                topic=Topic.PDL_NEW_EMAIL_ADDRESS_TO_ENRICH,
                data={"uuid": person_uuid, "email": email, "tenant_id": tenant_id, "user_id": user_id},
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
        tenant_id = event_body.get("tenant_id") or logger.get_tenant_id()
        user_id = event_body.get("user_id") or logger.get_user_id()
        if not person_dict:
            logger.error("No person data received in event")
            raise Exception("Update pdl personal data failed: No person data received in event")

        if isinstance(person_dict, str):
            person_dict = json.loads(person_dict)
        person: PersonDTO = PersonDTO.from_dict(person_dict)
        personal_data = self.personal_data_repository.get_pdl_personal_data(person.uuid)
        if not personal_data:
            logger.error("No personal data received in event")
            raise Exception("Update pdl personal data failed: No personal data found in database")
        person = create_person_from_pdl_personal_data(person)
        if not person:
            logger.error("Failed to create person from pdl personal data")
            return {"error": "Failed to create person from pdl personal data"}
        self.persons_repository.save_person(person)
        self.personal_data_repository.update_name_in_personal_data(person.uuid, person.name)
        self.personal_data_repository.update_linkedin_url(person.uuid, person.linkedin)

        if user_id:
            has_ownership = self.ownerships_repository.check_ownership(user_id, person.uuid)
            if not has_ownership:
                self.ownerships_repository.save_ownership(person.uuid, user_id, tenant_id)
        event = GenieEvent(
            Topic.APOLLO_NEW_PERSON_TO_ENRICH,
            data={"person": person.to_dict()},
        )
        event.send()
        data_to_send = {"person": person.to_dict(), "personal_data": personal_data, "user_id": user_id}
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
            raise Exception("Failed to enrich person: Person not found in event body")
        apollo_status = self.personal_data_repository.get_apollo_status(person.uuid)
        if not apollo_status:
            event = GenieEvent(
                Topic.APOLLO_NEW_PERSON_TO_ENRICH,
                {"person": person.to_dict()},
            )
            event.send()
            logger.info("Sent 'apollo' event to the event queue")
            return {"status": "success"}
        if apollo_status == self.personal_data_repository.FETCHED:
            logger.info(f"Person already has apollo personal data: {person.email}")
            return {"status": "success"}
        elif apollo_status == self.personal_data_repository.TRIED_BUT_FAILED:
            logger.info(f"Person already tried to get apollo personal data: {person.email}")
            event = GenieEvent(
                Topic.FAILED_TO_ENRICH_PERSON,
                {"person": person.to_dict()},
            )
            event.send()
            self.persons_repository.update_status(person.uuid, PersonStatus.FAILED)
            logger.info("Sent 'failed_to_find_personal_data' event to the event queue")
            return {"status": "failed"}
        return {"status": "failed"}

    async def handle_apollo_failed_to_enrich_person(self, event):
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
            raise Exception("Failed to enrich person: Person not found in event body")
        pdl_status = self.personal_data_repository.get_pdl_status(person.uuid)
        if not pdl_status:
            event = GenieEvent(
                Topic.PDL_NEW_PERSON_TO_ENRICH,
                {"person": person.to_dict()},
            )
            event.send()
            logger.info("Sent 'pdl' event to the event queue")
            return {"status": "success"}
        if pdl_status == self.personal_data_repository.FETCHED:
            logger.info(f"Person already has pdl personal data: {person.email}")
            return {"status": "success"}
        elif pdl_status == self.personal_data_repository.TRIED_BUT_FAILED:
            logger.info(f"Person already tried to get pdl personal data: {person.email}")
            event = GenieEvent(
                Topic.FAILED_TO_ENRICH_PERSON,
                {"person": person.to_dict()},
            )
            event.send()
            self.persons_repository.update_status(person.uuid, PersonStatus.FAILED)
            logger.info("Sent 'failed_to_find_personal_data' event to the event queue")
            return {"status": "failed"}
        return {"status": "failed"}

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
            raise Exception("Failed to enrich email: Email not found in event body")
        person = self.persons_repository.find_person_by_email(email)
        if not person:
            logger.warning(f"Person not found for email: {email}")
        if not person.linkedin:
            logger.warning(f"Person has no linkedin: {person}")
        apollo_status = self.personal_data_repository.get_apollo_status(person.uuid)
        if not apollo_status:
            event = GenieEvent(
                Topic.APOLLO_NEW_EMAIL_ADDRESS_TO_ENRICH,
                {"email": email},
            )
            event.send()
            logger.info("Sent 'apollo' event to the event queue")
            return {"status": "success"}
        if apollo_status == self.personal_data_repository.FETCHED:
            logger.info(f"Person already has apollo personal data: {person.email}")
            return {"status": "success"}
        elif apollo_status == self.personal_data_repository.TRIED_BUT_FAILED:
            logger.info(f"Person already tried to get apollo personal data: {person.email}")
            event = GenieEvent(
                Topic.FAILED_TO_ENRICH_EMAIL,
                {"person": person.to_dict()},
            )
            event.send()
            self.persons_repository.update_status(person.uuid, PersonStatus.FAILED)
            logger.info("Sent 'failed_to_find_personal_data' event to the event queue")
            return {"status": "failed"}
        return {"status": "failed"}

    async def handle_apollo_failed_to_enrich_email(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        email = event_body.get("email")
        if not email:
            logger.error(f"Email not found in event body: {event_body}")
            raise Exception("Failed to enrich email: Email not found in event body")
        person = self.persons_repository.find_person_by_email(email)
        if not person:
            logger.warning(f"Person not found for email: {email}")
        if not person.linkedin:
            logger.warning(f"Person has no linkedin: {person}")
        pdl_status = self.personal_data_repository.get_pdl_status(person.uuid)
        if not pdl_status:
            event = GenieEvent(
                Topic.PDL_NEW_EMAIL_ADDRESS_TO_ENRICH,
                {"email": email},
            )
            event.send()
            logger.info("Sent 'pdl' event to the event queue")
            return {"status": "success"}
        if pdl_status == self.personal_data_repository.FETCHED:
            logger.info(f"Person already has pdl personal data: {person.email}")
            return {"status": "success"}
        elif pdl_status == self.personal_data_repository.TRIED_BUT_FAILED:
            logger.info(f"Person already tried to get pdl personal data: {person.email}")
            event = GenieEvent(
                Topic.FAILED_TO_ENRICH_EMAIL,
                {"person": person.to_dict()},
            )
            event.send()
            self.persons_repository.update_status(person.uuid, PersonStatus.FAILED)
            logger.info("Sent 'failed_to_find_personal_data' event to the event queue")
            return {"status": "failed"}
        return {"status": "failed"}

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
                profile.picture_url = self.personal_data_repository.get_profile_picture_url(person.uuid)
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
            raise Exception("Update apollo personal data failed: No person data received in event")
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
                if not apollo_personal_data:
                    logger.error(f"Failed to get personal data for person: {person}")
                    raise Exception("Failed update apollo personal data: Failed to get personal data from database")
                person = create_person_from_apollo_personal_data(person)
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
                    raise Exception("Failed update apollo personal data: Failed to get personal data from database")
                person = create_person_from_apollo_personal_data(person)
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
            raise Exception("Should not have reached this point. Expect missing data or unexpected behavior")
        logger.warning(f"Should not have reached this point. Expect missing data or unexpected behavior")
        raise Exception("Should not have reached this point. Expect missing data or unexpected behavior")

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

        self.persons_repository.save_person(person)

        uuid = person_dict.get("uuid") if person_dict.get("uuid") else get_uuid4()

        # This is a test to get profile picture from social media links
        social_media_links = self.personal_data_repository.get_social_media_links(uuid)
        picture_url = self.profiles_repository.get_profile_picture(uuid)
        if not picture_url:
            picture_url = self.personal_data_repository.get_profile_picture_url(uuid)
        profile["picture_url"] = picture_url if picture_url else DEFAULT_PROFILE_PICTURE

        if profile.get("strengths") and isinstance(profile["strengths"], str):
            logger.warning("Strengths is a string again...")
            profile["strengths"] = json.loads(profile["strengths"])

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
                "work_history_summary": profile.get("work_history_summary", ""),
            }
        )
        profile_details = "\n".join(
            [f"{k}: {len(v) if isinstance(v, list) else v}" for k, v in profile_dto.__dict__.items()]
        )
        self.profiles_repository.save_profile(profile_dto)
        if profile.get("tenant_get_to_know"):
            user_id = logger.get_user_id()
            if user_id:
                self.user_profiles_repository.update_get_to_know(uuid, profile.get("tenant_get_to_know"), user_id)
                logger.info(f"Saved tenant profile for profile: {profile_dto.name}")
            else:
                logger.info("Could not get tenant id - skipping tenant profile save")
        logger.info(f"About to fetch profile picture for {person.email}")
        if not profile_dto.picture_url or profile_dto.picture_url == DEFAULT_PROFILE_PICTURE:
            profile_dto.picture_url = get_profile_picture(person, social_media_links)
            self.profiles_repository.update_profile_picture(str(profile_dto.uuid), profile_dto.picture_url)
        logger.info(f"Profile picture url: {profile_dto.picture_url}")
        event = GenieEvent(Topic.FINISHED_NEW_PROFILE, {"profile_uuid": str(profile_dto.uuid)})
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
            raise Exception("For some reason, person not found in event body for 'already failed to enrich person'")
        apollo_personal_data = self.personal_data_repository.get_apollo_personal_data(person.uuid)
        if apollo_personal_data:
            logger.info(f"Person already has apollo personal data: {person.email}")
            person = create_person_from_apollo_personal_data(person)
            self.persons_repository.save_person(person)
            return await self.check_profile_data_from_person(person)
        apollo_status = self.personal_data_repository.get_apollo_status(person.uuid)
        if apollo_status == self.personal_data_repository.TRIED_BUT_FAILED:
            logger.info(f"Person already tried to get apollo data: {person.email}")
            event = GenieEvent(
                Topic.FAILED_TO_ENRICH_PERSON,
                {"person": person.to_dict()},
            )
            event.send()
            return {"status": "failed"}
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
        tenant_id = event_body.get("tenant_id") or logger.get_tenant_id()
        user_id = event_body.get("user_id") or logger.get_user_id()
        if not person:
            logger.error(f"Invalid person data: {person}")
            raise Exception("Could not check profile data: Invalid person data")
        if user_id and tenant_id:
            self.ownerships_repository.save_ownership(uuid=person.uuid, user_id=user_id, tenant_id=tenant_id)
        else:
            logger.error(f"Could not get user_id or tenant_id: {user_id}, {tenant_id}")
            raise Exception("Could not check profile data: Could not get user_id or tenant_id")
        result = await self.check_profile_data_from_person(person)
        logger.info(f"Result: {result}")
        return {"status": "success"}

    async def handle_linkedin_scrape(self, event):
        # logger.info(f"Handling LinkedIn scrape event: {event}")
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person_dict = event_body.get("person")
        logger.info(f"Person: {person_dict}")
        if not person_dict:
            logger.error("No person data received in event")
            raise Exception("LinkedIn scrape failed: No person data received in event")
        person = PersonDTO.from_dict(person_dict)
        linkedin = person.linkedin
        uuid = person.uuid

        if not uuid:
            logger.error("UUID is missing in the event data")
            return {"error": "UUID is missing"}

        if not linkedin:
            logger.error(f"No LinkedIn URL found in event body, skipping this part: {event_body}")
            return {"error": "No LinkedIn URL found in event body"}
        logger.info(f"Calling LinkedIn scraper for URL: {linkedin}")
        news_in_database = self.personal_data_repository.get_news_data_by_uuid(uuid)
        if self.personal_data_repository.should_do_linkedin_posts_lookup(uuid):
            scraped_posts = linkedin_scrapper.fetch_and_process_posts(linkedin)

            if not scraped_posts:
                logger.error(f"No posts found or an error occurred while scraping {linkedin}")
                # before updating the status to TRIED_BUT_FAILED, check if there are any posts in the database

                if news_in_database:
                    logger.info(f"But found news in database for {uuid}")
                    return {"posts": news_in_database}
                self.personal_data_repository.update_news_to_db(
                    uuid, None, PersonalDataRepository.TRIED_BUT_FAILED
                )
                event = GenieEvent(Topic.FAILED_TO_GET_PERSONAL_NEWS, {"person_uuid": uuid})
                event.send()
                return {"error": "No posts found or an error occurred"}

            logger.info(f"Successfully scraped {len(scraped_posts)} posts from LinkedIn URL: {linkedin}")
            news_data_objects = []

            for post in scraped_posts:
                if not post:
                    continue
                if news_in_database and post in news_in_database:
                    logger.info(f"Post already in database: {post}")
                    continue
                post_dict = post.to_dict() if isinstance(post, NewsData) else post
                if post_dict.get("image_urls"):
                    post_dict["images"] = post_dict["image_urls"]
                logger.info(f"Post dict: {post_dict}")
                try:
                    news_data_objects.append(SocialMediaPost.from_dict(post_dict))
                except ValidationError as e:
                    logger.error(f"Failed to create SocialMediaLinks object: {e}")
                    continue
                # self.personal_data_repository.update_news_to_db(
                #     uuid, post_dict, PersonalDataRepository.FETCHED
                # )
            final_news_data_list = list(set(news_data_objects + news_in_database))
            if final_news_data_list:
                self.personal_data_repository.update_news_list_to_db(uuid, final_news_data_list, PersonalDataRepository.FETCHED)
            if news_data_objects:
                event = GenieEvent(Topic.NEW_PERSONAL_NEWS, {"person_uuid": uuid, "force": True})
                event.send()
            else:
                logger.info(f"No new posts found for {uuid}")
                event = GenieEvent(Topic.FAILED_TO_GET_PERSONAL_NEWS,
                   {"person_uuid": uuid})
                event.send()
            return {"posts": news_data_objects}
        else:
            logger.info(f"No need to scrape LinkedIn posts for {uuid} as it was scraped recently or never")
            event = GenieEvent(Topic.PERSONAL_NEWS_ARE_UP_TO_DATE,
                {"person_uuid": uuid})
            event.send()
            return {"error": "No need to scrape LinkedIn posts"}

    async def check_profile_data_from_person(self, person: PersonDTO):
        logger.info(f"Checking profile data for person: {person}")
        if not person:
            logger.error(f"Invalid person data: {person}")
            return {"error": "Invalid person data"}
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
        self.persons_repository.save_person(person)

        logger.info(f"About to check profile data for person: {person}")
        existing_profile = self.profiles_repository.get_profile_data(person.uuid)
        if existing_profile: # If there is a profile, check for tenant profile, check if the profile is full - and fix person
            user_id = logger.get_user_id()
            logger.info(f"person_uuid: {person.uuid}, user_id: {user_id}")
            sales_criteria, action_items = self.user_profiles_repository.get_sales_criteria_and_action_items(person.uuid, user_id)
            if not sales_criteria or not action_items:
                event = GenieEvent(Topic.NEW_PERSONAL_DATA,
                                   {"person": person.to_dict(), "personal_data": fetched_personal_data})
                event.send()
            logger.info(f"Has tenant profile for person: {person}")
            if not existing_profile.strengths or not existing_profile.work_history_summary:
                logger.info(f"Profile does not have strengths, sending event to langsmith. Email: {person.email}")
                data_to_send = {"person": person.to_dict(), "personal_data": fetched_personal_data}
                event = GenieEvent(Topic.NEW_PERSONAL_DATA, data_to_send)
                event.send()
            return {"status": "success"}
        else:
            logger.warning("Profile does not exist in database")
            event = GenieEvent(
                Topic.NEW_PERSONAL_DATA,
                data={"person": person.to_dict(), "personal_data": fetched_personal_data},
            )
            event.send()
            return {"status": "success"}
            # self.profiles_repository.save_new_profile_from_person(person)


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

    async def handle_finished_new_profile(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        profile_uuid = event_body.get("profile_uuid")
        if not profile_uuid:
            logger.error("No profile UUID found in event body")
            raise Exception("Failed to handle finished new profile: No profile UUID found in event body")
        profile = self.profiles_repository.get_profile_data(profile_uuid)
        if not profile:
            logger.error(f"Profile not found in database: {profile_uuid}")
            raise Exception("Failed to handle finished new profile: Profile not found in database")
        self.persons_repository.update_status(profile.uuid, PersonStatus.COMPLETED)
        person = self.persons_repository.get_person(profile_uuid)
        if not person:
            logger.error(f"Person not found in database: {profile_uuid}")
            raise Exception("Failed to handle finished new profile: Person not found in database")
        if not person.linkedin:
            person = self.validate_person(person)
            if not person.linkedin:
                logger.error(f"Person has no linkedin: {person}")
                return {"error": "Person has no linkedin"}

        if not profile.picture_url or str(profile.picture_url) == str(DEFAULT_PROFILE_PICTURE):
            self.profiles_repository.update_profile_picture(str(profile.uuid), DEFAULT_PROFILE_PICTURE)
            event = GenieEvent(Topic.FAILED_TO_GET_PROFILE_PICTURE, {"person": person.to_dict()})
            event.send()
            logger.info(f"Sent 'failed_to_get_profile_picture' event to the event queue for {person.email}")
        elif 'profile-picture' in str(profile.picture_url):
            logger.info(f'Profile picture already uploaded: {str(profile.picture_url)}')
            return {"status": "success"}
        else:
            logger.info(f"Profile picture url: {profile.picture_url}")
            result = self.azure_profile_picture_uploader.handle_profile_picture_upload(profile)
            if result:
                logger.info(f"Profile picture uploaded: {profile.picture_url}")
            else:
                logger.error(f"Failed to upload profile picture: {profile.picture_url}")
                self.profiles_repository.update_profile_picture(str(profile.uuid), DEFAULT_PROFILE_PICTURE)
                event = GenieEvent(Topic.FAILED_TO_GET_PROFILE_PICTURE, {"person": person.to_dict()})
                event.send()
                logger.info(f"Sent 'failed_to_upload_profile_picture' event to the event queue for {person.email}")
        return {"status": "failed"}

    async def handle_profile_error(self, event):
        """
        Should check for profile data, and if is broken that update person to be error
        """
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        email = event_body.get("email")
        uuid = event_body.get("uuid")
        if uuid:
            logger.info(f"About to update person status to error for {uuid}")
            self.persons_repository.update_status(uuid, PersonStatus.FAILED)
            return {"status": "success"}
        if email:
            logger.info(f"About to update person status to error for {email}")
            self.persons_repository.update_status_by_email(email, PersonStatus.FAILED)
            return {"status": "success"}
        logger.error("No email or uuid found in event body - could not update person status")
        return {"status": "failed"}



if __name__ == "__main__":
    person_consumer = PersonManager()
    try:
        asyncio.run(person_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
