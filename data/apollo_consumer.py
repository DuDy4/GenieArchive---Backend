import asyncio
import json
import os
import sys
import traceback

import data.data_common.repositories.persons_repository

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.api_services.apollo import ApolloClient

from data.data_common.utils.str_utils import get_uuid4, to_custom_title_case

from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.repositories.personal_data_repository import PersonalDataRepository
from data.data_common.dependencies.dependencies import persons_repository, personal_data_repository
from data.data_common.utils.persons_utils import create_person_from_apollo_personal_data

from data.data_common.data_transfer_objects.person_dto import PersonDTO

from common.genie_logger import GenieLogger

logger = GenieLogger()

CONSUMER_GROUP = "apollo_consumer_group"


class ApolloConsumer(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[
                Topic.APOLLO_NEW_EMAIL_ADDRESS_TO_ENRICH,
                Topic.APOLLO_NEW_PERSON_TO_ENRICH,
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.persons_repository: PersonsRepository = persons_repository()
        self.personal_data_repository: PersonalDataRepository = personal_data_repository()
        self.apollo_client = ApolloClient()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        match topic:
            case Topic.APOLLO_NEW_EMAIL_ADDRESS_TO_ENRICH:
                logger.info("Handling failed attempt to get linkedin url")
                await self.handle_new_email_address_to_enrich(event)
            case Topic.APOLLO_NEW_PERSON_TO_ENRICH:
                logger.info("Handling failed attempt to enrich data")
                await self.handle_new_person_to_enrich(event)
            case _:
                logger.error(f"Should not have reached here: {topic}, consumer_group: {CONSUMER_GROUP}")

    async def handle_new_person_to_enrich(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person = event_body.get("person")
        logger.info(f"Person: {person}")
        if isinstance(person, str):
            person = json.loads(person)
        person = PersonDTO(**person)
        logger.info(f"Person DTO: {person}")
        # if person.linkedin:
        #     logger.info(f"Person already has LinkedIn: {person.LinkedIn}")
        #     return {"status": "ok"}
        person_in_db = self.persons_repository.get_person_by_email(person.email)
        if person_in_db:
            # if person_in_db.linkedin:
            #     logger.info(f"Person in database already has linkedin_url: {person_in_db.LinkedIn}")
            personal_data = self.personal_data_repository.get_pdl_personal_data_by_email(person.email)
            if personal_data:
                logger.info(f"Personal data already exists for email: {person.email}")
                return {"status": "ok"}
        apollo_personal_data_from_db = self.personal_data_repository.get_apollo_personal_data_by_email(
            person.email
        )
        if apollo_personal_data_from_db:
            logger.warning(f"Already have personal data from apollo for email: {person.email}")
            logger.debug(f"Personal data: {str(apollo_personal_data_from_db)[:300]}")
            person = create_person_from_apollo_personal_data(person)
            self.persons_repository.save_person(person)
            # logger.error(f"To avoid loops, stopping here")
            event = GenieEvent(
                topic=Topic.APOLLO_UP_TO_DATE_ENRICHED_DATA,
                data={"person": person.to_dict()},
                scope="public",
            )
            event.send()
            return {"status": "ok"}

        # If we do not have any personal data on this person, fetch it from Apollo
        apollo_personal_data = self.apollo_client.enrich_person(person)
        logger.debug(f"Apollo personal data: {apollo_personal_data}")
        if not apollo_personal_data:
            logger.warning(f"Failed to get personal data for person: {person}")
            self.personal_data_repository.save_apollo_personal_data(
                person=person, personal_data=None, status=self.personal_data_repository.TRIED_BUT_FAILED
            )
            if not person:
                logger.error(f"Unexpected error: person is None")
                person = person_in_db
            event = GenieEvent(
                topic=Topic.APOLLO_FAILED_TO_ENRICH_PERSON,
                data={"person": person.to_dict(), "email": person.email},
                scope="public",
            )
            event.send()
            return {"error": "Failed to get personal data"}

        # If we successfully fetched personal data from Apollo, save it
        self.handle_successful_data_fetch(person, apollo_personal_data)
        # The method return success status

    async def handle_new_email_address_to_enrich(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        email = event_body.get("email")
        person = self.persons_repository.get_person_by_email(email)
        if not person:
            logger.error(f"Person not found for email: {email}")
            uuid = self.personal_data_repository.get_personal_uuid_by_email(email)
            if not uuid:
                logger.error(f"Personal data not found for email: {email}")
                uuid = get_uuid4()
            person = PersonDTO(
                uuid=uuid,
                name="",
                company="",
                position="",
                email=email,
                linkedin="",
                timezone="",
            )
        # if person.linkedin:
        #     logger.info(f"Person already has linkedin: {person.linkedin}")
        #     return {"status": "ok"}

        apollo_personal_data = self.apollo_client.enrich_person(person)
        if not apollo_personal_data:
            logger.warning(f"Failed to get personal data for person: {person}")
            self.personal_data_repository.save_apollo_personal_data(
                person=person, personal_data=None, status=self.personal_data_repository.TRIED_BUT_FAILED
            )
            event = GenieEvent(
                topic=Topic.APOLLO_FAILED_TO_ENRICH_EMAIL,
                data={"person": person.to_dict(), "email": person.email},
                scope="public",
            )
            event.send()
            return {"error": "Failed to get personal data"}

        # If we successfully fetched personal data from Apollo, save it
        self.handle_successful_data_fetch(person, apollo_personal_data)
        # The method return success status

    def handle_successful_data_fetch(self, person: PersonDTO, apollo_personal_data: dict):
        self.personal_data_repository.save_apollo_personal_data(person, apollo_personal_data)
        person = create_person_from_apollo_personal_data(person)
        logger.info(f"Person after creating from apollo data: {person}")
        self.persons_repository.save_person(person)
        self.personal_data_repository.update_name_in_personal_data(person.uuid, person.name)
        self.personal_data_repository.update_linkedin_url(person.uuid, person.linkedin)

        event = GenieEvent(
            topic=Topic.APOLLO_UPDATED_ENRICHED_DATA,
            data={"person": person.to_dict()},
            scope="public",
        )
        event.send()
        logger.info(f"Sent new person event: {person}")
        return {"status": "success"}


if __name__ == "__main__":
    apollo_consumer = ApolloConsumer()
    try:
        asyncio.run(apollo_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
