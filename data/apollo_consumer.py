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


from data.data_common.utils.str_utils import get_uuid4

from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.repositories.personal_data_repository import PersonalDataRepository
from data.data_common.dependencies.dependencies import persons_repository, personal_data_repository

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
                Topic.FAILED_TO_ENRICH_EMAIL,
                Topic.FAILED_TO_ENRICH_DATA,
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
            case Topic.FAILED_TO_ENRICH_EMAIL:
                logger.info("Handling failed attempt to get linkedin url")
                await self.handle_failed_to_enrich_email(event)
            case Topic.FAILED_TO_ENRICH_DATA:
                logger.info("Handling failed attempt to enrich data")
                await self.handle_failed_to_enrich_data(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_failed_to_enrich_data(self, event):
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person = event_body.get("person")
        logger.info(f"Person: {person}")
        person = PersonDTO(**person)
        logger.info(f"Person DTO: {person}")
        if person.linkedin:
            logger.info(f"Person already has linkedin: {person.linkedin}")
            return {"status": "ok"}
        person_in_db = self.persons_repository.get_person_by_email(person.email)
        if person_in_db:
            if person_in_db.linkedin:
                logger.info(f"Person in database already has linkedin_url: {person_in_db.linkedin}")
                personal_data = self.personal_data_repository.get_personal_uuid_by_email(person.email)
                if personal_data:
                    logger.info(f"Personal data already exists for email: {person.email}")
                    return {"status": "ok"}
                # return {"status": "ok"}
        apollo_personal_data_from_db = self.personal_data_repository.get_apollo_personal_data_by_email(
            person.email
        )
        if apollo_personal_data_from_db:
            logger.warning(f"Already have personal data from apollo for email: {person.email}")
            logger.debug(f"Personal data: {str(apollo_personal_data_from_db)[:300]}")
            logger.error(f"To avoid loops, stopping here")
            return {"status": "ok"}

        apollo_personal_data = self.apollo_client.enrich_contact([person.email])
        logger.debug(f"Apollo personal data: {apollo_personal_data}")
        if not apollo_personal_data:
            logger.warning(f"Failed to get personal data for person: {person}")
            if not person:
                logger.error(f"Unexpected error: person is None")
                person = person_in_db
            event = GenieEvent(
                topic=Topic.FAILED_TO_GET_LINKEDIN_URL,
                data={"person": person.to_dict(), "email": person.email},
                scope="public",
            )
            event.send()
            return {"error": "Failed to get personal data"}
        self.personal_data_repository.save_apollo_personal_data(person, apollo_personal_data)
        linkedin_url = apollo_personal_data.get("linkedin_url")

        # Should not happen, but just in case
        if not linkedin_url:
            logger.error(f"Failed to get linkedin url for person: {person}")
            if not person:
                logger.error(f"Unexpected error: person is None")
                person = person_in_db
            event = GenieEvent(
                topic=Topic.FAILED_TO_GET_LINKEDIN_URL,
                data={"person": person.to_dict(), "email": person.email},
                scope="public",
            )
            event.send()
            return {"error": "Failed to get linkedin url"}
        logger.info(f"Got linkedin url: {linkedin_url}")
        person.linkedin = linkedin_url
        self.persons_repository.save_person(person)
        event = GenieEvent(
            topic=Topic.NEW_PERSON,
            data=person.to_dict(),
            scope="public",
        )
        event.send()
        logger.info(f"Sent new person event: {person}")
        return {"status": "ok"}

    async def handle_failed_to_enrich_email(self, event):
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
        if person.linkedin:
            logger.info(f"Person already has linkedin: {person.linkedin}")
            return {"status": "ok"}

        apollo_personal_data = self.apollo_client.enrich_contact([email])
        if not apollo_personal_data:
            logger.warning(f"Failed to get personal data for person: {person}")
            event = GenieEvent(
                topic=Topic.FAILED_TO_GET_LINKEDIN_URL,
                data={"person": person.to_dict(), "email": person.email},
                scope="public",
            )
            event.send()
            return {"error": "Failed to get personal data"}
        self.personal_data_repository.save_apollo_personal_data(person, apollo_personal_data)
        linkedin_url = apollo_personal_data.get("linkedin_url")
        person = self.create_person_from_apollo_data(person, apollo_personal_data)
        person.linkedin = linkedin_url
        if not person.linkedin:
            logger.warning(f"Got personal data from Apollo, but no linkedin url: {person}")
        self.persons_repository.save_person(person)
        event = GenieEvent(
            topic=Topic.NEW_PERSON,
            data={"person": person.to_dict()},
            scope="public",
        )
        event.send()
        logger.info(f"Sent new person event: {person}")
        return {"status": "ok"}

    def create_person_from_apollo_data(self, person, apollo_data):
        person.name = apollo_data.get("name")
        person.company = apollo_data.get("company")
        person.position = apollo_data.get("position")
        person.linkedin = apollo_data.get("linkedin_url")
        return person


if __name__ == "__main__":
    apollo_consumer = ApolloConsumer()
    try:
        asyncio.run(apollo_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
