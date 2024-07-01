import json
import os
import sys

from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from common.dependencies.dependencies import (
    persons_repository,
    personal_data_repository,
    profiles_repository,
    interactions_repository,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class PersonManager(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[
                Topic.NEW_CONTACT,
                Topic.NEW_INTERACTION,
                Topic.UPDATED_ENRICHED_DATA,
                Topic.NEW_PROCESSED_PROFILE,
            ],
            consumer_group="personmanagerconsumergroup",
        )
        self.persons_repository = persons_repository()
        self.personal_data_repository = personal_data_repository()
        self.profiles_repository = profiles_repository()
        self.interactions_repository = interactions_repository()

    async def process_event(self, event):
        logger.info(f"PersonManager processing event: {event}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        # Should use Topic class

        match topic:
            case Topic.NEW_CONTACT:
                logger.info("Handling new salesforce contact")
                await self.handle_new_salesforce_contact(event)
            case Topic.NEW_INTERACTION:
                logger.info("Handling new interaction")
                await self.handle_new_interaction(event)
            case Topic.UPDATED_ENRICHED_DATA:
                logger.info("Handling updated enriched data")
                await self.handle_updated_enriched_data(event)
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
        new_person = PersonDTO.from_dict(contact_data)
        uuid = self.persons_repository.save_person(new_person)
        new_person.uuid = uuid
        if not new_person.linkedin:
            logger.error("Person got no LinkedIn profile, skipping PDL enrichment")
            return
        logger.info("Inserted new salesforce contact to persons_repository")

        # Send "pdl" event to the event queue
        person_json = new_person.to_json()
        event = GenieEvent(Topic.NEW_CONTACT_TO_ENRICH, person_json, "public")
        event.send()
        logger.info("Sent 'pdl' event to the event queue")

    async def handle_new_interaction(self, event):
        # Assuming the event body contains a JSON string with the contact data
        interaction_data = event.body_as_str()
        # should gather all of the interactions of this person, and the personal data - then send to langsmith
        self.interactions_repository.save_interaction(interaction_data)
        logger.info("Saved interaction to interactions_repository")

        # Here we should implement whatever we want to do with the interaction data
        # event = GenieEvent(Topic., interaction_data, "public")
        # event.send()

    async def handle_updated_enriched_data(self, event):
        # Assuming the event body contains an uuid and a JSON string with the personal data
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        personal_data = event_body.get("personal_data")
        person_dict = event_body.get("person")
        if isinstance(person_dict, str):
            person_dict = json.loads(person_dict)
        person: PersonDTO = PersonDTO.from_dict(person_dict)
        personal_data_in_database = self.personal_data_repository.get_personal_data(
            person.uuid
        )

        if personal_data_in_database != personal_data:
            logger.error(
                "Personal data in database does not match the one received from event"
            )
            logger.debug(f"Personal data in database: {personal_data_in_database}")
            logger.debug(f"Personal data received: {personal_data}")
        data_to_send = {"person": person.to_dict(), "personal_data": personal_data}
        # Send "new_personal_data" event to the event queue
        event = GenieEvent(Topic.NEW_PERSONAL_DATA, data_to_send, "public")
        event.send()
        logger.info("Sent 'new_personal_data' event to the event queue")

    async def handle_new_processed_profile(self, event):
        # Assuming the event body contains a JSON string with the processed data
        event_body_str = event.body_as_str()
        event_body = json.loads(event_body_str)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person_dict = event_body.get("person")
        profile = event_body.get("profile")

        profile_person = ProfileDTO.from_dict(
            {
                "uuid": person_dict.get("uuid"),
                "owner_id": person_dict.get("owner_id"),
                "name": person_dict.get("name"),
                "company": person_dict.get("company"),
                "position": person_dict.get("position"),
                "challenges": profile.get("challenges", []),
                "strengths": profile.get("strengths", []),
                "summary": profile.get("summary", ""),
            }
        )
        logger.debug(f"Profile person: {profile_person}")
        self.profiles_repository.save_profile(profile_person)
        json_profile = profile_person.to_json()
        event = GenieEvent(Topic.FINISHED_NEW_PROFILE, json_profile, "public")
        event.send()
        logger.info("Saved new processed data to profiles_repository")


if __name__ == "__main__":
    person_manager = PersonManager()
    # uvicorn.run(
    #     "person:app",
    #     host="0.0.0.0",
    #     port=PERSON_PORT,
    #     ssl_keyfile="../key.pem",
    #     ssl_certfile="../cert.pem",
    # )
    # print("Running person service")
    person_manager.run()
