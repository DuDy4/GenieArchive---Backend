import json
import os
import sys

from loguru import logger

from common.events.genie_consumer import GenieConsumer
from common.events.genie_event import GenieEvent
from common.events.topics import Topic
from common.data_transfer_objects.personDTO import PersonDTO
from common.dependencies.dependencies import (
    persons_repository,
    personal_data_repository,
    profiles_repository,
    interactions_repository,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.repositories.persons_repository import PersonsRepository
from common.repositories.personal_data_repository import PersonalDataRepository
from common.repositories.profiles_repository import ProfilesRepository
from common.repositories.interactions_repository import InteractionsRepository


class PersonManager(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[
                Topic.NEW_CONTACT,
                Topic.NEW_INTERACTION,
                Topic.UPDATED_ENRICHED_DATA,
                Topic.NEW_PROCESSED_DATA,
            ]
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
                logger.info("Handling new Salesforce contact")
                await self.handle_new_salesforce_contact(event)
            case Topic.NEW_INTERACTION:
                logger.info("Handling new interaction")
                await self.handle_new_interaction(event)
            case Topic.UPDATED_ENRICHED_DATA:
                logger.info("Handling updated enriched data")
                await self.handle_updated_enriched_data(event)
            case Topic.NEW_PROCESSED_DATA:
                logger.info("Handling new processed data")
                await self.handle_new_processed_data(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_new_salesforce_contact(self, event):
        # Assuming the event body contains a JSON string with the contact data
        logger.info("Handling new Salesforce contact")
        contact_data_str = event.body_as_str()
        logger.debug(f"Contact data: {contact_data_str}")
        contact_data = json.loads(contact_data_str)
        new_person = PersonDTO.from_dict(contact_data)
        self.persons_repository.save_person(new_person)
        if not new_person.linkedin:
            logger.error("Person got no LinkedIn profile, skipping PDL enrichment")
            return
        logger.info("Inserted new Salesforce contact to persons_repository")

        # Send "pdl" event to the event queue
        person_json = new_person.to_json()
        event = GenieEvent(Topic.PDL, person_json, "public")
        event.send()
        logger.info("Sent 'pdl' event to the event queue")

    async def handle_new_interaction(self, event):
        # Assuming the event body contains a JSON string with the contact data
        interaction_data = event.body_as_str()
        self.interactions_repository.save_interaction(interaction_data)
        logger.info("Saved interaction to interactions_repository")

        # Here we should implement whatever we want to do with the interaction data
        # event = GenieEvent(Topic., interaction_data, "public")
        # event.send()

    async def handle_updated_enriched_data(self, event):
        # Assuming the event body contains an uuid and a JSON string with the personal data
        event_body = event.body_as_str()
        personal_data = event_body.get("personal_data")
        uuid = event_body.get("uuid")
        self.personal_data_repository.save_personal_data(uuid, personal_data)
        logger.info("Inserted/Updated enriched data in personal_data_repository")

        # Send "new_personal_data" event to the event queue
        event = GenieEvent(Topic.NEW_PERSONAL_DATA, personal_data, "public")
        event.send()
        logger.info("Sent 'new_personal_data' event to the event queue")

    async def handle_new_processed_data(self, event):
        # Assuming the event body contains a JSON string with the processed data
        event_body = event.body_as_str()
        processed_person = PersonDTO.from_dict(event_body)
        self.profiles_repository.save_profile(processed_person)
        event = GenieEvent(Topic.NEW_PROCESSED_PROFILE, processed_person, "public")
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
