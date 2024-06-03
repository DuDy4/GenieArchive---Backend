from loguru import logger

from ai.langsmith.langsmith_loader import Langsmith
from common.events.genie_consumer import GenieConsumer
from common.events.topics import Topic
from common.dependencies.dependencies import (
    persons_repository,
    personal_data_repository,
    profiles_repository,
)
from common.repositories.persons_repository import PersonsRepository
from common.repositories.personal_data_repository import PersonalDataRepository
from common.repositories.profiles_repository import ProfilesRepository


class PersonManager(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[Topic.NEW_CONTACT, "updated_enriched_data", "new_processed_data"]
        )
        self.langsmith = Langsmith()
        self.persons_repository = persons_repository()
        self.personal_data_repository = personal_data_repository()
        self.profiles_repository = profiles_repository()

    async def process_event(self, event):
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")

        if topic == "new_salesforce_contact":
            await self.handle_new_salesforce_contact(event)
        elif topic == "updated_enriched_data":
            await self.handle_updated_enriched_data(event)
        elif topic == "new_processed_data":
            await self.handle_new_processed_data(event)
        else:
            logger.info(f"Unknown topic: {topic}")

    async def handle_new_salesforce_contact(self, event):
        # Assuming the event body contains a JSON string with the contact data
        contact_data = event.body_as_str()
        self.persons_repository.save_person(contact_data)
        logger.info("Inserted new Salesforce contact to persons_repository")

        # Send "pdl" event to the event queue
        await self.consumer.send_event("pdl", contact_data)
        logger.info("Sent 'pdl' event to the event queue")

    async def handle_updated_enriched_data(self, event):
        # Assuming the event body contains an uuid and a JSON string with the personal data
        event_body = event.body_as_str()
        personal_data = event_body.get("personal_data")
        uuid = event_body.get("uuid")
        self.personal_data_repository.save_personal_data(uuid, personal_data)
        logger.info("Inserted/Updated enriched data in personal_data_repository")

        # Send "new_personal_data" event to the event queue
        await self.event_queue.send_event("new_personal_data", personal_data)
        logger.info("Sent 'new_personal_data' event to the event queue")

    async def handle_new_processed_data(self, event):
        # Assuming the event body contains a JSON string with the processed data
        processed_data = event.body_as_str()
        self.profiles_repository.sa(processed_data)
        logger.info("Saved new processed data to profiles_repository")

    async def close(self):
        await self.event_queue.close()
