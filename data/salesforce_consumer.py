import asyncio
import json
import os
import sys

from common.genie_logger import GenieLogger, tenant_id
from common.utils.str_utils import get_uuid4
from data.data_common.data_transfer_objects.user_dto import UserDTO
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.genie_event_batch_manager import EventHubBatchManager
from data.data_common.events.topics import Topic
from data.data_common.repositories.contacts_repository import ContactsRepository
from data.data_common.repositories.profiles_repository import ProfilesRepository
from data.data_common.repositories.sf_creds_repository import SalesforceUsersRepository
from data.data_common.repositories.tenants_repository import TenantsRepository
from data.data_common.repositories.users_repository import UsersRepository

logger = GenieLogger()

CONSUMER_GROUP = "salesforce_consumer_group"


class SalesforceConsumer(GenieConsumer):
    def __init__(
            self,
    ):
        super().__init__(
            topics=[
                Topic.NEW_SF_CONTACTS,
                Topic.FINISHED_NEW_PROFILE,
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.contacts_repository = ContactsRepository()
        self.users_repository = UsersRepository()
        self.tenants_repository = TenantsRepository()
        self.profiles_repository = ProfilesRepository()
        self.sf_creds_repository = SalesforceUsersRepository()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        # Should use Topic class

        match topic:
            case Topic.NEW_SF_CONTACTS:
                logger.info("Handling new person")
                await self.handle_new_contacts(event)
            case Topic.FINISHED_NEW_PROFILE:
                logger.info("Handling email address")
                await self.handle_finished_profile(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_new_contacts(self, event):
        event_body = event.body_as_str()
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            try:
                event_body = json.loads(event_body)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON: {event_body}")
                return {"error": "Invalid JSON"}
        contacts = event_body.get("contacts")
        if not contacts:
            logger.error("No contacts")
            return
        event_batch = EventHubBatchManager()
        for contact in contacts:
            owner_email = contact.get("owner_email")
            id = contact.get("id")
            name = contact.get("name")
            contact_email = contact.get("email")
            if not owner_email or not contact_email:
                logger.error(f"Missing required fields: {contact}")
                continue
            user = self.users_repository.get_user_by_email(owner_email)
            if not user:
                logger.info(f"No user found for email: {owner_email}")
                user = self.create_sf_user(owner_email)
                if not user:
                    logger.error(f"Failed to create user for email: {owner_email}")
                    continue
            self.contacts_repository.save_contact(salesforce_id=id, name=name, email=contact_email, user_id=user.user_id)
            event_batch.queue_event(GenieEvent(topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
                data={"email": contact_email, "user_id": user.user_id, "tenant_id": user.tenant_id}))
            event_batch.queue_event(GenieEvent(topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                data={"email": contact_email, "user_id": user.user_id, "tenant_id": user.tenant_id}))
        await event_batch.send_batch()


    async def handle_finished_profile(self, event):
        pass

    def create_sf_user(self, owner_email):
        user_id = get_uuid4()
        tenant_id = self.tenants_repository.get_tenant_id_by_email(owner_email) or get_uuid4()
        user_dto = UserDTO(
            uuid=get_uuid4(),
            user_id=user_id,
            tenant_id=tenant_id,
            email=owner_email,
            name=""
        )
        self.users_repository.insert(user_dto)
        return user_dto


if __name__ == "__main__":
    logger.info(f"Starting SalesforceConsumer")
    consumer = SalesforceConsumer()
    try:
        asyncio.run(consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
