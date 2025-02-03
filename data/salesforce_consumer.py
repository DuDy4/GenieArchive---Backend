import asyncio
import json
import os
import sys

from common.genie_logger import GenieLogger
from common.utils import env_utils
from common.utils.str_utils import get_uuid4
from data.api_services.salesforce_manager import SalesforceManager
from data.data_common.data_transfer_objects.user_dto import UserDTO
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.genie_event_batch_manager import EventHubBatchManager
from data.data_common.events.topics import Topic
from data.data_common.repositories.contacts_repository import ContactsRepository
from data.data_common.repositories.profiles_repository import ProfilesRepository
from data.data_common.repositories.sf_creds_repository import SalesforceUsersRepository
from data.data_common.repositories.tenants_repository import TenantsRepository
from data.data_common.repositories.user_profiles_repository import UserProfilesRepository
from data.data_common.repositories.users_repository import UsersRepository

logger = GenieLogger()

CONSUMER_GROUP = "salesforce_consumer_group"

sf_client_id = env_utils.get("SALESFORCE_CONSUMER_KEY")
sf_client_secret = env_utils.get("SALESFORCE_CONSUMER_SECRET")
SELF_URL = env_utils.get("SELF_URL", "https://localhost:8000")
sf_redirect_uri = SELF_URL + "/v1/salesforce/callback"
key_file = "../salesforce-genie-private.pem"
public_key_file = "../salesforce-genie-public.pem"


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
        self.user_profiles_repository = UserProfilesRepository()
        self.sf_creds_repository = SalesforceUsersRepository()
        self.salesforce_manager = SalesforceManager(key_file=key_file, public_key_file=public_key_file)

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
        salesforce_user_id = event_body.get("salesforce_user_id")
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
            self.contacts_repository.save_contact(salesforce_id=id, name=name, email=contact_email, user_id=user.user_id, salesforce_user_id=salesforce_user_id)
            event_batch.queue_event(GenieEvent(topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
                data={"email": contact_email, "user_id": user.user_id, "tenant_id": user.tenant_id}))
            event_batch.queue_event(GenieEvent(topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                data={"email": contact_email, "user_id": user.user_id, "tenant_id": user.tenant_id}))
        await event_batch.send_batch()


    async def handle_finished_profile(self, event):
        event_body = event.body_as_str()
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            try:
                event_body = json.loads(event_body)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON: {event_body}")
                return {"error": "Invalid JSON"}
        profile_uuid = event_body.get("profile_uuid")
        if not profile_uuid:
            logger.error("No profile_uuid")
            return
        user_id = event_body.get("user_id") or logger.get_user_id()
        if not user_id:
            logger.error("No user_id")
            return
        profile_category = self.profiles_repository.get_profile_category(profile_uuid)
        sales_criteria, action_items = self.user_profiles_repository.get_sales_criteria_and_action_items(profile_uuid, user_id)
        if not profile_category:
            logger.error(f"No profile category found for profile: {profile_uuid}")
            return

        profile_email = self.profiles_repository.get_email_by_uuid(profile_uuid)
        if not profile_email:
            logger.error(f"No email found for profile: {profile_uuid}")
            return
        contact = self.contacts_repository.get_contact_by_email(profile_email, user_id)
        sf_creds = self.sf_creds_repository.get_sf_creds_by_salesforce_user_id(contact.salesforce_user_id)
        if not contact:
            logger.error(f"No contact found for email: {profile_email}")
            return
        payload = {
            "genieai__ProfileCategory__c": profile_category,
            "genieai__SalesCriteria__c": json.dumps([sale_criteria.to_dict() for sale_criteria in sales_criteria]),
            "genieai__ActionItems__c": json.dumps([action_item.to_dict() for action_item in action_items]),
        }
        self.salesforce_manager.update_contact(contact=contact, sf_creds=sf_creds, payload=payload)

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
