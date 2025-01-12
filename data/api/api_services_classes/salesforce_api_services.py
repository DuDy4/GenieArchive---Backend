import asyncio

from common.utils import env_utils
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.events.genie_event_batch_manager import EventHubBatchManager
from common.genie_logger import GenieLogger, tenant_id
from fastapi import HTTPException
import datetime

from data.data_common.repositories.sf_creds_repository import SalesforceUsersRepository
from data.data_common.repositories.users_repository import UsersRepository
from data.data_common.salesforce.salesforce_integrations_manager import SalesforceClient
from data.internal_services.tenant_service import TenantService

logger = GenieLogger()

DEV_MODE = env_utils.get("DEV_MODE", "")


class SalesforceApiService:
    def __init__(self):
        self.users_repository = UsersRepository()
        self.salesforce_client = None
        self.salesforce_creds_repository = SalesforceUsersRepository()


    async def handle_new_contact(self, contact_email: str, salesforce_user_id):
        if not contact_email:
            logger.error("No contact email")
            return
        # user_id = self.salesforce_creds_repository.get_user_id_by_sf_id(salesforce_user_id)
        user_id = "google-oauth2|117881894742800328091"
        tenant_id = self.users_repository.get_tenant_id_by_user_id(user_id)
        if not user_id or not tenant_id:
            logger.error(f"No user found for salesforce_user_id={salesforce_user_id}")
            return
        # event = GenieEvent(
        #     topic=Topic.NEW_CONTACT,
        #     data={"contact_email": contact_email, "user_id": user_id, "tenant_id": tenant_id},
        # )
        # event.send()
        event_batch = EventHubBatchManager()
        event_batch.queue_event(GenieEvent(
            topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
            data={"email": contact_email, "user_id": user_id, "tenant_id": tenant_id},
        ))
        event_batch.queue_event(GenieEvent(
            topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
            data={"email": contact_email, "user_id": user_id, "tenant_id": tenant_id},
        ))
        await event_batch.send_batch()
        logger.info(f"Sent events for contact email: {contact_email}")
        return {"message": f"Sent events for {contact_email}"}




sf_api_service = SalesforceApiService()
asyncio.run(sf_api_service.handle_new_contact("asaf@genieai.ai", "0052v00000B2Z3AAAV"))