import requests
import os
from sseclient import SSEClient
from loguru import logger
from requests_oauthlib import OAuth2Session
from app_common.repositories.salesforce_users_repository import (
    SalesforceUsersRepository,
)
from app_common.dependencies.dependencies import salesforce_users_repository
from app_common.utils.str_utils import get_uuid4
from app_common.repositories.contacts_repository import ContactsRepository
from app_common.data_transfer_objects.person_dto import PersonDTO
from app.services.salesforce import SalesforceClient

from events.genie_event import GenieEvent
from events.topics import Topic

load_dotenv()

SELF_URL = os.environ.get("self_url", "https://localhost:3000")


WEB_HOOK_URL = os.environ.get("WEB_HOOK_URL", SELF_URL + "/v1/webhook")


class SalesforceEventConsumer(SalesforceClient):
    def __init__(self, access_token: str, instance_url: str):
        super().__init__(access_token, instance_url)

    def subscribe_to_events(self, topic_name: str):
        """
        Subscribes to a Salesforce Platform Event.

        Args:
            topic_name (str): The name of the Platform Event topic.
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        url = f"{self.instance_url}/cometd/44.0"
        session = requests.Session()
        session.headers.update(headers)

        messages = SSEClient(url, session=session)

        for message in messages:
            if message.event == topic_name:
                logger.info(f"Received event: {message.data}")
                self.send_to_webhook(message.data)

    def send_to_webhook(self, event_data):
        """
        Sends the event data to the webhook endpoint.

        Args:
            event_data (dict): The event data to be sent.
        """
        try:
            response = requests.post(WEB_HOOK_URL, json=event_data)
            response.raise_for_status()
            logger.info(f"Event sent to webhook successfully: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending event to webhook: {e}")
