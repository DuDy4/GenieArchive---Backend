
import requests
from common.utils import env_utils
from common.utils.jwt_utils import generate_pkce_pair
from data.api_services.salesforce_manager import SalesforceManager
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.events.genie_event_batch_manager import EventHubBatchManager
from common.genie_logger import GenieLogger

from data.data_common.repositories.sf_creds_repository import SalesforceUsersRepository
from data.data_common.repositories.users_repository import UsersRepository

logger = GenieLogger()

DEV_MODE = env_utils.get("DEV_MODE", "")
consumer_key = env_utils.get("SALESFORCE_CONSUMER_KEY")
consumer_secret = env_utils.get("SALESFORCE_CONSUMER_SECRET")
salesforce_redirect_uri = f"{env_utils.get("SELF_URL")}/v1/salesforce-oauth/callback"

class SalesforceApiService:
    def __init__(self):
        self.users_repository = UsersRepository()
        self.sf_creds_repository = SalesforceUsersRepository()
        self.salesforce_creds_repository = SalesforceUsersRepository()
        self.sf_manager = SalesforceManager(consumer_key, consumer_secret, salesforce_redirect_uri)

    async def get_user_contacts(self, user_creds):
        if not user_creds:
            logger.error("No user creds")
            return
        salesforce_tenant_id = user_creds.get("salesforce_tenant_id")
        instance_url = user_creds.get("instance_url")
        access_token = user_creds.get("access_token")

        contacts = await (self.sf_manager.get_contacts(salesforce_tenant_id=salesforce_tenant_id, instance_url=instance_url, access_token=access_token))
        logger.info(f"Fetched {len(contacts)} contacts from Salesforce")
        return contacts

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

    def generate_salesforce_oauth_url(self):
        """Generates the Salesforce OAuth URL for the user to authenticate."""
        base_url = "https://login.salesforce.com/services/oauth2/authorize"
        response_type = "code"
        code_verifier, code_challenge = generate_pkce_pair()
        oauth_url = f"{base_url}?response_type={response_type}&client_id={consumer_key}&redirect_uri={salesforce_redirect_uri}&code_challenge={code_challenge}&code_challenge_method=S256&state={code_verifier}"
        return oauth_url

    async def handle_salesforce_oauth_callback(self, code: str, state: str):
        auth_response = self.exchange_salesforce_code(code, state)
        if not auth_response or not auth_response.get("access_token"):
            return {"error": "Error exchanging Salesforce code for tokens."}

        logger.info(f"Salesforce auth response: {auth_response}")
        access_token = auth_response.get("access_token")
        refresh_token = auth_response.get("refresh_token")
        instance_url = auth_response.get("instance_url")
        salesforce_id_url = auth_response.get("id")
        if not access_token or not instance_url or not salesforce_id_url:
            logger.error("Missing access token, instance URL, or Salesforce ID URL")
            return {"error": "Missing access token, instance URL, or Salesforce ID URL"}
        salesforce_user_id = salesforce_id_url.split("/")[-1]
        salesforce_tenant_id = salesforce_id_url.split("/")[-2]
        self.sf_creds_repository.save_user_creds(
            salesforce_user_id=salesforce_user_id,
            salesforce_tenant_id=salesforce_tenant_id,
            salesforce_instance_url=instance_url,
            salesforce_refresh_token=refresh_token,
            salesforce_access_token=access_token,
        )
        logger.info(f"Saved Salesforce credentials for user {salesforce_user_id}")
        logger.info(f"Salesforce user ID: {salesforce_user_id}, instance URL: {instance_url}, access token: {access_token}, refresh token: {refresh_token}")
        result = {
            "salesforce_user_id": salesforce_user_id,
            "salesforce_tenant_id": salesforce_tenant_id,
            "instance_url": instance_url,
            "access_token": access_token,
            "refresh_token": refresh_token,
        }
        return result

    def exchange_salesforce_code(self, auth_code: str, auth_state: str) -> dict:
        """
        Exchange Salesforce authorization code for access and refresh tokens.

        Args:
            auth_code (str): The authorization code received from Salesforce.
            redirect_uri (str): The callback URL configured in the Connected App.
            client_id (str): The Salesforce Connected App's Consumer Key.
            client_secret (str): The Salesforce Connected App's Consumer Secret.

        Returns:
            Dict: A dictionary containing the access token, refresh token, and other metadata.
        """
        token_endpoint = "https://login.salesforce.com/services/oauth2/token"

        payload = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": salesforce_redirect_uri,
            "client_id": consumer_key,
            "client_secret": consumer_secret,
            "code_verifier": auth_state,
        }

        try:
            response = requests.post(token_endpoint, data=payload, timeout=10)
            response.raise_for_status()  # Raise HTTPError for bad responses
            return response.json()  # Parse and return the JSON response
        except requests.exceptions.RequestException as e:
            print(f"Error during Salesforce token exchange: {e}")
            return {"error": str(e)}

    async def handle_new_salesforce_auth(self, user_creds):
        contacts = await self.get_user_contacts(user_creds)
        event = GenieEvent(
            topic=Topic.NEW_SF_CONTACTS,
            data={"contacts": contacts},
        )
        event.send()



