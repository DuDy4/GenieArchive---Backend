import requests
import jwt
import datetime
from requests_oauthlib import OAuth2Session

from common.genie_logger import GenieLogger
from common.utils import env_utils
from data.data_common.data_transfer_objects.contact_dto import ContactDTO
from data.data_common.data_transfer_objects.sf_creds_dto import SalesforceCredsDTO
from data.data_common.repositories.sf_creds_repository import SalesforceUsersRepository
from data.data_common.repositories.users_repository import UsersRepository

logger = GenieLogger()
sf_client_id = env_utils.get("SALESFORCE_CONSUMER_KEY")
sf_client_secret = env_utils.get("SALESFORCE_CONSUMER_SECRET")
SELF_URL = env_utils.get("SELF_URL", "https://localhost:8000")
SALESFORCE_REDIRECT_URI = SELF_URL + "/v1/salesforce/callback"
SALESFORCE_TOKEN_URL = env_utils.get("SALESFORCE_TOKEN_URL")


class SalesforceManager:

    def __init__(self, key_file='', public_key_file=''):
        self.users_repository = UsersRepository()
        self.sf_creds_repository = SalesforceUsersRepository()
        self.client_id = sf_client_id
        self.client_secret = sf_client_secret
        self.redirect_uri = SALESFORCE_REDIRECT_URI
        with open(key_file, "r") as key_file:
            self.private_key = key_file.read()
        with open(public_key_file, "r") as public_key_file:
            self.public_key = public_key_file.read()

    def create_signed_jwt(self, client_id, user_email, private_key, login_url):
        """
        Create a signed JWT for the Salesforce JWT Bearer Flow.

        Args:
            client_id (str): Salesforce Connected App client ID.
            user_email (str): The Salesforce service user's email.
            private_key (str): The private key to sign the JWT.
            login_url (str): Salesforce login URL (e.g., "https://login.salesforce.com").

        Returns:
            str: A signed JWT.
        """
        now = int(datetime.datetime.now(datetime.UTC).timestamp())
        payload = {
            "iss": client_id,              # Connected App Client ID
            "sub": user_email,             # Service user's email
            "aud": f"{login_url}/services/oauth2/token",  # Salesforce login endpoint
            "exp": now + 300               # Token expiry (5 minutes)
        }

        # Sign the JWT using the private key
        signed_jwt = jwt.encode(payload, private_key, algorithm="RS256")
        return signed_jwt


    async def get_contacts(self, salesforce_tenant_id, instance_url, access_token):
        """
        Retrieve contacts from salesforce.

        Returns:
        list: List of contact records.
        """
        url = f"{instance_url}/services/data/v61.0/query/"
        query = """
            SELECT Id, Name, Email, 
                   (SELECT Owner.Email FROM Opportunities) 
            FROM Contact 
            ORDER BY CreatedDate DESC 
            LIMIT 10
            """
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        params = {"q": query}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            res_contacts = response.json()["records"]
            logger.info(f"Retrieved contacts: {len(res_contacts)}")
            logger.info(res_contacts)
            contacts = []
            for contact in res_contacts:
                opportunities = contact.get('Opportunities', {})
                records = opportunities.get('records', []) if opportunities else []
                owner_email = records[0]['Owner']['Email'] if records else None

                contacts.append({
                    "id": contact.get("Id"),
                    "name": contact.get("Name"),
                    "email": contact.get("Email"),
                    "owner_email": owner_email,
                })
            logger.info(f"Processed contacts: {contacts}")
            return contacts
        except Exception as e:
            print(f"Failed to retrieve contacts: {e}")
            return []
        
    def update_contact(self, contact: ContactDTO, sf_creds: SalesforceCredsDTO, payload):
        logger.info(f"Updating contact {contact.id} with payload: {payload} and sf_creds: {sf_creds}")
        for key, value in payload.items():
            self.add_genie_category_field(sf_creds, key)
        endpoint = f"{sf_creds.instance_url}/services/data/v57.0/sobjects/Contact/{contact.id}"

        response = self.request_with_refresh_token(sf_creds, endpoint, payload)
        logger.info(response.status_code)  # Should return 204 for a successful update

    def request_with_refresh_token(self, sf_creds: SalesforceCredsDTO, endpoint, payload=None):
        headers = {
            "Authorization": f"Bearer {sf_creds.access_token}",
            "Content-Type": "application/json"
        }
        try:
            response = requests.post(endpoint, headers=headers, json=payload)
            if response.status_code == 401:
                logger.info("Access token expired. Refreshing token...")
                new_access_token = self.refresh_access_token(sf_creds.refresh_token)
                if new_access_token:
                    headers["Authorization"] = f"Bearer {new_access_token}"
                    response = requests.post(endpoint, headers=headers, json=payload)
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Error updating contact: {e}")
            return None
        
    def get_access_token(self, jwt_token, login_url):
        """
        Exchange a signed JWT for an access token.

        Args:
            jwt_token (str): The signed JWT.
            login_url (str): Salesforce login URL (e.g., "https://login.salesforce.com").

        Returns:
            dict: A dictionary containing the access token and metadata.
        """
        token_url = f"{login_url}/services/oauth2/token"
        payload = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt_token,
        }

        try:
            #self.validate_jwt(jwt_token, self.public_key)
            response = requests.post(token_url, data=payload)
            response.raise_for_status()  # Raise HTTPError for non-2xx responses
            return response.json()  # Access token and metadata
        except requests.exceptions.RequestException as e:
            print(f"Error during token exchange: {e}")
            return {"error": str(e)}
        

    def validate_jwt(self, jwt_token, public_key):
        try:
            decoded = jwt.decode(jwt_token, public_key, algorithms=["RS256"], audience="https://login.salesforce.com/services/oauth2/token")
            print("JWT is valid! Decoded payload:", decoded)
        except jwt.ExpiredSignatureError:
            print("JWT has expired.")
        except jwt.InvalidTokenError as e:
            print(f"Invalid JWT: {e}")

    def test_flow(self):
        # Create a signed JWT
        signed_jwt = self.create_signed_jwt(self.client_id, "asaf-service@genieai.ai", self.private_key, "https://login.salesforce.com")
        token_response = self.get_access_token(
            jwt_token=signed_jwt,
            login_url="https://login.salesforce.com"
        )
        print("Access Token Response:", token_response)

        if "access_token" in token_response:
            access_token=token_response["access_token"]
            instance_url=token_response["instance_url"]
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            }
            contacts = self.fetch_contacts(
                headers,
                instance_url
            )
            self.update_contact(headers, instance_url)
            print("Contacts:", contacts)
        else:
            print("Error:", token_response)

    def add_genie_category_field(self, sf_creds: SalesforceCredsDTO, field_name):
        endpoint = f"{sf_creds.instance_url}/services/data/v57.0/tooling/sobjects/CustomField"
        payload = {
            "fullName": f"Contact.{field_name}",  # Object and API field name
            "type": "Text",  # Field type
            "length": 255,  # Length for Text fields (required)
            "inlineHelpText": f"This is the {field_name} field for GenieAI.",
            "metadata": {
                "label": field_name,  # Label for the field
            },
        }
        response = self.request_with_refresh_token(sf_creds, endpoint, payload)
        if response.status_code == 201:
            logger.info(f"Field {field_name} created successfully.")
        else:
            logger.error(f"Failed to create field {field_name}: {response.status_code} - {response.text}")


    def refresh_access_token(self, refresh_token):
        token_url = SALESFORCE_TOKEN_URL
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        logger.info(f"Refreshing access token with payload: {payload}")
        try:
            response = requests.post(token_url, data=payload)
            response.raise_for_status()
            new_tokens = response.json()
            new_access_token = new_tokens.get('access_token')
            self.sf_creds_repository.update_access_token(refresh_token, new_access_token)
            return new_access_token
        except requests.exceptions.RequestException as e:
            logger.error(f"Error refreshing access token: {e}")
            return None