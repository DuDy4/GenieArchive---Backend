import requests
import jwt
import datetime

from common.genie_logger import GenieLogger
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.genie_event_batch_manager import EventHubBatchManager
from data.data_common.events.topics import Topic
from data.data_common.repositories.users_repository import UsersRepository

logger = GenieLogger()

class SalesforceManager:

    def __init__(self, client_id, client_secret, redirect_uri):
        self.users_repository = UsersRepository()
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        with open("salesforce-genie-private.pem", "r") as key_file:
            self.private_key = key_file.read()
        with open("salesforce-genie-public.pem", "r") as public_key_file:
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

            
    # def fetch_contacts(self, headers, instance_url):
    #     """
    #     Fetch contacts from Salesforce using the access token.
    #
    #     Args:
    #         access_token (str): Salesforce access token.
    #         instance_url (str): The Salesforce instance URL.
    #
    #     Returns:
    #         dict: A dictionary containing the contacts.
    #     """
    #
    #     try:
    #         response = requests.get(f"{instance_url}/services/data/v57.0/sobjects/Contact/0039k000000EZ4RAAW", headers=headers)
    #         response.raise_for_status()
    #         return response.json()
    #     except requests.exceptions.RequestException as e:
    #         print(f"Error fetching contacts: {e}")
    #         return {"error": str(e)}

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
        
    def update_contact(self, headers, instance_url):
        contact_id = "0039k000000EZ4RAAW" 
        endpoint = f"{instance_url}/services/data/v57.0/sobjects/Contact/{contact_id}"

        payload = {
            "Email": "updated.email@example.com",
        }

        response = requests.patch(endpoint, headers=headers, json=payload)
        logger.info(response.status_code)  # Should return 204 for a successful update

        
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
