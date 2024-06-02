"""
Module for interacting with the Salesforce API.
"""

import os
import uuid


from dotenv import load_dotenv
import requests
from loguru import logger
from requests_oauthlib import OAuth2Session
from app_common.repositories.salesforce_users_repository import (
    SalesforceUsersRepository,
)
from app_common.dependencies.dependencies import salesforce_users_repository
from app_common.utils.str_utils import get_uuid4
from app_common.postgres_connector import get_db_connection
from app_common.repositories.contacts_repository import ContactsRepository

load_dotenv()

SELF_URL = os.environ.get("self_url", "https://localhost:8444")
SALESFORCE_CLIENT_ID = os.environ.get("SALESFORCE_CLIENT_ID")
SALESFORCE_CLIENT_SECRET = os.environ.get("SALESFORCE_CLIENT_SECRET")
SALESFORCE_LOGIN_URL = os.environ.get("SALESFORCE_LOGIN_URL")
SALESFORCE_REDIRECT_URI = SELF_URL + "/v1/salesforce/callback"
SALESFORCE_TOKEN_URL = os.environ.get("SALESFORCE_TOKEN_URL")

sf_users_repository = salesforce_users_repository()


class SalesforceClient:
    """
    Class for interacting with the Salesforce API on behalf of a company.
    """

    def __init__(
        self,
        access_token: str,
        instance_url: str,
    ):
        """
        Initializes the SalesforceClient with the given parameters.

        Args:
            access_token (str): The access token for the Salesforce (for accessing client's salesforce instance).
            instance_url (str): The URL for the Salesforce instance (client's salesforce instance).
        """
        self.access_token = access_token
        self.instance_url = instance_url


class SalesforceAgent:
    """
    Class for communicating with salesforce and gather data to our database
    """

    def __init__(
        self,
        sf_client: SalesforceClient,
        sf_users_repository: SalesforceUsersRepository,
    ):
        self.sf_client = sf_client
        self.sf_users_repository = sf_users_repository

    async def get_contacts(self):
        """
        Retrieve contacts from Salesforce.

        Returns:
        list: List of contact records.
        """

        url = f"{self.sf_client.instance_url}/services/data/v60.0/query/"
        query = "SELECT Id, FirstName, LastName, Email, Title, Account.Name FROM Contact LIMIT 100"
        headers = {
            "Authorization": f"Bearer {self.sf_client.access_token}",
            "Content-Type": "application/json",
        }
        params = {"q": query}

        try:
            response = requests.get(url, headers=headers, params=params)
            logger.info(f"Response: {response}")
            response.raise_for_status()
            contacts = response.json()["records"]
            for contact in contacts:
                if contact["Account"] is not None:
                    # Access the Name field of Account
                    contact["AccountName"] = contact["Account"]["Name"]
                else:
                    contact["AccountName"] = None
            return contacts
        except Exception as e:
            print(f"Failed to retrieve contacts: {e}")
            return []

        # if response.status_code == 200:
        #     results = response.json()
        #     if results["totalSize"] > 0:
        #         # Contact exists, so update it
        #         contact_id = results["records"][0]["Id"]
        #         update_response = await requests.patch(
        #             f"{customer_base_url}/services/data/v60.0/sobjects/Contact/{contact_id}",
        #             json=contact_data,
        #             headers=headers,
        #         )
        #         return update_response.json()
        #     else:
        #         # Contact does not exist, create a new one
        #         create_response = await requests.post(
        #             f"{customer_base_url}/services/data/v60.0/sobjects/Contact/",
        #             json=contact_data,
        #             headers=headers,
        #         )
        #         return create_response.json()
        # else:
        #     # Handle errors
        #     return response.json()


def get_authorization_url(company: str) -> str:
    """
    Returns the authorization URL for the Salesforce API.

    Args:
        company (str): The name of the company.

    Returns:
        str: The authorization URL.
    """
    logger.info(f"Getting authorization URL for {company}")
    sf = OAuth2Session(
        client_id=SALESFORCE_CLIENT_ID,
        redirect_uri=SALESFORCE_REDIRECT_URI,
    )
    authorization_url, _ = sf.authorization_url(SALESFORCE_LOGIN_URL)
    logger.debug(f"Authorization URL for {company}: {authorization_url}")
    return authorization_url


def handle_callback(company: str, response_url: str) -> None:
    logger.info(f"Started handling callback")
    sf = OAuth2Session(
        client_id=SALESFORCE_CLIENT_ID,
        redirect_uri=SALESFORCE_REDIRECT_URI,
    )
    token_data = sf.fetch_token(
        SALESFORCE_TOKEN_URL,
        client_secret=SALESFORCE_CLIENT_SECRET,
        authorization_response=response_url,
    )
    logger.info(
        f"Salesforce data updated for {company}. Client URL: {token_data['instance_url']}."
    )
    logger.info(f"About to insert new user to salesforce repository")

    logger.info(f"{token_data}")

    sf_users_repository.insert(
        uuid=get_uuid4(),
        name="Asaf",
        company="Definitely not Kubiya.ai",
        client_url=token_data["instance_url"],
        refresh_token=token_data["refresh_token"],
        access_token=token_data["access_token"],
    )

    logger.info(f"Inserted user to repository")

    return token_data


def create_salesforce_client(
    company_name: str,
    refresh_token: str,
) -> SalesforceClient | None:
    """
    Factory method to create a SalesforceClient object.

    Args:
        company_name (str): The name of the company.
        refresh_token (str): The refresh token for the company.
    Returns:
        SalesforceClient: The SalesforceClient object or None if the company is not found.
    """

    if not refresh_token:
        logger.warning(f"No Salesforce refresh token found for {company_name}.")
        return None
    sf = OAuth2Session(
        client_id=SALESFORCE_CLIENT_ID,
        redirect_uri=SALESFORCE_REDIRECT_URI,
    )
    token_data = sf.refresh_token(
        SALESFORCE_TOKEN_URL,
        refresh_token=refresh_token,
        client_id=SALESFORCE_CLIENT_ID,
        client_secret=SALESFORCE_CLIENT_SECRET,
    )
    return SalesforceClient(
        access_token=token_data["access_token"],
        instance_url=token_data["instance_url"],
    )
