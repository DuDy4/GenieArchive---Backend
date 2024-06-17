"""
Module for interacting with the Salesforce API.
"""

import os
import uuid
from urllib.parse import urlencode

from dotenv import load_dotenv
import requests
from loguru import logger
from requests_oauthlib import OAuth2Session
from app_common.repositories.salesforce_users_repository import (
    SalesforceUsersRepository,
)
from app_common.dependencies.dependencies import salesforce_users_repository
from app_common.utils.str_utils import get_uuid4
from app_common.repositories.contacts_repository import ContactsRepository
from app_common.data_transfer_objects.person_dto import PersonDTO

from events.genie_event import GenieEvent
from events.topics import Topic

load_dotenv()

SELF_URL = os.environ.get("self_url", "https://localhost:3000")
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
        contacts_repository: ContactsRepository,
    ):
        self.sf_client = sf_client
        self.sf_users_repository = sf_users_repository
        self.contacts_repository = contacts_repository

    async def get_contacts(self):
        """
        Retrieve contacts from Salesforce.

        Returns:
        list: List of contact records.
        """
        url = f"{self.sf_client.instance_url}/services/data/v60.0/query/"
        query = "SELECT Id, FirstName, LastName, Email, Title, Account.Name, linkedInUrl__c FROM Contact LIMIT 100"
        headers = {
            "Authorization": f"Bearer {self.sf_client.access_token}",
            "Content-Type": "application/json",
        }
        params = {"q": query}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            contacts = response.json()["records"]
            logger.info(f"Retrieved contacts: {len(contacts)}")
            for contact in contacts:
                if contact["Account"] is not None:
                    contact["AccountName"] = contact["Account"]["Name"]
                else:
                    contact["AccountName"] = None
            changed_contacts = self.contacts_repository.handle_sf_contacts_list(
                contacts
            )
            logger.info(f"New contacts to handle: {changed_contacts}")
            if len(changed_contacts) > 0:
                handle_new_contacts_event(
                    changed_contacts
                )  # send new-contact events to eventhub
            return contacts
        except Exception as e:
            print(f"Failed to retrieve contacts: {e}")
            return []


# def get_all_fields(access_token, instance_url, object_name):
#     url = f"{instance_url}/services/data/v56.0/sobjects/{object_name}/describe"
#     headers = {
#         "Authorization": f"Bearer {access_token}",
#         "Content-Type": "application/json",
#     }
#
#     response = requests.get(url, headers=headers)
#     response.raise_for_status()
#     return response.json()


def handle_new_contacts_event(new_contacts: list[PersonDTO]):
    logger.info(f"Topic: {Topic.NEW_CONTACT}")
    try:
        for i in range(0, len(new_contacts)):
            contact = new_contacts[i].to_json()
            event = GenieEvent(
                Topic.NEW_CONTACT,
                contact,
                "public",
            )
            event.send()
    except Exception as e:
        logger.error(f"Error handling new contacts event: {e}")


def get_authorization_url(company: str) -> str:
    """
    Returns the authorization URL for the Salesforce API.

    Args:
        company (str): The name of the company.

    Returns:
        str: The authorization URL.
    """
    logger.info(f"Getting authorization URL for {company}")
    params = {
        "response_type": "code",
        "client_id": SALESFORCE_CLIENT_ID,
        "redirect_uri": SALESFORCE_REDIRECT_URI,
        "state": company,
    }
    return f"https://login.salesforce.com/services/oauth2/authorize?{urlencode(params)}"


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

    uuid = sf_users_repository.exists(company, token_data["instance_url"])
    if uuid:
        sf_users_repository.update_token(
            uuid, token_data["refresh_token"], token_data["access_token"]
        )
        logger.info(f"Updated user in repository")
        return token_data
    else:
        sf_users_repository.insert(
            uuid=get_uuid4(),
            company=company,
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
