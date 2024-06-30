"""
Module for interacting with the salesforce API.
"""
import base64
import io
import json
import os
import traceback
import uuid
import zipfile
from urllib.parse import urlencode

from dotenv import load_dotenv
import requests
from simple_salesforce import Salesforce
from loguru import logger
from requests_oauthlib import OAuth2Session
from data.data_common.repositories.salesforce_users_repository import (
    SalesforceUsersRepository,
)
from data.data_common.dependencies.dependencies import salesforce_users_repository
from data.data_common.utils.str_utils import get_uuid4
from data.data_common.repositories.contacts_repository import ContactsRepository
from data.data_common.data_transfer_objects.person_dto import PersonDTO

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

from data.data_common.salesforce.deployment_code import (
    trigger_code,
    class_code,
    metadata_package,
)

load_dotenv()

SELF_URL = os.environ.get("PERSON_URL", "https://localhost:8000")
SALESFORCE_CLIENT_ID = os.environ.get("SALESFORCE_CLIENT_ID")
SALESFORCE_CLIENT_SECRET = os.environ.get("SALESFORCE_CLIENT_SECRET")
SALESFORCE_LOGIN_URL = os.environ.get("SALESFORCE_LOGIN_URL")
SALESFORCE_REDIRECT_URI = SELF_URL + "/v1/salesforce/callback"
SALESFORCE_TOKEN_URL = os.environ.get("SALESFORCE_TOKEN_URL")

sf_users_repository = salesforce_users_repository()


class SalesforceClient:
    """
    Class for interacting with the salesforce API on behalf of a company.
    """

    def __init__(
        self,
        access_token: str,
        instance_url: str,
    ):
        """
        Initializes the SalesforceClient with the given parameters.

        Args:
            access_token (str): The access token for the salesforce (for accessing client's salesforce instance).
            instance_url (str): The URL for the salesforce instance (client's salesforce instance).
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

    def initialize_simple_sf_client(self, company):
        refresh_token = self.sf_users_repository.get_refresh_token(company)
        self.sf_client = Salesforce(refresh_token=refresh_token, domain="test")

    def verify_access_token(self):
        url = f"{self.sf_client.instance_url}/services/data/v61.0/query/"
        query = "SELECT Id FROM Account LIMIT 1"
        headers = {
            "Authorization": f"Bearer {self.sf_client.access_token}",
            "Content-Type": "application/json",
        }
        params = {"q": query}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return True
        except requests.exceptions.HTTPError:
            return False

    async def get_contacts(self, tenant_id: str):
        """
        Retrieve contacts from salesforce.

        Returns:
        list: List of contact records.
        """
        url = f"{self.sf_client.instance_url}/services/data/v61.0/query/"
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
                tenant_id, contacts
            )
            logger.info(f"New contacts to handle: {changed_contacts}")
            # if len(changed_contacts) > 0:
            #     handle_new_contacts_event(
            #         changed_contacts
            #     )  # send new-contact events to eventhub
            return contacts
        except Exception as e:
            print(f"Failed to retrieve contacts: {e}")
            return []

    def deploy_apex_code(self):
        zip_buffer = create_zip(trigger_code, class_code, metadata_package)
        encoded_zip = base64.b64encode(zip_buffer.read()).decode("utf-8")
        url = f"{self.sf_client.instance_url}/services/Soap/m/61.0"
        headers = {
            "Authorization": f"Bearer {self.sf_client.access_token}",
            "Content-Type": "text/xml",
            "SOAPAction": "deploy",
        }
        files = {"file": ("deploy.zip", zip_buffer, "application/zip")}
        deploy_envelope = f"""<?xml version="1.0" encoding="utf-8" ?>
            <env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/" xmlns="http://soap.sforce.com/2006/04/metadata">
              <env:Body>
                <deploy>
                  <ZipFile>{encoded_zip}</ZipFile>
                  <DeployOptions>
                    <performRetrieve>false</performRetrieve>
                    <purgeOnDelete>false</purgeOnDelete>
                    <rollbackOnError>true</rollbackOnError>
                    <singlePackage>true</singlePackage>
                  </DeployOptions>
                </deploy>
              </env:Body>
            </env:Envelope>"""
        try:
            logger.debug(f"Deploying Apex code to {url}")
            # logger.debug(f"Files: {files}")
            # logger.debug(f"deploy_envelope: {deploy_envelope}")
            # logger.debug(f"trigger_code: {trigger_code}")
            # logger.debug(f"class_code: {class_code}")

            # simple_sf_client = Salesforce(instance_url=self.sf_client.instance_url,
            #                               session_id=self.sf_client.access_token,
            #                               domain='test')  # or 'login' for production
            #
            # logger.debug(f"Simple SF client: {simple_sf_client}")
            #
            # response = simple_sf_client._call_soap(
            #     action='deploy',
            #     message=deploy_envelope,
            #     headers=headers,
            #     api_version='61.0'
            # )
            # logger.debug(f"Response status code: {response.status_code}")
            # logger.debug(f"Response text: {response.text}")
            # response.raise_for_status()
            # logger.info(f"Deployed Apex code: {response.text}")
            # if not (response.status_code == 401 or 'INVALID_SESSION_ID' in response.text):
            #     return response.text

            if not self.verify_access_token():
                logger.info("Access token invalid. Refreshing access token.")
                current_access_token = self.sf_client.access_token
                refresh_token = (
                    self.sf_users_repository.get_refresh_token_by_access_token(
                        current_access_token
                    )
                )
                self.sf_client.access_token = get_new_access_token(refresh_token)
            else:
                logger.info("Access token valid.")
            response = requests.post(url, headers=headers, data=deploy_envelope)
            logger.debug(f"Response status code: {response.status_code}")
            logger.debug(f"Response text: {response.text}")

            if response.status_code == 401 or "INVALID_SESSION_ID" in response.text:
                # Refresh the access token
                logger.info("Session ID invalid. Refreshing access token.")
                current_access_token = self.sf_client.access_token
                refresh_token = (
                    self.sf_users_repository.get_refresh_token_by_access_token(
                        current_access_token
                    )
                )
                self.sf_client.access_token = get_new_access_token(refresh_token)
                logger.debug(f"New access token: {self.sf_client.access_token}")
                headers["Authorization"] = f"Bearer {self.sf_client.access_token}"
                response = requests.post(url, headers=headers, data=deploy_envelope)
                logger.debug(
                    f"Response status code after token refresh: {response.status_code}"
                )
                logger.debug(f"Response text after token refresh: {response.text}")

            response.raise_for_status()
            logger.info(f"Deployed Apex code: {response.text}")
            logger.info(f"Deployed Apex code: {response.json()}")
            return response.json()
        except Exception as e:
            logger.error(f"Failed to deploy Apex code: {e}")
            traceback.print_exc()
            return None


def create_zip(trigger_code, class_code, metadata_package):
    # Create a ZIP file in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr(
            "unpackaged/triggers/ContactChangeEventTrigger.trigger", trigger_code
        )
        zip_file.writestr(
            "unpackaged/triggers/ContactChangeEventTrigger.trigger-meta.xml",
            """<?xml version="1.0" encoding="UTF-8"?>
            <Trigger xmlns="http://soap.sforce.com/2006/04/metadata">
                <apiVersion>61.0</apiVersion>
                <status>Active</status>
            </Trigger>""",
        )
        zip_file.writestr(
            "unpackaged/classes/ContactChangeEventHandler.cls", class_code
        )
        zip_file.writestr(
            "unpackaged/classes/ContactChangeEventHandler.cls-meta.xml",
            """<?xml version="1.0" encoding="UTF-8"?>
            <ApexClass xmlns="http://soap.sforce.com/2006/04/metadata">
                <apiVersion>61.0</apiVersion>
                <status>Active</status>
            </ApexClass>""",
        )
        zip_file.writestr("unpackaged/package.xml", metadata_package)
    zip_buffer.seek(0)
    return zip_buffer


def handle_new_contacts_event(new_contacts: list[PersonDTO | dict]):
    logger.info(f"Topic: {Topic.NEW_CONTACT}")
    try:
        for i in range(0, len(new_contacts)):
            if isinstance(new_contacts[i], PersonDTO):
                contact = new_contacts[i].to_json()
            if isinstance(new_contacts[i], dict):
                contact = json.dumps(new_contacts[i])
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
    Returns the authorization URL for the salesforce API.

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


def get_new_access_token(refresh_token: str) -> str:
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
    access_token = token_data["access_token"]
    logger.debug(f"New access token: {access_token}")
    return access_token


def handle_callback(company: str, response_url: str) -> dict:
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
        f"salesforce data updated for {company}. Client URL: {token_data['instance_url']}."
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
        logger.warning(f"No salesforce refresh token found for {company_name}.")
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
