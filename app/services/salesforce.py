"""
Module for interacting with the Salesforce API.
"""

import os
from dotenv import load_dotenv
import requests
from loguru import logger
from requests_oauthlib import OAuth2Session
from simple_salesforce import Salesforce

load_dotenv()


SELF_URL = os.environ.get("self_url", "https://localhost:8444")
SALESFORCE_CLIENT_ID = os.environ.get("SALESFORCE_CLIENT_ID")
SALESFORCE_CLIENT_SECRET = os.environ.get("SALESFORCE_CLIENT_SECRET")
SALESFORCE_LOGIN_URL = os.environ.get("SALESFORCE_LOGIN_URL")
SALESFORCE_REDIRECT_URI = SELF_URL + "/v1/salesforce/callback"
SALESFORCE_TOKEN_URL = os.environ.get("SALESFORCE_TOKEN_URL")


class SalesforceClient:
    """
    Class for interacting with the Salesforce API on behalf of a company.
    """

    def __init__(self, access_token: str, instance_url: str):
        """
        Initializes the SalesforceClient with the given parameters.

        Args:
            access_token (str): The access token for the Salesforce (for accessing client's salesforce instance).
            instance_url (str): The URL for the Salesforce instance (client's salesforce instance).
        """
        self.access_token = access_token
        self.instance_url = instance_url


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
