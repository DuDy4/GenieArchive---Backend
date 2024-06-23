import os
import secrets

from fastapi import Depends, Request, HTTPException
from fastapi.routing import APIRouter
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from loguru import logger
from starlette.responses import PlainTextResponse, RedirectResponse

from redis import Redis

SELF_URL = os.environ.get("self_url", "https://localhost:3000")
PERSON_URL = os.environ.get("PERSON_URL", "https://localhost:8000")
logger.info(f"Self url: {SELF_URL}")

v1_router = APIRouter(prefix="/v1")

redis_client = Redis(host="localhost", port=6379, db=0)

PROFILE_ID = 0


@v1_router.get("/profiles/{uuid}", response_model=dict)
def get_profile(
    uuid: str,
):
    """
    Fetches and returns a specific profile.
    """

    logger.info("Got profile request")
    # Define the number of retries and backoff factor
    retries = Retry(total=5, backoff_factor=1)

    # Create a session
    session = requests.Session()

    # Mount the adapter to handle retries
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Disable SSL verification
    session.verify = False
    response = session.get(PERSON_URL + f"/v1/profiles/{uuid}")

    # Check if the request was successful
    if response.status_code == 200:
        # Return the response JSON
        return response.json()
    else:
        # Raise an HTTPException if the request was not successful
        raise HTTPException(status_code=response.status_code, detail=response.text)


@v1_router.get("/salesforce/auth/{company}", response_class=RedirectResponse)
def oauth_salesforce(company: str) -> RedirectResponse:
    """
    Triggers the salesforce oauth2.0 process
    """
    # Define the number of retries and backoff factor
    retries = Retry(total=5, backoff_factor=1)

    # Create a session
    session = requests.Session()

    # Mount the adapter to handle retries
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Disable SSL verification
    session.verify = False
    response = session.get(PERSON_URL + f"/v1/salesforce/auth/{company}")
    logger.info(f"Response: {response.json()}")

    authorization_url = response.json().url

    logger.info(f"Redirect url: {authorization_url}")
    return RedirectResponse(url=authorization_url)
