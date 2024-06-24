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
    response = session.get(
        PERSON_URL + f"/v1/salesforce/auth/{company}", allow_redirects=False
    )
    logger.info(f"Response: {response}")
    logger.info(f"Response status code: {response.status_code}")
    logger.info(f"Response headers: {response.headers}")

    if response.status_code == 307:  # 307 Temporary Redirect
        redirect_url = response.headers.get("Location")
        if redirect_url:
            logger.info(f"Redirecting to: {redirect_url}")
            return RedirectResponse(url=redirect_url)

    logger.error(f"Unexpected response status: {response.status_code}")
    return RedirectResponse(
        url="/error"
    )  # Redirect to an error page or handle accordingly


@v1_router.get("/salesforce/callback", response_class=PlainTextResponse)
def callback_salesforce(request: Request) -> PlainTextResponse:
    """
    Triggers the salesforce oauth2.0 callback process
    """
    # logger.debug(f"Request session: {request.session}")
    # logger.info(f"Received callback from salesforce oauth integration. Company: {request.session['salesforce_company']}"
    # )

    company = request.query_params.get("state")
    url = str(request.url)

    logger.info(f"Callback for company: {company}, url: {url}")

    retries = Retry(total=5, backoff_factor=1)

    # Create a session
    session = requests.Session()

    # Mount the adapter to handle retries
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Disable SSL verification
    session.verify = False
    response = session.get(
        PERSON_URL + f"/v1/salesforce/callback/{company}/{url}", allow_redirects=False
    )

    return PlainTextResponse(
        f"Successfully authenticated with salesforce for {company}. \nYou can now close this tab"
    )
