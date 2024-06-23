import os

from fastapi import Depends, Request, HTTPException
from fastapi.routing import APIRouter
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from loguru import logger
from starlette.responses import PlainTextResponse, RedirectResponse
from app_common.dependencies.dependencies import (
    contacts_repository,
    salesforce_users_repository,
    salesforce_event_handler,
)

from redis import Redis

from services.salesforce import (
    get_authorization_url,
    handle_callback,
    create_salesforce_client,
    SalesforceAgent,
)

from app_common.consumers.salesforce_event_handler import SalesforceEventHandler

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


# @v1_router.post("/profiles/", response_model=str)
# async def insert_new_profile(request: Request):
#     """
#     Fetches and returns a specific profile.
#     """
#     request_body = await request.json()
#     logger.info(f"Request body {request_body}")
#     uuid = request_body.get("uuid")
#     name = request_body.get("name")
#     company = request_body.get("company")
#     logger.info("Got profile POST request")
#     return f"Added new profile: {uuid}: {name} who works at {company}"
#
#
# @v1_router.delete("/profiles/{uuid}", response_model=str)
# def delete_profile(
#     uuid: str,
# ):
#     """
#     Fetches and returns a specific profile.
#     """
#     logger.info("Got profile DELETE request")
#     return f"Deleted profile: {uuid}"
#
#
# @v1_router.put("/profiles/{uuid}", response_model=str)
# async def update_profile(uuid: str, request: Request):
#     """
#     Fetches and returns a specific profile.
#     """
#     request_body = await request.json()
#     logger.info(f"Request body {request_body}")
#     name = request_body.get("name")
#     company = request_body.get("company")
#     if not name or not company:
#         return "Name and company are required"
#     logger.info("Got profile PUT request")
#     return f"Updated profile: {uuid}: {name} who works at {company}"


@v1_router.get("/salesforce/auth/{company}", response_class=RedirectResponse)
def oauth_salesforce(request: Request, company: str) -> RedirectResponse:
    """
    Triggers the salesforce oauth2.0 process
    """
    logger.debug(f"request.session before start: {request.session}")

    logger.info(f"Beginning salesforce oauth integration for {company}")
    request.session["salesforce_company"] = company

    state = secrets.token_urlsafe(32)
    request.session["salesforce_state"] = state  # Store state in session

    context["salesforce_company"] = company
    logger.debug(f"Context: {context['salesforce_company']}")

    authorization_url = get_authorization_url(company) + f"&company={company}"
    logger.info(f"Redirect url: {authorization_url}")
    return RedirectResponse(url=authorization_url)


@v1_router.get("/salesforce/callback", response_class=PlainTextResponse)
def callback_salesforce(request: Request) -> PlainTextResponse:
    """
    Triggers the salesforce oauth2.0 callback process
    """
    # logger.debug(f"Request session: {request.session}")
    # logger.info(f"Received callback from salesforce oauth integration. Company: {request.session['salesforce_company']}"
    # )

    #  company's name supposed to be save in the state parameter
    company = request.query_params.get("state")

    token_data = handle_callback(company, str(request.url))

    return PlainTextResponse(
        f"Successfully authenticated with Salesforce for {company}. \nYou can now close this tab"
    )


@v1_router.get("/salesforce/{company}/contact", response_class=PlainTextResponse)
async def get_all_contact(
    request: Request,
    company: str,
    sf_users_repository=Depends(salesforce_users_repository),
    contacts_repository=Depends(contacts_repository),
) -> PlainTextResponse:
    """
    Triggers the salesforce oauth2.0 callback process
    """
    logger.info(f"Received get contacts request")

    refresh_token = sf_users_repository.get_refresh_token(company)
    logger.info(f"refresh_token: {refresh_token}")
    salesforce_client = create_salesforce_client(company, refresh_token)

    salesforce_agent = SalesforceAgent(
        salesforce_client, sf_users_repository, contacts_repository
    )
    contacts = await salesforce_agent.get_contacts()

    logger.info(f"Got contacts: {len(contacts)}")
    return PlainTextResponse(f"Got contacts: {len(contacts)}")


@v1_router.post("/salesforce/webhook", response_model=dict)
async def salesforce_webhook(
    request: Request,
    salesforce_event_handler: SalesforceEventHandler = Depends(
        salesforce_event_handler
    ),
):
    """
    Endpoint to receive and process Salesforce Platform Events.
    """
    try:
        event_data = await request.json()
        logger.info(f"Received event data: {event_data}")

        result = salesforce_event_handler.handle_event(event_data)
        if result:
            return {"status": "success", "message": "Event processed"}
        else:
            return {"status": "error", "message": "Failed to process event"}
    except Exception as e:
        logger.error(f"Error processing webhook event: {e}")
        return {"status": "error", "message": str(e)}
