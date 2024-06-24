import os

from fastapi import Depends, Request
from fastapi.routing import APIRouter
from loguru import logger
from starlette.responses import PlainTextResponse, RedirectResponse

from data.data_common.salesforce.salesforce_event_handler import SalesforceEventHandler
from data.data_common.salesforce.salesforce_integrations_manager import (
    handle_callback,
    get_authorization_url,
    create_salesforce_client,
    SalesforceAgent,
)
from data.data_common.repositories.salesforce_users_repository import (
    SalesforceUsersRepository,
)
from data.data_common.repositories.profiles_repository import ProfilesRepository
from data.data_common.dependencies.dependencies import (
    profiles_repository,
    contacts_repository,
    salesforce_event_handler,
    salesforce_users_repository,
)
from data.data_common.events.topics import Topic

from redis import Redis

SELF_URL = os.environ.get("PERSON_URL", "https://localhost:8000")
logger.info(f"Self url: {SELF_URL}")

v1_router = APIRouter(prefix="/v1")

redis_client = Redis(host="localhost", port=6379, db=0)


@v1_router.get("/profiles/{uuid}", response_model=dict)
def get_profile(
    uuid: str,
    profiles_repository: ProfilesRepository = Depends(profiles_repository),
):
    """
    Fetches and returns a specific profile.
    """
    logger.info("Got profile request")
    profile = profiles_repository.get_profile_data(uuid)
    logger.info(f"Got profile: {profile}")
    if profile:
        return profile.to_dict()
    else:
        return {"error": "Profile not found"}


@v1_router.get("/salesforce/auth/{company}", response_class=RedirectResponse)
def oauth_salesforce(request: Request, company: str) -> RedirectResponse:
    """
    Triggers the salesforce oauth2.0 process
    """
    logger.debug(f"request.session before start: {request.session}")

    logger.info(f"Beginning salesforce oauth integration for {company}")
    request.session["salesforce_company"] = company

    authorization_url = get_authorization_url(company) + f"&company={company}"
    logger.info(f"Redirect url: {authorization_url}")
    return RedirectResponse(url=authorization_url)


@v1_router.get("/salesforce/callback", response_class=PlainTextResponse)
def callback_salesforce(
    request: Request,
    state: str,
) -> PlainTextResponse:
    """
    Triggers the salesforce oauth2.0 callback process
    """
    # logger.debug(f"Request session: {request.session}")
    # logger.info(f"Received callback from salesforce oauth integration. Company: {request.session['salesforce_company']}"
    # )
    logger.info(
        f"Received callback from salesforce oauth integration. Company: {state}"
    )
    #  company's name supposed to be save in the state parameter
    company = request.query_params.get("state")

    token_data = handle_callback(company, str(request.url))

    return PlainTextResponse(
        f"Successfully authenticated with salesforce for {company}. \nYou can now close this tab"
    )


@v1_router.get("salesforce/topics", response_model=dict)
def get_all_topics():
    """
    Fetches and returns a specific profile.
    """
    logger.info("Got topic request")
    return Topic


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
    Endpoint to receive and process salesforce Platform Events.
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
