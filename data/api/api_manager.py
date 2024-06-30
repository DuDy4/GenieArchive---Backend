import os
import traceback

import requests
from fastapi import Depends, Request
from fastapi.routing import APIRouter
from loguru import logger
from requests.adapters import HTTPAdapter
from starlette.responses import PlainTextResponse, RedirectResponse, JSONResponse
from urllib3 import Retry

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
APP_URL = os.environ.get("APP_URL", "https://localhost:3000")
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


@v1_router.get("/salesforce/auth/{tenantId}", response_class=RedirectResponse)
def oauth_salesforce(request: Request, tenantId: str) -> RedirectResponse:
    """
    Triggers the salesforce oauth2.0 process
    """
    logger.debug(f"request.session before start: {request.session}")

    logger.info(f"Beginning salesforce oauth integration for {tenantId}")
    request.session["salesforce_tenantId"] = tenantId

    authorization_url = get_authorization_url(tenantId) + f"&tenantId={tenantId}"
    logger.info(f"Redirect url: {authorization_url}")
    return RedirectResponse(url=authorization_url)


@v1_router.get("/salesforce/callback", response_class=JSONResponse)
def callback_salesforce(
    request: Request,
) -> JSONResponse:
    """
    Triggers the salesforce oauth2.0 callback process
    """
    # logger.debug(f"Request session: {request.session}")
    # logger.info(f"Received callback from salesforce oauth integration. Company: {request.session['salesforce_company']}"
    # )
    #  company's name supposed to be save in the state parameter
    tenant_id = request.query_params.get("state")
    url = request.query_params.get("url")

    logger.debug(f"Tenant ID: {tenant_id}")

    token_data = handle_callback(tenant_id, str(url))

    json_to_app = {
        "client_url": token_data.get("instance_url"),
        "refresh_token": token_data.get("refresh_token"),
        "access_token": token_data.get("access_token"),
    }

    logger.info(f"Token data: {json_to_app}")

    return JSONResponse(content=json_to_app)


@v1_router.post("/salesforce/deploy-apex/{company}", response_model=dict)
async def salesforce_deploy_apex(
    request: Request,
    company: str,
    sf_users_repository=Depends(salesforce_users_repository),
    contacts_repository=Depends(contacts_repository),
):
    """
    Endpoint to receive and process salesforce Platform Events.
    """
    try:
        refresh_token = sf_users_repository.get_refresh_token(company)
        logger.info(f"refresh_token: {refresh_token}")
        salesforce_client = create_salesforce_client(company, refresh_token)

        salesforce_agent = SalesforceAgent(
            salesforce_client, sf_users_repository, contacts_repository
        )

        result = salesforce_agent.deploy_apex_code()
        logger.debug(f"Deployed apex code: {result}")
        if result:
            return {"status": "success", "message": "Event processed"}
        else:
            return {"status": "error", "message": "Failed"}
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"status": "error", "message": str(e)}


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


@v1_router.get("/salesforce/contacts/{tenant_id}", response_class=JSONResponse)
async def get_all_contact_for_tenant(
    request: Request,
    tenant_id: str,
    sf_users_repository=Depends(salesforce_users_repository),
    contacts_repository=Depends(contacts_repository),
) -> JSONResponse:
    """
    Triggers the salesforce oauth2.0 callback process
    """
    logger.info(f"Received get contacts request")

    refresh_token = sf_users_repository.get_refresh_token(tenant_id)
    logger.info(f"refresh_token: {refresh_token}")
    salesforce_client = create_salesforce_client(tenant_id, refresh_token)

    salesforce_agent = SalesforceAgent(
        salesforce_client, sf_users_repository, contacts_repository
    )
    contacts = await salesforce_agent.get_contacts(tenant_id)

    logger.info(f"Got contacts: {len(contacts)}")
    return JSONResponse(content=contacts)


@v1_router.get("/salesforce/contacts/{tenant_id}", response_class=JSONResponse)
async def get_all_contact_for_tenant(
    request: Request,
    tenant_id: str,
    sf_users_repository=Depends(salesforce_users_repository),
    contacts_repository=Depends(contacts_repository),
) -> JSONResponse:
    """
    Triggers the salesforce oauth2.0 callback process
    """
    logger.info(f"Received get contacts request")

    refresh_token = sf_users_repository.get_refresh_token(tenant_id)
    logger.info(f"refresh_token: {refresh_token}")
    salesforce_client = create_salesforce_client(tenant_id, refresh_token)

    salesforce_agent = SalesforceAgent(
        salesforce_client, sf_users_repository, contacts_repository
    )
    contacts = await salesforce_agent.get_contacts(tenant_id)

    logger.info(f"Got contacts: {len(contacts)}")
    return JSONResponse(content=contacts)


@v1_router.post(
    "/salesforce/handle-contacts/{tenant_id}", response_class=PlainTextResponse
)
async def process_contacts(
    request: Request,
    tenant_id: str,
    sf_users_repository=Depends(salesforce_users_repository),
    contacts_repository=Depends(contacts_repository),
) -> PlainTextResponse:
    """
    Triggers the salesforce oauth2.0 callback process
    """
    try:

        logger.info(f"Received get contacts request")
        logger.debug(f"Tenant ID: {tenant_id}")
        contact_ids = await request.json()
        logger.debug(f"Contact IDs: {contact_ids}")

        contacts = [
            contacts_repository.get_contact_by_salesforce_id(tenant_id, contact_id)
            for contact_id in contact_ids
        ]
        logger.debug(f"Contacts: {contacts}")

        return PlainTextResponse(f"Got contacts: {len(contact_ids)}")
    except Exception as e:
        logger.error(f"Error: {e}")
        traceback.print_exc()
        return PlainTextResponse(f"Error: {e}")
