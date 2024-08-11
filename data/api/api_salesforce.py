import asyncio
import json
import os
import time
import traceback
import datetime
import requests
import urllib.parse


from fastapi import Depends, FastAPI, Request, HTTPException, Query
from fastapi.routing import APIRouter
from loguru import logger
from starlette.responses import PlainTextResponse, RedirectResponse, JSONResponse
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleRequest
from googleapiclient.discovery import build
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from google.oauth2 import id_token
from google.auth import credentials


from common.utils import env_utils
from data.api.base_models import *
from data.data_common.repositories.tenants_repository import TenantsRepository
from data.data_common.repositories.profiles_repository import ProfilesRepository
from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.repositories.google_creds_repository import GoogleCredsRepository

from data.api.mock_api import profiles, meetings

from data.data_common.salesforce.salesforce_event_handler import SalesforceEventHandler
from data.data_common.salesforce.salesforce_integrations_manager import (
    handle_callback,
    get_authorization_url,
    create_salesforce_client,
    SalesforceAgent,
    handle_new_contacts_event,
)

from data.data_common.dependencies.dependencies import (
    profiles_repository,
    contacts_repository,
    salesforce_event_handler,
    tenants_repository,
    meetings_repository,
    google_creds_repository,
    ownerships_repository,
    persons_repository,
    personal_data_repository,
    hobbies_repository,
)

from data.data_common.events.topics import Topic
from data.data_common.events.genie_event import GenieEvent
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.utils.str_utils import get_uuid4

from data.meetings_consumer import MeetingManager

SELF_URL = env_utils.get("PERSON_URL", "https://localhost:8000")
logger.info(f"Self url: {SELF_URL}")

v1_router = APIRouter(prefix="/v1")


@v1_router.get(
    "/salesforce/auth/{tenantId}",
    response_class=RedirectResponse,
    summary="Initiates Salesforce OAuth2.0 process",
    include_in_schema=False,
)
def initiate_salesforce_oauth(request: Request, tenantId: str) -> RedirectResponse:
    """
    Initiates the Salesforce OAuth2.0 process
    """
    logger.debug(f"Request session before start: {request.session}")

    logger.info(f"Starting Salesforce OAuth integration for {tenantId}")
    request.session["salesforce_tenantId"] = tenantId

    authorization_url = get_authorization_url(tenantId) + f"&state={tenantId}"
    logger.info(f"Redirect URL: {authorization_url}")
    return RedirectResponse(url=authorization_url)


@v1_router.get(
    "/salesforce/callback",
    response_class=PlainTextResponse,
    summary="Handles Salesforce OAuth2.0 callback",
    include_in_schema=False,
)
def handle_salesforce_callback(request: Request) -> PlainTextResponse:
    """
    Handles the Salesforce OAuth2.0 callback process
    """
    tenant_id = request.query_params.get("state")
    url = request.url

    logger.debug(f"URL: {url}")
    logger.debug(f"Tenant ID: {tenant_id}")

    token_data = handle_callback(tenant_id, str(url))

    json_to_app = {
        "client_url": token_data.get("instance_url"),
        "refresh_token": token_data.get("refresh_token"),
        "access_token": token_data.get("access_token"),
    }

    logger.info(f"Token data: {json_to_app}")

    return PlainTextResponse(
        f"Successfully authenticated with Salesforce for {tenant_id}. \nYou can now close this tab"
    )


@v1_router.delete(
    "/salesforce/credentials/{tenantId}",
    response_model=dict,
    summary="Deletes Salesforce credentials for a tenant",
    include_in_schema=False,
)
def remove_salesforce_credentials(
    tenantId: str,
    tenants_repository=Depends(tenants_repository),
):
    """
    Deletes the Salesforce credentials for a given tenant.
    """
    logger.info(f"Deleting Salesforce credentials for tenant: {tenantId}")
    tenants_repository.delete_salesforce_credentials(tenantId)

    result = tenants_repository.get_refresh_token(tenantId)
    if not result:
        return {"status": "success"}
    else:
        return {"status": "error"}


@v1_router.post(
    "/salesforce/deploy-apex/{company}",
    response_model=dict,
    summary="Deploys Apex code via Salesforce platform events",
    include_in_schema=False,
)
async def deploy_salesforce_apex_code(
    request: Request,
    company: str,
    tenants_repository=Depends(tenants_repository),
    contacts_repository=Depends(contacts_repository),
):
    """
    Deploys Apex code via Salesforce platform events.
    """
    try:
        refresh_token = tenants_repository.get_refresh_token(company)
        logger.info(f"Refresh token: {refresh_token}")
        salesforce_client = create_salesforce_client(company, refresh_token)

        salesforce_agent = SalesforceAgent(
            salesforce_client, tenants_repository, contacts_repository
        )

        result = salesforce_agent.deploy_apex_code()
        logger.debug(f"Deployed Apex code: {result}")
        if result:
            return {"status": "success", "message": "Event processed"}
        else:
            return {"status": "error", "message": "Failed"}
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"status": "error", "message": str(e)}


@v1_router.get(
    "/salesforce/topics",
    response_model=dict,
    summary="Fetches all Salesforce topics",
    include_in_schema=False,
)
def fetch_salesforce_topics():
    """
    Fetches all Salesforce topics.
    """
    logger.info("Received topic request")
    return Topic


@v1_router.get(
    "/salesforce/{tenant_id}/contacts",
    response_class=PlainTextResponse,
    summary="Fetches all Salesforce contacts for a tenant",
    include_in_schema=False,
)
async def fetch_salesforce_contacts(
    request: Request,
    tenant_id: str,
    tenants_repository=Depends(tenants_repository),
    contacts_repository=Depends(contacts_repository),
) -> PlainTextResponse:
    """
    Fetches all Salesforce contacts for a given tenant.
    """
    logger.info(f"Received get contacts request")

    refresh_token = tenants_repository.get_refresh_token(tenant_id)
    logger.info(f"Refresh token: {refresh_token}")
    salesforce_client = create_salesforce_client(tenant_id, refresh_token)

    salesforce_agent = SalesforceAgent(
        salesforce_client, tenants_repository, contacts_repository
    )
    contacts = await salesforce_agent.get_contacts(tenant_id)

    logger.info(f"Fetched contacts: {len(contacts)}")
    return PlainTextResponse(f"Fetched contacts: {len(contacts)}")


@v1_router.post(
    "/salesforce/webhook",
    response_model=dict,
    summary="Processes Salesforce platform events via webhook",
    include_in_schema=False,
)
async def process_salesforce_webhook(
    request: Request,
    salesforce_event_handler: SalesforceEventHandler = Depends(
        salesforce_event_handler
    ),
):
    """
    Processes Salesforce platform events via webhook.
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


@v1_router.get(
    "/salesforce/contacts/{tenant_id}",
    response_class=JSONResponse,
    summary="Fetches all Salesforce contacts for a tenant",
    include_in_schema=False,
)
async def fetch_salesforce_contacts_for_tenant(
    request: Request,
    tenant_id: str,
    tenants_repository=Depends(tenants_repository),
    contacts_repository=Depends(contacts_repository),
) -> JSONResponse:
    """
    Fetches all Salesforce contacts for a given tenant.
    """
    logger.info(f"Received get contacts request")

    refresh_token = tenants_repository.get_refresh_token(tenant_id)
    logger.info(f"Refresh token: {refresh_token}")
    salesforce_client = create_salesforce_client(tenant_id, refresh_token)

    salesforce_agent = SalesforceAgent(
        salesforce_client, tenants_repository, contacts_repository
    )
    contacts = await salesforce_agent.get_contacts(tenant_id)

    logger.info(f"Fetched contacts: {len(contacts)}")
    return JSONResponse(content=contacts)


@v1_router.post(
    "/salesforce/profiles/{tenant_id}",
    response_class=PlainTextResponse,
    summary="Processes contacts and builds profiles",
    include_in_schema=False,
)
async def process_salesforce_contacts(
    request: Request,
    tenant_id: str,
    tenants_repository=Depends(tenants_repository),
    contacts_repository=Depends(contacts_repository),
) -> PlainTextResponse:
    """
    Processes contacts and builds profiles.
    """
    try:
        logger.info(f"Received process contacts request")
        logger.debug(f"Tenant ID: {tenant_id}")
        contact_ids = await request.json()
        logger.debug(f"Contact IDs: {contact_ids}")

        contacts = [
            contacts_repository.get_contact_by_salesforce_id(tenant_id, contact_id)
            for contact_id in contact_ids
        ]
        logger.debug(f"Contacts: {contacts}")

        handle_new_contacts_event(contacts)

        return PlainTextResponse(f"Processed contacts: {len(contact_ids)}")
    except Exception as e:
        logger.error(f"Error: {e}")
        traceback.print_exc()
        return PlainTextResponse(f"Error: {e}")
