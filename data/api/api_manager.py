import json
import os
import traceback
import datetime

import requests

from fastapi import Depends, FastAPI, Request, HTTPException, Query
from fastapi.routing import APIRouter
from loguru import logger
from starlette.responses import PlainTextResponse, RedirectResponse, JSONResponse
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleRequest
from googleapiclient.discovery import build

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

from redis import Redis

from data.meetings_consumer import MeetingManager

SELF_URL = os.environ.get("PERSON_URL", "https://localhost:8000")
logger.info(f"Self url: {SELF_URL}")

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
REDIRECT_URI = f"{SELF_URL}/v1/google-callback"
GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"

v1_router = APIRouter(prefix="/v1")

redis_client = Redis(host="localhost", port=6379, db=0)


@v1_router.get("/users", response_model=UserResponse)
def get_user(request: Request):
    """
    Returns a tetant object - MOCK.
    """
    tenant = {
        "tenantId": "TestOwner",
        "name": "Dan Shevel",
        "email": "dan.shevel@genie.ai",
    }
    return JSONResponse(content=tenant)


@v1_router.post(
    "/users/signin",
    response_model=dict,
    summary="Creates a new user account",
    include_in_schema=False,
)
async def get_user_account(
    request: Request,
    tenants_repository: TenantsRepository = Depends(tenants_repository),
):
    """
    Get user account.

    This endpoint allows the creation of a new user account. It expects a JSON request body
    containing `tenantId` and `name`. If the user already exists, it returns the existing
    Salesforce credentials. Otherwise, it creates a new user account and returns the newly
    created user's UUID along with Salesforce credentials.

    - **request**: The request object.
    - **tenants_repository**: Dependency that provides access to the tenants repository.

    Returns:
        - **message**: Success or error message.
        - **salesforce_creds**: Salesforce credentials if the user already exists or after creation.
    """
    try:
        logger.debug(f"Received signup request: {request}")
        data = await request.json()
        logger.debug(f"Received signup data: {data}")
        tenants_repository.create_table_if_not_exists()
        uuid = tenants_repository.exists(data.get("tenantId"), data.get("name"))

        if uuid:
            logger.info(f"User already exists in database")
            salesforce_creds = tenants_repository.get_salesforce_credentials(
                data.get("tenantId")
            )
            logger.debug(f"Salesforce creds: {salesforce_creds}")
            return {
                "message": "User already exists in database",
                "salesforce_creds": salesforce_creds,
            }
        uuid = tenants_repository.insert(data)
        logger.debug(f"User account created successfully with uuid: {uuid}")

        salesforce_creds = tenants_repository.get_salesforce_credentials(
            data.get("tenantId")
        )
        logger.debug(f"Salesforce creds: {salesforce_creds}")
        return {
            "message": f"User account created successfully with uuid: {uuid}",
            "salesforce_creds": salesforce_creds,
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@v1_router.get(
    "/profile/{uuid}",
    response_model=ProfileResponse,
    summary="Fetches and returns a specific profile",
    include_in_schema=False,
)
def get_profile(
    uuid: str,
    profiles_repository: ProfilesRepository = Depends(profiles_repository),
):
    """
    Fetches and returns a specific profile.
    """
    logger.info("Received profile request")
    profile = profiles_repository.get_profile_data(uuid)
    logger.info(f"Fetched profile: {profile}")
    if profile:
        return profile.to_dict()
    else:
        return {"error": "Profile not found"}


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


@v1_router.get(
    "/meetings/{tenant_id}",
    response_model=MeetingsListResponse,
    summary="Gets all *meeting* that the tenant has profiles participants in",
)
async def get_all_meetings_by_profile_name(
    request: Request,
    tenant_id: str,
    name: str = Query(None, description="Partial text to search profile names"),
    ownerships_repository=Depends(ownerships_repository),
    persons_repository=Depends(persons_repository),
    meetings_repository=Depends(meetings_repository),
) -> MeetingsListResponse:
    """
    Gets all *meeting* that the tenant has profiles participants in.

    Steps:
    1. Get all persons for the tenant.
    2. Get all emails for the persons with name that includes the search text.
    3. Get all meetings with participants that have the emails.

    """
    logger.info(f"Received get profiles request, with search: '{name}'")

    persons_uuid = ownerships_repository.get_all_persons_for_tenant(tenant_id)
    logger.info(f"Got persons_uuid: {persons_uuid}")
    persons_emails = persons_repository.get_emails_list(persons_uuid, name)
    logger.info(f"Got persons_emails: {persons_emails}")
    meetings = meetings_repository.get_meetings_by_participants_emails(persons_emails)
    dict_meetings = [meeting.to_dict() for meeting in meetings]
    logger.info(f"Got meetings: {dict_meetings}")

    return JSONResponse(content=dict_meetings)


@v1_router.get("/meetings/{tenant_id}", response_model=MeetingsListResponse)
def get_all_meetings(
    tenant_id: str,
    meetings_repository=Depends(meetings_repository),
) -> MeetingsListResponse:
    """
    Gets all meetings for a given tenant.
    """
    logger.info(f"Received meetings request for tenant: {tenant_id}")
    meetings = meetings_repository.get_all_meetings_by_tenant_id(tenant_id)
    logger.info(f"Got meetings: {len(meetings)}")
    meetings_list = [meeting.to_dict() for meeting in meetings]
    return MeetingsListResponse(meetings=meetings_list)


@v1_router.get(
    "/oauth/google", summary="Initiates Google OAuth process", include_in_schema=False
)
async def initiate_google_oauth(request: Request):
    """
    Initiates Google OAuth process.
    """
    authorization_url = (
        "https://accounts.google.com/o/oauth2/v2/auth"
        "?response_type=code"
        f"&client_id={GOOGLE_CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        "&scope=https://www.googleapis.com/auth/calendar"
        "&access_type=offline"
        "&prompt=consent"
    )
    return RedirectResponse(url=authorization_url)


@v1_router.get(
    "/oauth/google-callback",
    summary="Handles Google OAuth callback",
    include_in_schema=False,
)
async def handle_google_oauth_callback(
    request: Request,
    google_creds_repository: GoogleCredsRepository = Depends(google_creds_repository),
):
    """
    Handles Google OAuth callback.
    """
    code = request.query_params.get("code")
    if not code:
        raise HTTPException(status_code=400, detail="Code not found in request")

    token_url = "https://oauth2.googleapis.com/token"
    token_data = {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }

    token_response = requests.post(token_url, data=token_data)
    if token_response.status_code != 200:
        logger.error(f"Token request failed: {token_response.text}")
        raise HTTPException(status_code=400, detail="Failed to fetch token")

    tokens = token_response.json()
    logger.debug(f"Tokens: {tokens}")
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")

    # Store tokens in the database
    tenant_id = "asaf-savich"  # Replace with actual tenant ID

    google_creds_repository.insert(
        {
            "uuid": "example-uuid",
            "tenantId": tenant_id,
            "accessToken": access_token,
            "refreshToken": refresh_token,
        }
    )

    return JSONResponse(
        content={"message": "OAuth flow completed", "access token": access_token}
    )


@v1_router.get(
    "/google/credentials/{tenant_id}",
    response_class=JSONResponse,
    summary="Fetches Google credentials for a tenant",
    include_in_schema=False,
)
def fetch_google_credentials(
    tenant_id: str,
    google_creds_repository=Depends(google_creds_repository),
) -> JSONResponse:
    """
    Fetches Google credentials for a given tenant.
    """
    logger.info(f"Received Google credentials request for tenant: {tenant_id}")
    creds = google_creds_repository.get_creds(tenant_id)
    logger.info(f"Fetched Google credentials: {creds}")
    return JSONResponse(content=creds)


@v1_router.get(
    "/google/meetings/{tenant_id}",
    response_class=JSONResponse,
    summary="Fetches all Google Calendar meetings for a tenant",
    include_in_schema=False,
)
def fetch_google_meetings(
    tenant_id: str,
    google_creds_repository: GoogleCredsRepository = Depends(google_creds_repository),
    include_in_schema=False,
) -> JSONResponse:
    """
    Fetches all Google Calendar meetings for a given tenant.
    """
    logger.info(f"Received Google meetings request for tenant: {tenant_id}")

    google_credentials = google_creds_repository.get_creds(tenant_id)
    if not google_credentials:
        raise HTTPException(
            status_code=404, detail="Google credentials not found for the tenant"
        )

    google_credentials = Credentials(
        token=google_credentials["access_token"],
        refresh_token=google_credentials["refresh_token"],
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        token_uri=GOOGLE_TOKEN_URI,
    )

    logger.debug(f"Google credentials before refresh: {google_credentials}")

    try:
        google_credentials.refresh(GoogleRequest())
    except Exception as e:
        logger.error(f"Error refreshing Google credentials: {e}")
        raise HTTPException(
            status_code=401, detail="Error refreshing Google credentials"
        )

    logger.debug(f"Google credentials after refresh: {google_credentials}")

    google_creds_repository.update_creds(
        {
            "tenant_id": tenant_id,
            "access_token": google_credentials.token,
            "refresh_token": google_credentials.refresh_token,
        }
    )

    access_token = google_credentials.token

    credentials = Credentials(token=access_token)
    service = build("calendar", "v3", credentials=credentials)

    now = datetime.datetime.utcnow().isoformat() + "Z"  # 'Z' indicates UTC time
    logger.info(f"Fetching meetings starting from: {now}")

    try:
        events_result = (
            service.events()
            .list(
                calendarId="primary",
                timeMin=now,
                maxResults=10,
                singleEvents=True,
                orderBy="startTime",
            )
            .execute()
        )
    except Exception as e:
        logger.error(f"Error fetching events from Google Calendar: {e}")
        raise HTTPException(
            status_code=500, detail="Error fetching events from Google Calendar"
        )

    meetings = events_result.get("items", [])
    logger.info(f"Fetched events: {meetings}")

    if not meetings:
        return JSONResponse(content={"message": "No upcoming events found."})

    for meeting in meetings:
        meeting = MeetingDTO.from_google_calendar_event(meeting, tenant_id)
        event = GenieEvent(
            topic=Topic.NEW_MEETING, data=meeting.to_json(), scope="public"
        )
        event.send()

    return JSONResponse(content={"events": meetings})


@v1_router.get("/profiles/{meeting_id}/{tenant_id}", response_model=MiniProfileResponse)
def get_all_profile_for_meeting(
    tenant_id: str,
    meeting_id: str,
    meetings_repository=Depends(meetings_repository),
    ownerships_repository=Depends(ownerships_repository),
    persons_repository=Depends(persons_repository),
    profiles_repository=Depends(profiles_repository),
    tenants_repository=Depends(tenants_repository),
) -> MiniProfileResponse:
    """
    Get all profile IDs and names for a specific meeting.

    - **tenant_id**: Tenant ID - the right one is 'abcde'
    - **meeting_id**: Meeting ID
    """
    logger.info(f"Received profiles request for meeting: {meeting_id}")
    meeting = meetings_repository.get_meeting_data(meeting_id)
    if not meeting:
        return JSONResponse(content={"error": "Meeting not found"})
    if meeting.tenant_id != tenant_id:
        return JSONResponse(content={"error": "Tenant mismatch"})
    tenant_email = tenants_repository.get_tenant_email(tenant_id)
    logger.info(f"Tenant email: {tenant_email}")
    participants_emails = meeting.participants_emails
    logger.debug(f"Participants emails: {participants_emails}")
    filtered_participants_emails = MeetingManager.filter_emails(
        host_email=tenant_email, participants_emails=participants_emails
    )
    logger.debug(f"Filtered participants emails: {filtered_participants_emails}")
    logger.info(f"Filtered participants emails: {filtered_participants_emails}")
    filtered_emails = filtered_participants_emails
    persons_uuid = []
    for email in filtered_emails:
        person = persons_repository.find_person_by_email(email)
        if person:
            persons_uuid.append(person.uuid)
    logger.info(f"Got persons_uuid for the meeting: {persons_uuid}")
    profiles = []
    for uuid in persons_uuid:
        profile = profiles_repository.get_profile_data(uuid)
        logger.info(f"Got profile: {profile}")
        if profile:
            profiles.append({"uuid": profile.uuid, "name": profile.name})
    logger.info(f"Sending profiles: {profiles}")
    return JSONResponse(content=profiles)


@v1_router.get(
    "/profiles/{tenant_id}/{uuid}/attendee-info", response_model=AttendeeInfo
)
def get_profile_attendee_info(
    uuid: str,
    tenant_id: str,
    ownerships_repository=Depends(ownerships_repository),
    profiles_repository=Depends(profiles_repository),
    personal_data_repository=Depends(personal_data_repository),
) -> AttendeeInfo:
    """
    Get the attendee-info of a profile - Mock version.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID

    returns:
    - **attendee-info**:
        - **picture**: Profile picture URL
        - **name**: Profile name
        - **company**: Profile company
        - **position**: Profile position
        - **social_media_links**: List of social media links
            - **platform**: Social media platform
            - **url**: Social media URL
    """
    logger.info(f"Received attendee-info request for profile: {uuid}")
    if not ownerships_repository.check_ownership(tenant_id, uuid):
        return JSONResponse(content={"error": "Profile not found under this tenant"})
    profile = profiles_repository.get_profile_data(uuid)
    if not profile:
        return JSONResponse(content={"error": "Could not find profile"})
    picture = profile.picture_url
    name = profile.name
    company = profile.company
    position = profile.position
    links = personal_data_repository.get_social_media_links(uuid)
    logger.info(f"Got links: {links}, type: {type(links)}")
    for link in links:
        link.pop("id")
        link.pop("username")
        link["platform"] = link.pop("network")
    profile = {
        "picture": picture,
        "name": name,
        "company": company,
        "position": position,
        "social_media_links": links,
    }
    return JSONResponse(content=profile)


@v1_router.get(
    "/profiles/{tenant_id}/{uuid}/strengths",
    response_model=StrengthsListResponse,
    summary="Fetches strengths of a profile",
)
def get_profile_strengths(
    uuid: str,
    tenant_id: str,
    ownerships_repository=Depends(ownerships_repository),
    profiles_repository=Depends(profiles_repository),
) -> StrengthsListResponse:
    """
    Get the strengths of a profile - Mock version.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID

    returns:
    - **strengths**: List of strengths:
        - **strength_name**: Strength name
        - **score**: Strength score between 1-100
        - **reason**: Reasons for choosing the strength and the score
    """
    logger.info(f"Received strengths request for profile: {uuid}")
    if not ownerships_repository.check_ownership(tenant_id, uuid):
        return JSONResponse(content={"error": "Profile not found under this tenant"})
    profile = profiles_repository.get_profile_data(uuid)
    if profile:
        return JSONResponse(content=profile.strengths)
    return JSONResponse(content={"error": "Could not find profile"})


@v1_router.get(
    "/profiles/{tenant_id}/{uuid}/get-to-know",
    response_model=GetToKnowResponse,
    summary="Fetches 'get-to-know' information of a profile",
)
def get_profile_get_to_know(
    uuid: str,
    tenant_id: str,
    ownerships_repository=Depends(ownerships_repository),
    profiles_repository=Depends(profiles_repository),
) -> GetToKnowResponse:
    """
    Get the 'get-to-know' information of a profile - Mock version.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got get-to-know request for profile: {uuid}")

    if not ownerships_repository.check_ownership(tenant_id, uuid):
        return JSONResponse(content={"error": "Profile not found under this tenant"})
    profile = profiles_repository.get_profile_data(uuid)
    logger.info(f"Got profile: {profile}")
    if profile:
        return JSONResponse(content=profile.get_to_know)
    return JSONResponse(content={"error": "Could not find profile"})


#
# @v1_router.get("/profiles/{tenant_id}/{uuid}/connections", response_class=JSONResponse)
# def get_profile_connections(
#     uuid: str,
#     tenant_id: str,
#     profiles_repository=Depends(profiles_repository),
#     ownerships_repository=Depends(ownerships_repository),
#     persons_repository=Depends(persons_repository),
# ) -> JSONResponse:
#     """
#     Get the connections of a profile - Mock version.
#
#     - **tenant_id**: Tenant ID
#     - **uuid**: Profile UUID
#     """
#     logger.info(f"Got connections request for profile: {uuid}")
#     if not ownerships_repository.check_ownership(tenant_id, uuid):
#         return JSONResponse(content={"error": "Profile not found under this tenant"})
#     profile = profiles_repository.get_profile_data(uuid)
#     if profile:
#         connections_uuid = profile.connections
#         logger.info(f"Got connections: {connections_uuid}")
#         connections = [
#             persons_repository.get_person(connection_uuid)
#             for connection_uuid in connections_uuid
#         ]
#         connections = [connection.to_dict() for connection in connections]
#         for i in range(len(connections)):
#             connections[i]["picture_url"] = profiles_repository.get_profile_picture(
#                 connections[i]["uuid"]
#             )
#             connections[i].pop("company")
#             connections[i].pop("position")
#             connections[i].pop("email")
#             connections[i].pop("timezone")
#         return JSONResponse(content=connections)
#     return JSONResponse(content={"error": "Could not find profile"})
#
#
# @v1_router.get("/profiles/{tenant_id}/{uuid}/hobbies", response_class=JSONResponse)
# def get_profile_hobbies(
#     uuid: str,
#     tenant_id: str,
#     profiles_repository=Depends(profiles_repository),
#     hobbies_repository=Depends(hobbies_repository),
#     ownerships_repository=Depends(ownerships_repository),
# ) -> JSONResponse:
#     """
#     Get the hobbies of a profile - Mock version.
#
#     - **tenant_id**: Tenant ID
#     - **uuid**: Profile UUID
#     """
#     logger.info(f"Received hobbies request for profile: {uuid}")
#     if not ownerships_repository.check_ownership(tenant_id, uuid):
#         return JSONResponse(content={"error": "Profile not found under this tenant"})
#     profile = profiles_repository.get_profile_data(uuid)
#     if profile:
#         hobbies_uuid = profile.hobbies
#         logger.info(f"Got hobbies: {hobbies_uuid}")
#         hobbies = [
#             hobbies_repository.get_hobby(hobbie_uuid) for hobbie_uuid in hobbies_uuid
#         ]
#         logger.info(f"Got hobbies: {hobbies}")
#
#         return JSONResponse(content=hobbies)
#     return JSONResponse(content={"error": "Could not find profile"})
#
#
# @v1_router.get("/profiles/{tenant_id}/{uuid}/news", response_class=JSONResponse)
# def get_profile_news(
#     uuid: str,
#     tenant_id: str,
#     profiles_repository=Depends(profiles_repository),
#     ownerships_repository=Depends(ownerships_repository),
# ) -> JSONResponse:
#     """
#     Get the news of a profile - Mock version.
#
#     - **tenant_id**: Tenant ID
#     - **uuid**: Profile UUID
#     """
#     logger.info(f"Received news request for profile: {uuid}")
#     if not ownerships_repository.check_ownership(tenant_id, uuid):
#         return JSONResponse(content={"error": "Profile not found under this tenant"})
#     profile = profiles_repository.get_profile_data(uuid)
#     if profile:
#         return JSONResponse(content=profile.news)
#     return JSONResponse(content={"error": "Could not find profile"})


@v1_router.get(
    "/profiles/{tenant_id}/{uuid}/good-to-know", response_model=GoodToKnowResponse
)
def get_profile_good_to_know(
    uuid: str,
    tenant_id: str,
    profiles_repository=Depends(profiles_repository),
    ownerships_repository=Depends(ownerships_repository),
    persons_repository=Depends(persons_repository),
    hobbies_repository=Depends(hobbies_repository),
) -> GoodToKnowResponse:
    """
    Get the 'good-to-know' information of a profile - Mock version.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got good-to-know request for profile: {uuid}")
    if not ownerships_repository.check_ownership(tenant_id, uuid):
        return JSONResponse(content={"error": "Profile not found under this tenant"})
    profile = profiles_repository.get_profile_data(uuid)
    if profile:
        news = profile.news

        hobbies_uuid = profile.hobbies
        logger.info(f"Got hobbies: {hobbies_uuid}")
        hobbies = [
            hobbies_repository.get_hobby(hobbie_uuid) for hobbie_uuid in hobbies_uuid
        ]
        logger.info(f"Got hobbies: {hobbies}")

        connections_uuid = profile.connections
        logger.info(f"Got connections: {connections_uuid}")
        connections = [
            persons_repository.get_person(connection_uuid)
            for connection_uuid in connections_uuid
        ]
        connections = [connection.to_dict() for connection in connections]
        for i in range(len(connections)):
            connections[i]["picture_url"] = profiles_repository.get_profile_picture(
                connections[i]["uuid"]
            )
        good_to_know = {
            "news": news,
            "hobbies": hobbies,
            "connections": connections,
        }
        return JSONResponse(content=good_to_know)
    return JSONResponse(content={"error": "Could not find profile"})


@v1_router.get(
    "/profiles/{tenant_id}/{uuid}/work-experience",
    response_model=WorkExperienceResponse,
)
def get_profile_work_experience(
    uuid: str,
    tenant_id: str,
    personal_data_repository=Depends(personal_data_repository),
) -> WorkExperienceResponse:
    """
    Get the work experience of a profile - *Mock version*.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got work experience request for profile: {uuid}")

    personal_data = personal_data_repository.get_personal_data(uuid)
    logger.debug(f"Personal data: {personal_data}")

    if personal_data:
        return JSONResponse(content=personal_data["experience"])
    return JSONResponse(content={"error": "Could not find profile"})


# @v1_router.get("/meetings-mock", response_class=JSONResponse)
# def get_all_meetings() -> JSONResponse:
#     """
#     Get all meetings for a specific tenant - Mock version.
#     """
#     return JSONResponse(content=meetings)
