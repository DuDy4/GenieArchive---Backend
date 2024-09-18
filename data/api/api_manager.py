import json
import traceback
import requests
import uuid

from fastapi import Depends, Request, Query
from fastapi.routing import APIRouter

from common.utils import env_utils, email_utils

from starlette.responses import JSONResponse
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from fastapi import HTTPException, Depends

from data.api.base_models import *
import datetime

from data.api.api_services_classes.meetings_api_services import MeetingsApiService
from data.api.api_services_classes.tenants_api_services import TenantsApiService
from data.api.api_services_classes.profiles_api_services import ProfilesApiService
from data.api.api_services_classes.admin_api_services import AdminApiService


from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.repositories.tenants_repository import TenantsRepository
from data.data_common.repositories.profiles_repository import ProfilesRepository
from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.repositories.ownerships_repository import OwnershipsRepository
from data.data_common.repositories.google_creds_repository import GoogleCredsRepository
from data.data_common.repositories.companies_repository import CompaniesRepository

from data.data_common.dependencies.dependencies import (
    profiles_repository,
    tenants_repository,
    meetings_repository,
    google_creds_repository,
    ownerships_repository,
    persons_repository,
    personal_data_repository,
    hobbies_repository,
    companies_repository,
)

from data.data_common.events.topics import Topic
from data.data_common.events.genie_event import GenieEvent
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.data_transfer_objects.tenant_dto import TenantDTO
from data.data_common.utils.str_utils import get_uuid4

from data.api_services.meeting_manager import (
    process_agenda_to_all_meetings,
    process_classification_to_all_meetings,
)

logger = GenieLogger()
SELF_URL = env_utils.get("PERSON_URL", "https://localhost:8000")
logger.info(f"Self url: {SELF_URL}")

GOOGLE_CLIENT_ID = env_utils.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = env_utils.get("GOOGLE_CLIENT_SECRET")
REDIRECT_URI = f"{SELF_URL}/v1/google-callback"
GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"
DEFAULT_INTERNAL_API_KEY = "g3n13admin"
ZENDESK_URL = env_utils.get("ZENDESK_URL")
ZENDESK_USERNAME = env_utils.get("ZENDESK_USERNAME")
ZENDESK_API_TOKEN = env_utils.get("ZENDESK_API_TOKEN")

v1_router = APIRouter(prefix="/v1")

meetings_api_service = MeetingsApiService()
tenants_api_service = TenantsApiService()
profiles_api_service = ProfilesApiService()
admin_api_service = AdminApiService()


@v1_router.post("/successful-login")
async def post_successful_login(
    request: Request,
):
    """
    Returns a tenant ID.
    """
    logger.info("Received JWT data")
    auth_data = await request.json()
    logger.info(f"Received auth data: {auth_data}")
    response = tenants_api_service.post_successful_login(auth_data)
    return {"verdict": "allow", "response": response}


@v1_router.post("/social-auth-data", response_model=UserResponse)
async def post_social_auth_data(
    request: Request,
    google_creds_repository: GoogleCredsRepository = Depends(google_creds_repository),
    tenants_repository: TenantsRepository = Depends(tenants_repository),
):
    """
    Returns a tetant ID - MOCK.
    """
    logger.info("Received social auth data")
    user_auth_data = await request.json()
    # time.sleep(15)
    logger.info(f"Received social auth data: {user_auth_data}")
    prehook_data = user_auth_data["prehookContext"]
    logger.info(f"Prehook data: {prehook_data}")
    auth_data = user_auth_data["data"]["authData"]
    user_email = auth_data["user"]["email"]
    user_access_token = auth_data["tokens"]["accessToken"]
    user_id_token = auth_data["tokens"]["idToken"]

    # if "tenantId" in prehook_data:
    #     tenant_id = prehook_data["tenantId"]
    # else:
    #     logger.info("Tenant ID not found in prehook data, fetching by email")
    #     tenant_id = tenants_repository.get_tenant_id_by_email(user_email)

    if user_email:
        google_creds_repository.insert(
            {
                "email": user_email,
                "accessToken": user_access_token,
                "refreshToken": user_id_token,
            }
        )
    else:
        logger.error("Tenant ID not found. Skipping credentials insertion")
    return JSONResponse(content={"verdict": "allow"})


@v1_router.post("/users/login-event")
async def login_event(
    request: Request,
    tenants_repository: TenantsRepository = Depends(tenants_repository),
    google_creds_repository: GoogleCredsRepository = Depends(google_creds_repository),
):
    """
    Handle user signup process.
    """
    try:
        logger.info("Fetching user info")
        user_info = await request.json()
        logger.info(f"Received user info: {user_info}")

        # Call the service method
        response = tenants_api_service.login_event(user_info)
        return JSONResponse(content=response, status_code=200)

    except HTTPException as e:
        # FastAPI will handle the response based on the exception
        raise e

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


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
    containing tenantId and name. If the user already exists, it returns the existing
    uuid.

    - **request**: The request object.
    - **tenants_repository**: Dependency that provides access to the tenants repository.

    Returns:
        - **message**: Success or error message.
    """
    try:
        logger.debug(f"Received signup request: {request}")
        data = await request.json()
        logger.debug(f"Received signup data: {data}")

        uuid = tenants_repository.exists(data.get("tenantId"), data.get("name"))

        if uuid:
            logger.info(f"User already exists in database")
            return {"message": "User already exists in database"}

        tenant_email = data.get("email")

        old_tenant_id = tenants_repository.get_tenant_id_by_email(data.get("email"))
        uuid = tenants_repository.insert(data)
        logger.debug(f"User account created successfully with uuid: {uuid}")

        return {
            "message": f"User account created successfully with uuid: {uuid}",
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@v1_router.post("/create-ticket")
async def create_ticket(ticket_data: TicketData):
    auth = (f"{ZENDESK_USERNAME}/token", ZENDESK_API_TOKEN)
    zendesk_api_url = f"{ZENDESK_URL}/api/v2/tickets.json"
    logger.info(f"Creating ticket with data: {ticket_data}")

    ticket_payload = {
        "ticket": {
            "subject": ticket_data.subject,
            "description": ticket_data.description,
            "requester": {
                "name": ticket_data.name,
                "email": ticket_data.email,
            },
            "priority": ticket_data.priority,
            "custom_fields": [
                {"id": 21371588554386, "value": ticket_data.name},
                {"id": 21371622513810, "value": ticket_data.email},
            ],
        }
    }

    try:
        # Make the request to the Zendesk API
        response = requests.post(zendesk_api_url, json=ticket_payload, auth=auth)

        # Check if the request was successful
        if response.status_code == 201:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)

    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error creating Zendesk ticket: {str(e)}")


# @v1_router.get(
#     "/{tenant_id}/profiles",
#     response_model=List[ProfileDTO],
#     include_in_schema=False,
#     summary="Gets all profiles for a given tenant",
# )
# async def get_all_profiles(
#     request: Request,
#     tenant_id: str,
#     search: str = Query(None, description="Partial text to search profile names"),
#     ownerships_repository: OwnershipsRepository = Depends(ownerships_repository),
#     profiles_repository: ProfilesRepository = Depends(profiles_repository),
# ) -> List[ProfileDTO]:
#     """
#     Gets all profiles for a given tenant.
#     """
#     logger.info(f"Received get profiles request, with search: '{search}'")
#
#     profiles_uuid = ownerships_repository.get_all_persons_for_tenant(tenant_id)
#     logger.info(f"Got profiles_uuid: {profiles_uuid}")
#
#     profiles_list = profiles_repository.get_profiles_from_list(profiles_uuid, search)
#     logger.info(f"Got profiles: {len(profiles_list)}")
#     profiles_response_list = [ProfileResponse(profile=profile) for profile in profiles_list]
#
#     logger.debug(f"Profiles: {[profile.profile.name for profile in profiles_response_list]}")
#     return profiles_list
#


@v1_router.get(
    "/{tenant_id}/meetings",
    response_model=MeetingsListResponse,
    summary="Gets all *meeting* that the tenant has profiles participants in",
)
async def get_all_meetings(
    request: Request,
    tenant_id: str,
    impersonate_tenant_id: Optional[str] = Query(None),
    # name: str = Query(None, description="Partial text to search profile names"),
) -> MeetingsListResponse | JSONResponse:
    """
    Gets all *meeting* that the tenant has profiles participants in.

    Steps:
    1. Get all persons for the tenant.
    2. Get all emails for the persons with name that includes the search text.
    3. Get all meetings with participants that have the emails.

    """
    logger.info(f"Received get profiles request, with tenant: {tenant_id}")
    allowed_impersonate_tenant_id = get_tenant_id_to_impersonate(impersonate_tenant_id, request)
    tenant_id = allowed_impersonate_tenant_id if allowed_impersonate_tenant_id else tenant_id
    logger.info(f"Getting profile for tenant ID: {tenant_id}")
    response = meetings_api_service.get_all_meetings(tenant_id)
    return JSONResponse(content=response)


@v1_router.get("/{tenant_id}/{meeting_id}/profiles", response_model=List[MiniProfileResponse])
def get_all_profiles_for_meeting(
    request: Request, tenant_id: str, meeting_id: str, impersonate_tenant_id: Optional[str] = Query(None)
) -> Union[List[MiniProfileResponse], JSONResponse]:
    """
    Get all profile IDs and names for a specific meeting.

    - **tenant_id**: Tenant ID - the right one is 'abcde'
    - **meeting_id**: Meeting ID
    """
    logger.info(f"Received profiles request for meeting: {meeting_id}")
    allowed_impersonate_tenant_id = get_tenant_id_to_impersonate(impersonate_tenant_id, request)
    tenant_id = allowed_impersonate_tenant_id if allowed_impersonate_tenant_id else tenant_id
    logger.info(f"Getting profile for tenant ID: {tenant_id}")
    response = profiles_api_service.get_profiles_for_meeting(tenant_id, meeting_id)
    logger.info(f"About to send response: {response}")
    return response


@v1_router.get("/{tenant_id}/profiles/{uuid}/attendee-info", response_model=AttendeeInfo)
def get_profile_attendee_info(
    request: Request,
    uuid: str,
    tenant_id: str,
    impersonate_tenant_id: Optional[str] = Query(None),
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
    allowed_impersonate_tenant_id = get_tenant_id_to_impersonate(impersonate_tenant_id, request)
    tenant_id = allowed_impersonate_tenant_id if allowed_impersonate_tenant_id else tenant_id
    response = profiles_api_service.get_profile_attendee_info(tenant_id, uuid)
    logger.info(f"About to send response: {response}")
    return response


@v1_router.get(
    "/{tenant_id}/profiles/{uuid}/strengths",
    response_model=StrengthsListResponse,
    summary="Fetches strengths of a profile",
)
def get_profile_strengths(
    request: Request,
    uuid: str,
    tenant_id: str,
    impersonate_tenant_id: Optional[str] = Query(None),
) -> StrengthsListResponse | JSONResponse:
    """
    Get the strengths of a profile - Mock version.

    - **tenant_id**: Tenant ID
    - **uuid**: The uuid of the requested profile

    returns:
    - **strengths**: List of strengths:
        - **strength_name**: Strength name
        - **score**: Strength score between 1-100
        - **reason**: Reasons for choosing the strength and the score
    """
    logger.info(f"Received strengths request for profile: {uuid}")
    allowed_impersonate_tenant_id = get_tenant_id_to_impersonate(impersonate_tenant_id, request)
    tenant_id = allowed_impersonate_tenant_id if allowed_impersonate_tenant_id else tenant_id
    response = profiles_api_service.get_profile_strengths(tenant_id, uuid)
    logger.info(f"About to send response: {response}")
    return response


@v1_router.get(
    "/{tenant_id}/profiles/{uuid}/get-to-know",
    response_model=GetToKnowResponse,
    summary="Fetches 'get-to-know' information of a profile",
)
def get_profile_get_to_know(
    request: Request,
    uuid: str,
    tenant_id: str,
    impersonate_tenant_id: Optional[str] = Query(None),
) -> GetToKnowResponse:
    """
    Get the 'get-to-know' information of a profile - Mock version.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got get-to-know request for profile: {uuid}")

    allowed_impersonate_tenant_id = get_tenant_id_to_impersonate(impersonate_tenant_id, request)
    tenant_id = allowed_impersonate_tenant_id if allowed_impersonate_tenant_id else tenant_id
    response = profiles_api_service.get_profile_get_to_know(tenant_id, uuid)
    logger.info(f"About to send response: {response}")
    return response


@v1_router.get("/{tenant_id}/profiles/{uuid}/good-to-know", response_model=GoodToKnowResponse)
def get_profile_good_to_know(
    request: Request,
    uuid: str,
    tenant_id: str,
    impersonate_tenant_id: Optional[str] = Query(None),
) -> GoodToKnowResponse:
    """
    Get the 'good-to-know' information of a profile - Mock version.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got good-to-know request for profile: {uuid}")

    allowed_impersonate_tenant_id = get_tenant_id_to_impersonate(impersonate_tenant_id, request)
    tenant_id = allowed_impersonate_tenant_id if allowed_impersonate_tenant_id else tenant_id
    response = profiles_api_service.get_profile_good_to_know(tenant_id, uuid)
    logger.info(f"About to send response: {response}")
    return response


@v1_router.get(
    "/{tenant_id}/profiles/{uuid}/work-experience",
    response_model=WorkExperienceResponse,
)
def get_work_experience(
    request: Request,
    uuid: str,
    tenant_id: str,
    impersonate_tenant_id: Optional[str] = Query(None),
) -> WorkExperienceResponse:
    """
    Get the work experience of a profile - *Mock version*.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got work experience request for profile: {uuid}")

    allowed_impersonate_tenant_id = get_tenant_id_to_impersonate(impersonate_tenant_id, request)
    tenant_id = allowed_impersonate_tenant_id if allowed_impersonate_tenant_id else tenant_id
    response = profiles_api_service.get_work_experience(tenant_id, uuid)
    logger.info(f"About to send response: {response}")
    return JSONResponse(content=response)


@v1_router.get(
    "/{tenant_id}/{meeting_uuid}/meeting-overview",
    response_model=Union[
        MiniMeetingOverviewResponse, InternalMeetingOverviewResponse, PrivateMeetingOverviewResponse
    ],
)
def get_meeting_overview(
    request: Request,
    tenant_id: str,
    meeting_uuid: str,
    impersonate_tenant_id: Optional[str] = Query(None),
) -> Union[
    MiniMeetingOverviewResponse, InternalMeetingOverviewResponse, PrivateMeetingOverviewResponse, JSONResponse
]:
    """
    Get the meeting information.

    - **tenant_id**: Tenant ID
    - **meeting_id**: Meeting ID
    """
    logger.info(f"Got meeting info request for meeting: {meeting_uuid}")

    allowed_impersonate_tenant_id = get_tenant_id_to_impersonate(impersonate_tenant_id, request)
    tenant_id = allowed_impersonate_tenant_id if allowed_impersonate_tenant_id else tenant_id
    response = meetings_api_service.get_meeting_overview(tenant_id, meeting_uuid)
    logger.info(f"About to send response: {response}")
    return response


@v1_router.get("/internal/sync-profile/{person_uuid}")
def sync_profile(
    person_uuid: str, api_key: str, persons_repository: PersonsRepository = Depends(persons_repository)
) -> JSONResponse:
    """
    Sync a profile with the PDL API.

    - **person_uuid**: The UUID of the person to sync.
    - **api_key**: The internal API key
    """
    internal_api_key = env_utils.get("INTERNAL_API_KEY", DEFAULT_INTERNAL_API_KEY)
    if api_key != internal_api_key:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    validate_uuid(person_uuid)
    person = persons_repository.get_person(person_uuid)
    if not person:
        logger.error(f"Person not found: {person_uuid}")
        return JSONResponse(content={"error": "Person not found"})
    logger.info(f"Got person: {person}")
    if person.linkedin:
        event = GenieEvent(Topic.PDL_NEW_PERSON_TO_ENRICH, person.to_json(), "public")
        event.send()
    else:
        logger.error(f"Person does not have a LinkedIn URL")
        return JSONResponse(content={"error": "Person does not have a LinkedIn URL"})
    return JSONResponse(content={"message": "Profile sync initiated for " + person.email})


@v1_router.get("/internal/sync-email/{person_uuid}")
def sync_email(
    person_uuid: str,
    api_key: str,
    persons_repository: PersonsRepository = Depends(persons_repository),
    ownerships_repository: OwnershipsRepository = Depends(ownerships_repository),
) -> JSONResponse:
    """
    Sync an email from the beginning

    - **person_uuid**: The UUID of the person to sync.
    - **api_key**: The internal API key
    """
    internal_api_key = env_utils.get("INTERNAL_API_KEY", DEFAULT_INTERNAL_API_KEY)
    if api_key != internal_api_key:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    validate_uuid(person_uuid)
    person = persons_repository.get_person(person_uuid)
    if not person:
        logger.error(f"Person not found: {person_uuid}")
        return JSONResponse(content={"error": "Person not found"})
    logger.info(f"Got person: {person}")
    tenants = ownerships_repository.get_tenants_for_person(person_uuid)
    if not tenants or len(tenants) == 0:
        logger.error(f"Person does not have any tenants: {person_uuid}")
        return JSONResponse(content={"error": "Person does not have any tenants"})
    event = GenieEvent(
        topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
        data=json.dumps(
            {
                "tenant_id": tenants[0],
                "email": person.email,
            }
        ),
        scope="public",
    )
    event.send()
    return JSONResponse(content={"message": "Profile email sync initiated for " + person.email})


@v1_router.get("/internal/sync-meetings-agenda")
def process_meetings_agendas(api_key: str, meetings_number: int = 10) -> JSONResponse:
    """
    Sync an email from the beginning

    - **person_uuid**: The UUID of the person to sync.
    - **api_key**: The internal API key
    """
    internal_api_key = env_utils.get("INTERNAL_API_KEY", DEFAULT_INTERNAL_API_KEY)
    if api_key != internal_api_key:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    logger.info(f"Processing all meetings agendas")
    process_agenda_to_all_meetings(meetings_number)
    return JSONResponse(content={"message": "Meetings agenda processing initiated"})


@v1_router.get("/internal/sync-meeting-classification")
def process_meetings_classification(api_key: str) -> JSONResponse:
    """
    Sync an email from the beginning

    - **person_uuid**: The UUID of the person to sync.
    - **api_key**: The internal API key
    """
    internal_api_key = env_utils.get("INTERNAL_API_KEY", DEFAULT_INTERNAL_API_KEY)
    if api_key != internal_api_key:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    logger.info(f"Processing all meetings classification")
    process_classification_to_all_meetings()
    return JSONResponse(content={"message": "Meetings classification processing initiated"})


@v1_router.get(
    "/google/meetings/{user_email}",
    response_class=JSONResponse,
    summary="Fetches all Google Calendar meetings for a tenant",
    include_in_schema=False,
)
def fetch_google_meetings(
    user_email: str,
    google_creds_repository: GoogleCredsRepository = Depends(google_creds_repository),
    tenants_repository: TenantsRepository = Depends(tenants_repository),
) -> JSONResponse:
    """
    Fetches all Google Calendar meetings for a given tenant.
    """
    logger.info(f"Received Google meetings request for tenant: {user_email}")

    google_credentials = google_creds_repository.get_creds(user_email)
    if not google_credentials:
        logger.error("Google credentials not found for the tenant")
        return JSONResponse(content={"error": "Google credentials not found"})

    last_fetch_meetings = google_credentials.get("last_fetch_meetings")
    if last_fetch_meetings:
        # If last_fetch_meetings is already a datetime object, no need to parse it
        if isinstance(last_fetch_meetings, datetime.datetime):
            time_diff = datetime.datetime.now() - last_fetch_meetings
        else:
            # Otherwise, convert it from a string (if needed)
            last_fetch_meetings = datetime.datetime.strptime(last_fetch_meetings, "%Y-%m-%d %H:%M:%S.%f")
            time_diff = datetime.datetime.now() - last_fetch_meetings

        if time_diff.total_seconds() < 30:
            logger.info("Meetings already fetched in the last minute. Skipping.")
            return JSONResponse(content={"message": "Meetings already fetched in the last hour. Skipping..."})
    else:
        logger.warning("Missing last_fetch_meetings. Skipping check.")

    # Ensure all necessary fields are present in the dictionary
    google_credentials["token_uri"] = GOOGLE_TOKEN_URI
    google_credentials["client_id"] = GOOGLE_CLIENT_ID
    google_credentials["client_secret"] = GOOGLE_CLIENT_SECRET

    missing_fields = []
    required_fields = ["access_token", "refresh_token", "client_id", "client_secret", "token_uri"]
    for field in required_fields:
        if field not in google_credentials or not google_credentials[field]:
            missing_fields.append(field)

    if missing_fields:
        logger.error(f"Google credentials do not contain all necessary fields: {missing_fields}")
        return JSONResponse(
            content={"error": f"Incomplete Google credentials: missing {', '.join(missing_fields)}"}
        )
    else:
        logger.debug(f"All required fields found in Google credentials")

    # Construct the Credentials object
    credentials = Credentials(
        token=google_credentials["access_token"],
        refresh_token=google_credentials["refresh_token"],
        client_id=google_credentials["client_id"],
        client_secret=google_credentials["client_secret"],
        token_uri=google_credentials["token_uri"],
    )

    # Log credentials before using them
    logger.debug(f"Google credentials before refresh: {credentials}")

    # Build the service using the credentials
    service = build("calendar", "v3", credentials=credentials)

    now = datetime.datetime.utcnow().isoformat() + "Z"
    now_minus_10_hours = (datetime.datetime.utcnow() - datetime.timedelta(hours=10)).isoformat() + "Z"
    logger.info(f"[EMAIL={user_email}] Fetching meetings starting from: {now_minus_10_hours}")

    try:
        events_result = (
            service.events()
            .list(
                calendarId="primary",
                timeMin=now_minus_10_hours,
                maxResults=30,
                singleEvents=True,
                orderBy="startTime",
            )
            .execute()
        )
    except Exception as e:
        error_message = str(e)
        if "invalid_grant" in error_message:
            logger.error(f"Invalid grant error: {e}. The user may need to re-authenticate.")
            raise HTTPException(
                status_code=401,
                detail="Invalid grant: Refresh token is no longer valid. Please re-authenticate.",
            )
        else:
            logger.error(f"Error fetching events from Google Calendar: {e}")
            raise HTTPException(status_code=500, detail=f"Error fetching events from Google Calendar: {e}")

    meetings = events_result.get("items", [])
    logger.info(f"Fetched events: {meetings}")

    if not meetings:
        google_creds_repository.update_last_fetch_meetings(user_email)
        return JSONResponse(content={"message": "No upcoming events found."})
    tenant_id = tenants_repository.get_tenant_id_by_email(user_email)
    data_to_send = {"tenant_id": tenant_id, "meetings": meetings}
    event = GenieEvent(
        topic=Topic.NEW_MEETINGS_TO_PROCESS,
        data=data_to_send,
        scope="public",
    )
    event.send()
    google_creds_repository.update_last_fetch_meetings(user_email)
    logger.info(f"Sent {len(meetings)} meetings to the processing queue")

    return JSONResponse(
        {"status": "success", "message": f"Sent {len(meetings)} meetings to the processing queue"}
    )


@v1_router.get(
    "/profile_pictures",
    response_class=JSONResponse,
    summary="send a list of all profile pictures",
    include_in_schema=False,
)
def get_profile_pictures(
    profiles_repository: ProfilesRepository = Depends(profiles_repository),
    persons_repository: PersonsRepository = Depends(persons_repository),
) -> JSONResponse:
    """
    Get all profile pictures.
    """
    logger.info(f"Received get profile pictures request")
    names_and_pictures = profiles_repository.get_all_profiles_pictures()
    return JSONResponse(content=names_and_pictures)


@v1_router.get(
    "/google/import-meetings/{tenant_id}",
    response_class=JSONResponse,
    summary="Fetches all Google Calendar meetings for a tenant",
    include_in_schema=False,
)
def import_google_meetings(
    tenant_id: str,
    google_creds_repository: GoogleCredsRepository = Depends(google_creds_repository),
    tenants_repository: TenantsRepository = Depends(tenants_repository),
) -> JSONResponse:
    """
    Fetches all Google Calendar meetings for a given tenant.
    """
    email_address = tenants_repository.get_tenant_email(tenant_id)
    logger.info(f"Received Google meetings request for tenant: {email_address}")
    return fetch_google_meetings(email_address, google_creds_repository, tenants_repository)


@v1_router.get(
    "/admin/tenants",
    response_class=JSONResponse,
    summary="Fetches all tenants for an admin",
    include_in_schema=False,
)
def fetch_all_tenants(
    request: Request,
    tenants_repository: TenantsRepository = Depends(tenants_repository),
) -> JSONResponse:
    """
    Fetches all tenants for an admin
    """
    if (
        request.state
        and hasattr(request.state, "user_email")
        and email_utils.is_genie_admin(request.state.user_email)
    ):
        all_tenants = tenants_repository.get_all_tenants()
        response = {"admin": True, "tenants": [tenant.to_dict() for tenant in all_tenants]}
        return JSONResponse(content=response)
    else:
        raise HTTPException(status_code=403, detail="Forbidden endpoint")


def validate_uuid(uuid_string: str):
    try:
        val = uuid.UUID(uuid_string, version=4)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")
    return str(val)


def get_tenant_id_to_impersonate(
    impersonate_tenant_id: str,
    request: Request,
    tenants_repository: TenantsRepository = Depends(tenants_repository),
):
    logger.info(f"Checking if user is impersonating tenant")
    logger.info(f"Request state: {request.state}")
    if (
        impersonate_tenant_id
        and request.state
        and hasattr(request.state, "user_email")
        and email_utils.is_genie_admin(request.state.user_email)
    ):
        logger.info(f"User is impersonating tenant")
        impersonated_email = tenants_repository.get_tenant_email(impersonate_tenant_id)
        if impersonated_email:
            logger.warning(f"User {request.state.user_email} is IMPERONSATING {impersonated_email}")
            return impersonate_tenant_id
        else:
            logger.info(f"Could not find tenant to impersonate. Continue with original tenant id")
    logger.info(f"User is not impersonating tenant")
    return None
