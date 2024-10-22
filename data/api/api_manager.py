from typing import Union

import requests

from fastapi import Request, Query, BackgroundTasks, Depends
from fastapi.routing import APIRouter
from deep_translator import GoogleTranslator

from common.genie_logger import tenant_id
from common.utils import env_utils, email_utils
from starlette.responses import JSONResponse
from fastapi import HTTPException

from data.api.base_models import *

from data.api.api_services_classes.meetings_api_services import MeetingsApiService
from data.api.api_services_classes.tenants_api_services import TenantsApiService
from data.api.api_services_classes.profiles_api_services import ProfilesApiService
from data.api.api_services_classes.admin_api_services import AdminApiService
from data.api.api_services_classes.user_materials_services import UserMaterialServices
from data.api.api_services_classes.stats_api_services import StatsApiService
from data.api.api_services_classes.badges_api_services import BadgesApiService


logger = GenieLogger()
SELF_URL = env_utils.get("PERSON_URL", "https://localhost:8000")
logger.info(f"Self url: {SELF_URL}")

ZENDESK_URL = env_utils.get("ZENDESK_URL")
ZENDESK_USERNAME = env_utils.get("ZENDESK_USERNAME")
ZENDESK_API_TOKEN = env_utils.get("ZENDESK_API_TOKEN")
INTERNAL_API_KEY = env_utils.get("INTERNAL_API_KEY")

v1_router = APIRouter(prefix="/v1")

meetings_api_service = MeetingsApiService()
tenants_api_service = TenantsApiService()
profiles_api_service = ProfilesApiService()
admin_api_service = AdminApiService()
user_materials_service = UserMaterialServices()
stats_api_service = StatsApiService()
badges_api_service = BadgesApiService()

logger.info("Imported all services")


@v1_router.post("/file-uploaded")
async def file_uploaded(request: Request):
    logger.info(f"New file uploaded")
    uploaded_files = await request.json()
    if not uploaded_files:
        logger.error(f"Body not found in azure event")
        return
    try:
        # This is used for going through azure validation and must not be used in production scenario
        if uploaded_files[0]["data"]:
            data = uploaded_files[0]["data"]
            if data["validationCode"] and data["validationUrl"]:
                logger.info(f"Azure data: {uploaded_files}")
                logger.info("Azure validation completed")
                return
    except:
        logger.info("Handling uploaded file")
    result = user_materials_service.file_uploaded(uploaded_files)
    if result:
        return JSONResponse(content={"status": "success", "message": "File uploaded"})
    else:
        return JSONResponse(content={"status": "error", "message": "Error handling file upload"})


@v1_router.post("/generate-upload-url")
async def get_file_upload_url(request: Request):
    tenant_id = get_request_state_value(request, "tenant_id")
    if not tenant_id:
        raise HTTPException(
            status_code=401, detail=f"""Unauthorized request. JWT is missing tenant id or tenant id invalid"""
        )

    body = await request.json()
    if not body or not body["file_name"]:
        raise HTTPException(status_code=401, detail=f"""Missing filename""")
    file_name = body["file_name"]
    upload_url = user_materials_service.generate_upload_url(tenant_id, file_name)
    return JSONResponse(content={"upload_url": upload_url})


@v1_router.get("/uploaded-files")
async def get_file_upload_url(request: Request):
    tenant_id = get_request_state_value(request, "tenant_id")
    if not tenant_id:
        raise HTTPException(
            status_code=401, detail=f"""Unauthorized request. JWT is missing tenant id or tenant id invalid"""
        )
    uploaded_files = user_materials_service.get_all_files(tenant_id)
    if not uploaded_files:
        return JSONResponse(content=[])
    return JSONResponse(content=uploaded_files)


@v1_router.get("/user-badges")
async def get_user_badges(request: Request, impersonate_tenant_id: Optional[str] = Query(None)):
    email = get_request_state_value(request, "user_email")
    if not email:
        raise HTTPException(
            status_code=401, detail=f"""Unauthorized request. JWT is missing user email or email invalid"""
        )
    allowed_impersonate_tenant_id = get_tenant_id_to_impersonate(impersonate_tenant_id, request)
    # if allowed_impersonate_tenant_id:
    #     email = tenants_repository.get_tenant_email(allowed_impersonate_tenant_id)
    if not allowed_impersonate_tenant_id:
        raise HTTPException(
            status_code=401, detail=f"""Unauthorized request. JWT is missing tenant id or tenant id invalid"""
        )
    badges_progress = badges_api_service.get_user_badges_status(tenant_id=allowed_impersonate_tenant_id)
    return JSONResponse(content=badges_progress)


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


@v1_router.post("/users/login-event")
async def login_event(
    background_tasks: BackgroundTasks,
    request: Request,
):
    """
    Handle user signup process.
    """
    logger.info("Fetching user info")
    user_info = await request.json()
    logger.info(f"Received user info: {user_info}")
    background_tasks.add_task(tenants_api_service.login_event, user_info)
    background_tasks.add_task(stats_api_service.login_event, tenant_id=user_info.get("tenant_id"))
    return JSONResponse(content="Login event received. Updated credentials", status_code=200)


@v1_router.post("/google-services/translate")
async def translate_text(request: TranslateRequest):

    try:
        logger.info(f"Text: {request.text}")
        result = GoogleTranslator(source="auto", target="en").translate(request.text)
        return {"translatedText": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Translation failed: {str(e)}")


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
        response = requests.post(zendesk_api_url, json=ticket_payload, auth=auth)

        if response.status_code == 201:
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code, detail=response.text)

    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error creating Zendesk ticket: {str(e)}")


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
    background_tasks: BackgroundTasks,
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
    if not allowed_impersonate_tenant_id:
        background_tasks.add_task(stats_api_service.view_profile_event, tenant_id=tenant_id, profile_id=uuid)
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
    background_tasks: BackgroundTasks,
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
    if not allowed_impersonate_tenant_id:
        background_tasks.add_task(
            stats_api_service.view_meeting_event, tenant_id=tenant_id, meeting_id=meeting_uuid
        )
    return response


@v1_router.delete("/{tenant_id}/{meeting_uuid}", response_class=JSONResponse)
def delete_meeting(
    request: Request,
    tenant_id: str,
    meeting_uuid: str,
    impersonate_tenant_id: Optional[str] = Query(None),
) -> JSONResponse:
    """
    Delete a meeting.

    - **tenant_id**: Tenant ID
    - **meeting_uuid**: Meeting UUID
    """
    logger.info(f"Got delete meeting request for meeting: {meeting_uuid}")

    allowed_impersonate_tenant_id = get_tenant_id_to_impersonate(impersonate_tenant_id, request)
    tenant_id = allowed_impersonate_tenant_id if allowed_impersonate_tenant_id else tenant_id
    response = meetings_api_service.delete_meeting(tenant_id, meeting_uuid)
    logger.info(f"About to send response: {response}")
    return JSONResponse(content=response)


@v1_router.get("/internal/sync-profile/{person_uuid}")
def sync_profile(person_uuid: str, api_key: str) -> JSONResponse:
    """
    Sync a profile with the PDL API.

    - **person_uuid**: The UUID of the person to sync.
    - **api_key**: The internal API key
    """
    if api_key != INTERNAL_API_KEY:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    response = admin_api_service.sync_profile(person_uuid)
    return JSONResponse(content=response)


@v1_router.get("/internal/sync-email/{person_uuid}")
def sync_email(
    person_uuid: str,
    api_key: str,
) -> JSONResponse:
    """
    Sync an email from the beginning

    - **person_uuid**: The UUID of the person to sync.
    - **api_key**: The internal API key
    """
    if api_key != INTERNAL_API_KEY:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    response = admin_api_service.sync_email(person_uuid)
    return JSONResponse(content=response)


@v1_router.get("/internal/sync-meeting-agenda/{meeting_uuid}")
def sync_meeting_agenda(meeting_uuid: str, api_key: str) -> JSONResponse:
    """
    Sync an email from the beginning

    - **person_uuid**: The UUID of the person to sync.
    - **api_key**: The internal API key
    """
    if api_key != INTERNAL_API_KEY:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    response = admin_api_service.process_single_meeting_agenda(meeting_uuid)
    return JSONResponse(content=response)


@v1_router.get("/internal/sync-meetings-agenda")
def process_meetings_agendas(
    background_tasks: BackgroundTasks, api_key: str, meetings_number: int = 10
) -> JSONResponse:
    """
    Sync an email from the beginning

    - **person_uuid**: The UUID of the person to sync.
    - **api_key**: The internal API key
    """
    if api_key != INTERNAL_API_KEY:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    logger.info(f"Processing {meetings_number} meetings agendas")
    background_tasks.add_task(admin_api_service.process_agenda_to_all_meetings, meetings_number)
    return JSONResponse(content={"status": "success", "message": "Processing all meetings agendas"})


@v1_router.get("/internal/sync-meeting-classification")
def process_meetings_classification(api_key: str) -> JSONResponse:
    """
    Sync an email from the beginning

    - **person_uuid**: The UUID of the person to sync.
    - **api_key**: The internal API key
    """
    if api_key != INTERNAL_API_KEY:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    logger.info(f"Processing all meetings classification")
    response = admin_api_service.process_classification_to_all_meetings()
    return JSONResponse(content=response)


@v1_router.get("/internal/sync-meeting-classification-from-scratch")
def process_meetings_classification(api_key: str) -> JSONResponse:
    """
    Sync an email from the beginning

    - **person_uuid**: The UUID of the person to sync.
    - **api_key**: The internal API key
    """
    if api_key != INTERNAL_API_KEY:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    logger.info(f"Processing all meetings classification")
    response = admin_api_service.process_new_classification_to_all_meetings()
    return JSONResponse(content=response)


@v1_router.get("/internal/sync-personal-news")
def process_personal_news(background_tasks: BackgroundTasks, api_key: str, num: str = "5") -> JSONResponse:
    """
    Sync an email from the beginning

    - **person_uuid**: The UUID of the person to sync.
    - **api_key**: The internal API key
    """
    if api_key != INTERNAL_API_KEY:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    logger.info(f"Processing {num} personal news")
    try:
        num = int(num)
    except ValueError:
        logger.error(f"Invalid number of news: {num}")
        num = 5
    background_tasks.add_task(admin_api_service.sync_personal_news, int(num))
    return JSONResponse(content={"status": "success", "message": "Processing personal news"})


@v1_router.get("/internal/sync-personal-data-apollo")
def process_personal_data_apollo(background_tasks: BackgroundTasks, api_key: str) -> JSONResponse:
    """
    Sync an email from the beginning

    - **person_uuid**: The UUID of the person to sync.
    - **api_key**: The internal API key
    """
    if api_key != INTERNAL_API_KEY:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    logger.info(f"Processing personal data from Apollo")
    background_tasks.add_task(admin_api_service.process_missing_apollo_personal_data)
    return JSONResponse(content={"status": "success", "message": "Processing personal data from Apollo"})


@v1_router.get(
    "/google/import-meetings/{tenant_id}",
    response_class=JSONResponse,
    summary="Fetches all Google Calendar meetings for a tenant",
    include_in_schema=False,
)
def import_google_meetings(
    tenant_id: str,
) -> JSONResponse:
    """
    Fetches all Google Calendar meetings for a given tenant.
    """

    logger.info(f"Received Google meetings request for tenant: {tenant_id}")
    response = tenants_api_service.import_google_meetings(tenant_id)
    logger.info(f"Finished fetching meetings")
    return JSONResponse(content=response)


@v1_router.get(
    "/admin/tenants",
    response_class=JSONResponse,
    summary="Fetches all tenants for an admin",
    include_in_schema=False,
)
def fetch_all_tenants(
    request: Request,
) -> JSONResponse:
    """
    Fetches all tenants for an admin
    """
    if (
        request.state
        and hasattr(request.state, "user_email")
        and email_utils.is_genie_admin(request.state.user_email)
    ):
        response = admin_api_service.fetch_all_tenants()
        logger.info(f"Returning tenants: {response}")
        return JSONResponse(content=response)

    else:
        raise HTTPException(status_code=403, detail="Forbidden endpoint")


def get_tenant_id_to_impersonate(
    impersonate_tenant_id: str,
    request: Request,
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
        return impersonate_tenant_id
    logger.info(f"User is not impersonating tenant")
    return None


def get_request_state_value(request: Request, key: str):
    if request and request.state and hasattr(request.state, key):
        return getattr(request.state, key)
    return None
