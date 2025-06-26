import json
from typing import Union

from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import requests
import asyncio
from fastapi import Form, Request, Query, BackgroundTasks
from fastapi.routing import APIRouter
from deep_translator import GoogleTranslator
from sse_starlette.sse import EventSourceResponse

from common.utils import env_utils, email_utils
from starlette.responses import JSONResponse, RedirectResponse
from fastapi import HTTPException

from data.api.base_models import *

from data.api.api_services_classes.meetings_api_services import MeetingsApiService
from data.api.api_services_classes.profiles_api_services import ProfilesApiService
from data.api.api_services_classes.admin_api_services import AdminApiService
from data.api.api_services_classes.user_materials_services import UserMaterialServices
from data.api.api_services_classes.stats_api_services import StatsApiService
from data.api.api_services_classes.badges_api_services import BadgesApiService
# from data.api.api_services_classes.params_api_services import ParamsApiService
from data.api.api_services_classes.users_api_services import UsersApiService

from common.genie_logger import GenieLogger

logger = GenieLogger()
SELF_URL = env_utils.get("PERSON_URL", "https://localhost:8000")
logger.info(f"Self url: {SELF_URL}")

ZENDESK_URL = env_utils.get("ZENDESK_URL")
ZENDESK_USERNAME = env_utils.get("ZENDESK_USERNAME")
ZENDESK_API_TOKEN = env_utils.get("ZENDESK_API_TOKEN")
INTERNAL_API_KEY = env_utils.get("INTERNAL_API_KEY")

v1_router = APIRouter(prefix="/v1")

meetings_api_service = MeetingsApiService()
profiles_api_service = ProfilesApiService()
admin_api_service = AdminApiService()
user_materials_service = UserMaterialServices()
stats_api_service = StatsApiService()
badges_api_service = BadgesApiService()
# params_api_service = ParamsApiService()
users_api_service = UsersApiService()

logger.info("Imported all services")


@v1_router.get("/google-oauth/start")
async def google_oauth_start():
    """Initiates the OAuth flow and returns an authorization URL."""
    auth_url = users_api_service.start_google_oauth()
    return RedirectResponse(auth_url)


@v1_router.get("/google-oauth/callback")
async def google_oauth_callback(request: Request, code: str = Query(...)):
    """Handles the OAuth callback and saves tokens to the database."""
    try:
        logger.info("Handling OAuth callback")
        response = users_api_service.handle_google_oauth_callback(code)
        if response:
            return JSONResponse(content={"status": "success", "message": "Tokens saved to database."})
        else:
            return JSONResponse(content={"status": "error", "message": "Error saving tokens to database."})

    except Exception as e:
        logger.error(f"Error during OAuth callback: {str(e)}")
        raise HTTPException(status_code=500, detail="Error during OAuth callback")


# @v1_router.get("/salesforce-oauth")
# async def salesforce_oauth(request: Request):
#     oauth_url = salesforce_api_service.generate_salesforce_oauth_url()
#     return RedirectResponse(oauth_url)
#
#
# @v1_router.get("/salesforce-oauth/callback")
# async def salesforce_oauth_callback(background_tasks: BackgroundTasks, request: Request, code: str = Query(...), state: str = Query(...)):
#     """Handles the OAuth callback and saves tokens to the database."""
#     try:
#         logger.info("Handling Salesforce OAuth callback")
#         response = await salesforce_api_service.handle_salesforce_oauth_callback(code, state)
#         if response:
#             logger.info(f"About to start new Salesforce auth with: {response}")
#             background_tasks.add_task(salesforce_api_service.handle_new_salesforce_auth, response)
#             return JSONResponse(content={"status": "success", "message": "Tokens saved to database."})
#         else:
#             return JSONResponse(content={"status": "error", "message": "Error saving tokens to database."})
#
#     except Exception as e:
#         logger.error(f"Error during Salesforce OAuth callback: {str(e)}")
#         raise HTTPException(status_code=500, detail="Error during Salesforce OAuth callback")


@v1_router.post("/file-uploaded")
async def file_uploaded(background_tasks: BackgroundTasks, request: Request):
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
    file_upload_dto_list = user_materials_service.file_uploaded(uploaded_files)
    logger.info(f"File upload DTOs: {file_upload_dto_list}")
    for file_upload_dto in file_upload_dto_list:
        logger.info(f"File upload DTO: {file_upload_dto}")
        background_tasks.add_task(stats_api_service.file_uploaded_event, file_upload_dto=file_upload_dto)
    if file_upload_dto_list:
        return JSONResponse(content={"status": "success", "message": "File uploaded"})
    else:
        return JSONResponse(content={"status": "error", "message": "Error handling file upload"})


@v1_router.get("/user-info/{user_id}")
async def get_user_info(user_id: str):
    """Returns the user info for a given user."""
    user_info = users_api_service.get_user_info(user_id)
    return JSONResponse(content=user_info)


@v1_router.post("/unsubscribe/{user_id}")
async def unsubscribe(user_id: str):
    """Unsubscribes the user from the service."""
    response = users_api_service.update_user_reminder_subscription(user_id, False)
    return JSONResponse(content=response)


@v1_router.post("/generate-upload-url")
async def get_file_upload_url(request: Request, impersonate_tenant_id: Optional[str] = Query(None)):
    tenant_id = get_request_state_value(request, "tenant_id")
    if not tenant_id:
        raise HTTPException(
            status_code=401, detail=f"""Unauthorized request. JWT is missing tenant id or tenant id invalid"""
        )
    allowed_impersonate_tenant_id = get_tenant_id_to_impersonate(impersonate_tenant_id, request)
    if allowed_impersonate_tenant_id:
        logger.info(f"Impersonating tenant: {allowed_impersonate_tenant_id}")
        tenant_id = allowed_impersonate_tenant_id
    logger.info(f"Generating upload URL for tenant: {tenant_id}")
    body = await request.json()
    if not body or not body["file_name"]:
        raise HTTPException(status_code=401, detail=f"""Missing filename""")
    file_name = body["file_name"]
    upload_url = user_materials_service.generate_upload_url(tenant_id, file_name)
    return JSONResponse(content={"upload_url": upload_url})


@v1_router.get("/uploaded-files")
async def get_file_upload_url(request: Request, impersonate_tenant_id: Optional[str] = Query(None)):
    tenant_id = get_request_state_value(request, "tenant_id")
    if not tenant_id:
        raise HTTPException(
            status_code=401, detail=f"""Unauthorized request. JWT is missing tenant id or tenant id invalid"""
        )
    allowed_impersonate_tenant_id = get_tenant_id_to_impersonate(impersonate_tenant_id, request)
    if allowed_impersonate_tenant_id:
        tenant_id = allowed_impersonate_tenant_id
    uploaded_files = user_materials_service.get_all_files(tenant_id)
    if not uploaded_files:
        return JSONResponse(content=[])
    return JSONResponse(content=uploaded_files)


@v1_router.get("/user-badges")
async def get_user_badges(request: Request, impersonate_user_id: Optional[str] = Query(None)):
    user_id = get_request_state_value(request, "user_id")
    if not user_id:
        raise HTTPException(
            status_code=401, detail=f"""Unauthorized request. JWT is missing user user_id or user_id invalid"""
        )
    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    if allowed_impersonate_user_id:
        user_id = allowed_impersonate_user_id
    badges_progress = badges_api_service.get_user_badges_status(user_id=user_id)
    return JSONResponse(content=badges_progress)

@v1_router.post("/badge-seen")
def mark_badge_as_seen(request: Request):
    user_id = get_request_state_value(request, "user_id")
    if not user_id:
        raise HTTPException(
            status_code=401, detail=f"""Unauthorized request. JWT is missing user user_id or user_id invalid"""
        )
    badges_api_service.mark_badges_as_seen(user_id)
    return JSONResponse(content={"status": "success", "message": "Badge marked as seen"})

@v1_router.post("/successful-login")
async def post_successful_login(
    request: Request,
):
    """
    Returns a tenant ID.
    """
    consumer_key = "3MVG9uq9ANVdsbAW4kjddHk9hFp6uB1LARAPKa4Qdmc30o1opMVaFK91jHCorAMBC.OT37Um3q4nAATDnCV0u"
    consumer_secret = "C196892FDED6DF704A5D5C356113937993D4309827530A82B17312AEE801943D"
    logger.info("Received JWT data")
    auth_data = await request.json()
    logger.info(f"Received auth data: {auth_data}")
    response = users_api_service.post_successful_login(auth_data)
    return {"verdict": "allow", "response": response}

@v1_router.get("/notifications/badges")
async def badge_notifications_stream(request: Request):
    user_id = get_request_state_value(request, "user_id")
    if not user_id:
        raise HTTPException(
            status_code=401, detail="Unauthorized request. JWT is missing user user_id or user_id invalid"
        )

    async def event_generator():
        while True:
            if await request.is_disconnected():
                break

            unseen_badge_ids = badges_api_service.get_unseen_badges(user_id)

            if unseen_badge_ids:
                yield json.dumps(unseen_badge_ids)

            await asyncio.sleep(2)

    return EventSourceResponse(event_generator())



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
    background_tasks.add_task(users_api_service.login_event, user_info)
    background_tasks.add_task(stats_api_service.login_event, user_id=user_info.get("user_id"), tenant_id=user_info.get("tenant_id"))
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
    "/{user_id}/meetings",
    response_model=MeetingsListResponse,
    summary="Gets all *meeting* that the user has profiles participants in",
)
async def get_all_meetings(
    request: Request,
    user_id: str,
    impersonate_user_id: Optional[str] = Query(None),
    name: str = Query(None, description="Partial text to search profile names"),
) -> MeetingsListResponse | JSONResponse:
    """
    Gets all *meeting* that the user has profiles participants in.

    Steps:
    1. Get all persons for the user.
    2. Get all emails for the persons with name that includes the search text.
    3. Get all meetings with participants that have the emails.

    """
    logger.info(f"Received get profiles request, with user: {user_id}")
    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    logger.info(f"Allowed impersonate user ID: {allowed_impersonate_user_id}")
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    logger.info(f"Getting meetings for user ID: {user_id}")
    response = meetings_api_service.get_all_meetings(user_id)
    return JSONResponse(content=response)


@v1_router.get(
    "/{user_id}/meetings-search",
    response_model=List[SearchMeeting],
    summary="Gets all *meeting* for the search attendees",
)
async def get_all_meetings_to_search(
        request: Request,
        user_id: str,
        impersonate_user_id: Optional[str] = Query(None),
) ->List[SearchMeeting] | JSONResponse:
    """
    Gets all *meeting* that the tenant has profiles participants in.

    Steps:
    1. Get all persons for the tenant.
    2. Get all emails for the persons with name that includes the search text.
    3. Get all meetings with participants that have the emails.

    """
    logger.info(f"Received get profiles request, with user: {user_id}")
    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    logger.info(f"Getting meetings for tenant ID: {user_id}")
    response = meetings_api_service.handle_search_meetings(user_id)
    return response


@v1_router.get(
    "/{user_id}/meetings/{selected_date}",
    response_model=MeetingsListResponse,
    summary="Gets all *meeting* that the tenant has profiles participants in",
)
async def get_all_meetings_with_selected_date(
        request: Request,
        user_id: str,
        selected_date: str,
        impersonate_user_id: Optional[str] = Query(None),
) -> MeetingsListResponse | JSONResponse:
    """
    Gets all *meeting* that the tenant has profiles participants in.

    Steps:
    1. Get all persons for the tenant.
    2. Get all emails for the persons with name that includes the search text.
    3. Get all meetings with participants that have the emails.

    """
    logger.info(f"Received get profiles request, with user: {user_id}")
    if selected_date:
        selected_datetime = datetime.fromisoformat(selected_date)
        logger.info(f"Selected Date: {selected_datetime}, type: {type(selected_datetime)}")
    else:
        selected_datetime = datetime.utcnow()  # Default to current date/time
    logger.info(f"Selected Date: {selected_datetime}")
    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    logger.info(f"Getting meetings for tenant ID: {user_id}")
    response = meetings_api_service.get_all_meetings_with_selected_date(user_id, selected_datetime)
    return JSONResponse(content=response)


@v1_router.get("/{user_id}/{meeting_id}/profiles", response_model=MiniProfilesAndPersonsListResponse)
def get_all_profiles_and_persons_for_meeting(
        request: Request, user_id: str, meeting_id: str, impersonate_user_id: Optional[str] = Query(None)
                                             ):
    """
    Get all profile IDs and persons (without profiles) for a specific meeting.

    - **user_id**: Tenant ID - the right one is 'abcde'
    - **meeting_id**: Meeting ID
    """
    logger.info(f"Received profiles request for meeting: {meeting_id}")
    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    logger.info(f"Getting profile for tenant ID: {user_id}")
    response = profiles_api_service.get_profiles_and_persons_for_meeting(user_id, meeting_id)
    logger.info(f"About to send response: {response}")
    return response



@v1_router.get("/{user_id}/profiles/{uuid}/attendee-info", response_model=AttendeeInfo)
def get_profile_attendee_info(
    request: Request,
    uuid: str,
    user_id: str,
    impersonate_user_id: Optional[str] = Query(None),
) -> AttendeeInfo:
    """
    Get the attendee-info of a profile - Mock version.

    - **user_id**: Tenant ID
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
    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    response = profiles_api_service.get_profile_attendee_info(user_id, uuid)
    logger.info(f"About to send response: {response}")
    return response


@v1_router.get(
    "/{user_id}/profiles/{uuid}/strengths",
    response_model=StrengthsListResponse,
    summary="Fetches strengths of a profile",
)
def get_profile_strengths(
    request: Request,
    uuid: str,
    user_id: str,
    impersonate_user_id: Optional[str] = Query(None),
) -> StrengthsListResponse | JSONResponse:
    """
    Get the strengths of a profile - Mock version.

    - **user_id**: Tenant ID
    - **uuid**: The uuid of the requested profile

    returns:
    - **strengths**: List of strengths:
        - **strength_name**: Strength name
        - **score**: Strength score between 1-100
        - **reason**: Reasons for choosing the strength and the score
    """
    logger.info(f"Received strengths request for profile: {uuid}")
    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    response = profiles_api_service.get_profile_strengths(user_id, uuid)
    logger.info(f"About to send response: {response}")
    return response

@v1_router.get(
    "/{user_id}/profiles/{uuid}/profile-category",
    response_model=ProfileCategory,
    summary="Fetches strengths of a profile",
)
def get_profile_category_v2(
        request: Request,
        uuid: str,
        user_id: str,
        impersonate_user_id: Optional[str] = Query(None)
) -> ProfileCategory:
    logger.info(f"Received strengths request for profile: {uuid}")
    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    response = profiles_api_service.get_profile_category_v2(user_id, uuid)
    logger.info(f"About to send response: {response}")
    if response:
        return response
    logger.error(f"Profile category not found for UUID: {uuid}")
    raise HTTPException(status_code=404, detail="Profile category not found")

@v1_router.get(
    "/{user_id}/profiles/{uuid}/get-to-know",
    response_model=GetToKnowResponse,
    summary="Fetches 'get-to-know' information of a profile",
)
def get_profile_get_to_know(
    request: Request,
    uuid: str,
    user_id: str,
    impersonate_user_id: Optional[str] = Query(None),
) -> GetToKnowResponse:
    """
    Get the 'get-to-know' information of a profile - Mock version.

    - **user_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got get-to-know request for profile: {uuid}")

    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    response = profiles_api_service.get_profile_get_to_know(user_id, uuid)
    logger.info(f"About to send response: {response}")
    return response

#
# @v1_router.get(
#     "/{user_id}/profiles/{uuid}/action-items",
#     response_model=ActionItemsResponse,
#     summary="Fetches action items information of a profile",
# )
# def get_profile_action_items(
#     request: Request,
#     uuid: str,
#     user_id: str,
#     impersonate_user_id: Optional[str] = Query(None),
# ) -> ActionItemsResponse:
#     """
#     Get the action items information of a profile - Mock version.
#
#     - **user_id**: Tenant ID
#     - **uuid**: Profile UUID
#     """
#     logger.info(f"Got action items request for profile: {uuid}")
#
#     allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
#     user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
#     response = profiles_api_service.get_profile_action_items(user_id, uuid)
#     logger.info(f"About to send response: {response}")
#     return response


@v1_router.get("/{user_id}/profiles/{uuid}/good-to-know", response_model=GoodToKnowResponse)
def get_profile_good_to_know(
    background_tasks: BackgroundTasks,
    request: Request,
    uuid: str,
    user_id: str,
    impersonate_user_id: Optional[str] = Query(None),
) -> GoodToKnowResponse:
    """
    Get the 'good-to-know' information of a profile - Mock version.

    - **user_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got good-to-know request for profile: {uuid}")

    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    response = profiles_api_service.get_profile_good_to_know(user_id, uuid)
    if not allowed_impersonate_user_id:
        background_tasks.add_task(stats_api_service.view_profile_event, user_id=user_id, profile_id=uuid)
    return response


@v1_router.get("/{user_id}/profiles/{uuid}/sales-criteria", response_model=SalesCriteriaResponse)
def get_profile_sales_criteria(
    request: Request,
    uuid: str,
    user_id: str,
    impersonate_user_id: Optional[str] = Query(None),
) -> SalesCriteriaResponse:
    """
    Get the sales criteria of a profile - Mock version.

    - **user_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got sales criteria request for profile: {uuid}")

    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    response = profiles_api_service.get_sales_criteria(user_id, uuid)
    logger.info(f"About to send response: {response}")
    return response


@v1_router.get("/{user_id}/profiles/{uuid}/action-items", response_model=ActionItemsResponse)
def get_profile_action_items(
    request: Request,
    uuid: str,
    user_id: str,
    impersonate_user_id: Optional[str] = Query(None),
) -> ActionItemsResponse:
    """
    Get the action items of a profile - Mock version.

    - **user_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got action items request for profile: {uuid}")

    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    response = profiles_api_service.get_profile_action_items(user_id, uuid)
    logger.info(f"About to send response: {response}")
    return response

@v1_router.get(
    "/{user_id}/profiles/{uuid}/work-experience",
    response_class=JSONResponse,
)
def get_work_experience(
    request: Request,
    uuid: str,
    user_id: str,
    impersonate_user_id: Optional[str] = Query(None),
) -> JSONResponse:
    """
    Get the work experience of a profile - *Mock version*.

    - **user_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got work experience request for profile: {uuid}")

    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    response = profiles_api_service.get_work_experience(user_id, uuid)
    logger.info(f"About to send response: {response}")
    return JSONResponse(content=response)


@v1_router.get(
    "/{user_id}/{meeting_uuid}/meeting-overview",
    response_model=Union[
        MiniMeetingOverviewResponse, InternalMeetingOverviewResponse, PrivateMeetingOverviewResponse
    ],
)
def get_meeting_overview(
    background_tasks: BackgroundTasks,
    request: Request,
    user_id: str,
    meeting_uuid: str,
    impersonate_user_id: Optional[str] = Query(None),
) -> Union[
    MiniMeetingOverviewResponse, InternalMeetingOverviewResponse, PrivateMeetingOverviewResponse, JSONResponse
]:
    """
    Get the meeting information.

    - **user_id**: Tenant ID
    - **meeting_id**: Meeting ID
    """
    logger.info(f"Got meeting info request for meeting: {meeting_uuid}")

    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    response = meetings_api_service.get_meeting_overview(user_id, meeting_uuid)
    logger.info(f"About to send response: {response}")
    if not allowed_impersonate_user_id:
        background_tasks.add_task(
            stats_api_service.view_meeting_event, user_id=user_id, meeting_id=meeting_uuid
        )
    return response


@v1_router.delete("/{user_id}/{meeting_uuid}", response_class=JSONResponse)
def delete_meeting(
    request: Request,
    user_id: str,
    meeting_uuid: str,
    impersonate_user_id: Optional[str] = Query(None),
) -> JSONResponse:
    """
    Delete a meeting.

    - **user_id**: Tenant ID
    - **meeting_uuid**: Meeting UUID
    """
    logger.info(f"Got delete meeting request for meeting: {meeting_uuid}")

    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    response = meetings_api_service.delete_meeting(user_id, meeting_uuid)
    logger.info(f"About to send response: {response}")
    return JSONResponse(content=response)

@v1_router.post("/fake-meeting")
async def create_fake_meeting(request: Request) -> JSONResponse:
    """
    Create a fake meeting for testing purposes.

    """
    if (
        request.state
        and hasattr(request.state, "user_email")
        and hasattr(request.state, "user_id")
        and email_utils.is_genie_admin(request.state.user_email)
    ):
        body = await request.json()
        user_id = request.state.user_id
        emails = body.get("emails")
        linkedins = body.get("linkedins")
        if linkedins:
            for linkedin in linkedins:
                fake_linkedin_email = email_utils.create_fake_linkedin_email(linkedin)
                emails.append(fake_linkedin_email)
        logger.info(f"Creating fake meeting for user: {user_id} and emails: {emails}")
        meetings_api_service.create_fake_meeting(user_id, emails)
        return JSONResponse(content={"status": "success", "message": "Fake meeting created"})
    else:
        raise HTTPException(status_code=403, detail="Forbidden endpoint")


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

@v1_router.get("/internal/sync-profile-params/{person_uuid}")
def sync_profile_params(person_uuid: str, api_key: str) -> JSONResponse:
    """
    Sync a profile with the PDL API.

    - **person_uuid**: The UUID of the person to sync.
    - **api_key**: The internal API key
    """
    if api_key != INTERNAL_API_KEY:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    response = admin_api_service.sync_params(person_uuid)
    return JSONResponse(content=response)


@v1_router.get("/internal/latest-profiles")
def get_latest_profiles(request: Request, limit: int = 3, search_term: str = None) -> JSONResponse:
    """
    Get the latest profiles created by genie for admins.

    """
    if (
        request.state
        and hasattr(request.state, "user_email")
        and email_utils.is_genie_admin(request.state.user_email)
    ):
        response = admin_api_service.get_latest_profiles(limit, search_term)
        return JSONResponse(content=response)
    else:
        raise HTTPException(status_code=403, detail="Forbidden endpoint")


@v1_router.post("/{user_id}/{uuid}/update-action-item", response_class=JSONResponse)
async def update_action_item(
        user_id: str,
        uuid: str,
        request: Request,
        impersonate_user_id: Optional[str] = Query(None),
):
    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    body = await request.json()
    criteria = body.get("criteria")
    description = body.get("description")
    logger.info(f"Updating action item for tenant: {user_id}, uuid: {uuid}, criteria: {criteria}, description: {description}")
    response = admin_api_service.update_action_item(user_id, uuid, criteria, description)
    return JSONResponse(content=response)


@v1_router.post("/internal/update-profiles")
async def update_profiles(request: Request) -> JSONResponse:
    """
    Update profiles for admins.

    """
    if (
        request.state
        and hasattr(request.state, "user_email")
        and email_utils.is_genie_admin(request.state.user_email)
    ):
        body = await request.json()
        response = admin_api_service.update_profiles(body)
        return JSONResponse(content=response)
    else:
        raise HTTPException(status_code=403, detail="Forbidden endpoint")


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


@v1_router.get("/internal/sync-action-items")
def process_action_items(background_tasks: BackgroundTasks, api_key: str,
                         num_sync: int = 5,
                         force_refresh: bool = False) -> JSONResponse:
    if api_key != INTERNAL_API_KEY:
        logger.error(f"Invalid API key: {api_key}")
        return JSONResponse(content={"error": "Invalid API key"})
    logger.info(f"Processing {num_sync} action items")
    background_tasks.add_task(admin_api_service.sync_action_items, num_sync, force_refresh)
    return JSONResponse(content={"status": "success", "message": "Processing action items"})


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
    "/google/import-meetings/{user_id}",
    response_class=JSONResponse,
    summary="Fetches all Google Calendar meetings for a tenant",
    include_in_schema=False,
)
def import_google_meetings(
    request: Request,
    user_id: str,
    meetings_number: Optional[int] = Query(30, description="Number of meetings to fetch"),
    impersonate_user_id: Optional[str] = Query(None),
) -> JSONResponse:
    """
    Fetches all Google Calendar meetings for a given tenant.
    """
    allowed_impersonate_user_id = get_user_id_to_impersonate(impersonate_user_id, request)
    user_id = allowed_impersonate_user_id if allowed_impersonate_user_id else user_id
    logger.info(f"Meeting number: {meetings_number}")
    logger.info(f"Received Google meetings request for tenant: {user_id}")
    response = users_api_service.import_google_meetings(user_id, meetings_number)
    logger.info(f"Finished fetching meetings")
    return JSONResponse(content=response)

templates = Jinja2Templates(directory="templates")

@v1_router.get("/posts", response_class=HTMLResponse)
async def get_form(request: Request):
    """
    Serve the form template.
    """
    return templates.TemplateResponse("multi-post-form.html", {"request": request})


# @v1_router.post("/posts/submit")
# async def handle_posts_form(
#     request: Request,
# ):
#     """
#     Handle form submission and return parsed data.
#     """
#     body = await request.json()
#     response = await params_api_service.evaluate_posts(body['linkedin'], body['num_posts'], body['name'], body['selected_numbers'])
#
#     # Return parsed data
#     return JSONResponse(content=response)

# @v1_router.get("/params", response_class=HTMLResponse)
# async def get_form(request: Request):
#     """
#     Serve the form template.
#     """
#     return templates.TemplateResponse("form.html", {"request": request})
#
# @v1_router.post("/params/submit")
# async def handle_form(
#     request: Request,
# ):
#     """
#     Handle form submission and return parsed data.
#     """
#     body = await request.json()
#     await params_api_service._initialize_sheet()
#     response = await params_api_service.evaluate_param(body['artifact'], body['name'], body['position'], body['company'], body['param_id'])
#
#     # Return parsed data
#     return JSONResponse(content=response)


# @v1_router.post("/salesforce/contact", response_class=JSONResponse)
# async def new_contact(request: Request):
#     """
#     Get new contact from salesforce and start the process of creating a new profile.
#     """
#     body = await request.json()
#     contact_email = body.get("email")
#     logger.info(f"Received new contact request for email: {contact_email}")
#     salesforce_id = body.get("salesforce_id")
#     logger.info(f"Received new contact request for salesforce_id: {salesforce_id}")
#     response = await salesforce_api_service.handle_new_contact(contact_email=contact_email, salesforce_user_id=salesforce_id)
#     return JSONResponse(content=response)


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
    # if (
    #     request.state
    #     and hasattr(request.state, "user_email")
    #     and email_utils.is_genie_admin(request.state.user_email)
    # ):
    response = admin_api_service.fetch_all_tenants()
    logger.info(f"Returning tenants: {response}")
    return JSONResponse(content=response)

    # else:
    #     raise HTTPException(status_code=403, detail="Forbidden endpoint")


@v1_router.get(
    "/admin/users",
    response_class=JSONResponse,
    summary="Fetches all users for an admin",
    include_in_schema=False,
)
def fetch_all_users(
    request: Request,
) -> JSONResponse:
    """
    Fetches all users for an admin
    """
    if (
        request.state
        and hasattr(request.state, "user_email")
        and email_utils.is_genie_admin(request.state.user_email)
    ):
        response = admin_api_service.fetch_all_users()
        logger.info(f"Returning users: {response}")
        return JSONResponse(content=response)

    else:
        raise HTTPException(status_code=403, detail="Forbidden endpoint")


def get_tenant_id_to_impersonate(
    impersonate_tenant_id: str,
    request: Request,
):
    logger.info(f"Checking if user is impersonating tenant")
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


def get_user_id_to_impersonate(
        impersonate_user_id: str,
        request: Request,
):
    logger.info(f"Checking if user is impersonating user")
    if (
            impersonate_user_id
            and request.state
            and hasattr(request.state, "user_email")
            and email_utils.is_genie_admin(request.state.user_email)
    ):
        logger.info(f"User is impersonating user")
        return impersonate_user_id
    logger.info(f"User is not impersonating user")
    return None


def get_request_state_value(request: Request, key: str):
    if request and request.state and hasattr(request.state, key):
        return getattr(request.state, key)
    return None
