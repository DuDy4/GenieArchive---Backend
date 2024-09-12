import asyncio
import json
import os
import time
import traceback
import requests
import urllib.parse
import uuid

from fastapi import Depends, FastAPI, Request, HTTPException, Query
from fastapi.routing import APIRouter
from common.genie_logger import GenieLogger

from common.utils import env_utils, email_utils, job_utils
from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from data.data_common.utils.str_utils import titleize_values, to_custom_title_case, titleize_name
from data.internal_services.tenant_service import TenantService

from starlette.responses import PlainTextResponse, RedirectResponse, JSONResponse
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleRequest
from googleapiclient.discovery import build
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from google.oauth2 import id_token
from google.auth import credentials

from data.api.base_models import *
import datetime

from data.data_common.repositories.hobbies_repository import HobbiesRepository
from data.data_common.repositories.personal_data_repository import (
    PersonalDataRepository,
)
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
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.utils.str_utils import get_uuid4

from data.api_services.auth0 import handle_auth0_user_signup
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

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

v1_router = APIRouter(prefix="/v1")


@v1_router.get("/test-google-token")
def test_google_token(token: str):
    token_response = requests.get(GOOGLE_TOKEN_URI, params={"id_token": token})

    if token_response.status_code != 200:
        logger.error(f"Token request failed: {token_response.raise_for_status()}")
        raise HTTPException(status_code=400, detail="Failed to fetch token")

    tokens = token_response.json()
    logger.debug(f"Tokens: {tokens}")
    return tokens


@v1_router.post("/successful-login")
async def post_successful_login(
    request: Request,
    google_creds_repository: GoogleCredsRepository = Depends(google_creds_repository),
    tenants_repository: TenantsRepository = Depends(tenants_repository),
):
    """
    Returns a tetant ID.
    """
    logger.info("Received JWT data")
    auth_data = await request.json()
    logger.info(f"Received auth data: {auth_data}")
    auth_claims = auth_data["data"]["claims"]
    user_email = auth_claims.get("email")
    user_tenant_id = auth_claims.get("tenantId")
    user_name = auth_claims.get("userId")
    logger.info(f"Fetching google meetings for user email: {user_email}, tenant ID: {user_tenant_id}")
    tenant_data = {"tenantId": user_tenant_id, "name": user_name, "email": user_email, "user_id": user_name}
    tenants_repository.insert(tenant_data)
    fetch_google_meetings(user_email, google_creds_repository, tenants_repository)
    response = {
        "claims": {
            "tenantId": user_tenant_id,
        },
    }
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
        user_name = user_info.get("name")
        user_id = user_info.get("user_id")
        user_email = user_info.get("user_email")
        user_access_token = user_info.get("google_access_token")
        user_refresh_token = user_info.get("google_refresh_token")
        tenant_id = user_info.get("tenant_id")
        tenant_name = user_info.get("tenant_name")

        if tenants_repository.exists(tenant_id):
            logger.debug(f"Tenant ID {tenant_id} already exists in database")
            google_creds_repository.save_creds(user_email, user_access_token, user_refresh_token)
            logger.debug(f"Updated google creds for user: {user_email}. About to fetch google meetings")
            fetch_google_meetings(user_email, google_creds_repository, tenants_repository)
        elif tenants_repository.email_exists(user_email):
            logger.debug(f"Another tenant ID exists for this email: {user_email}. About to update tenant ID")
            old_tenant_id = tenants_repository.get_tenant_id_by_email(user_email)
            TenantService.changed_old_tenant_to_new_tenant(
                new_tenant_id=tenant_id, old_tenant_id=old_tenant_id, user_id=user_id, user_name=user_name
            )
            google_creds_repository.save_creds(user_email, user_access_token, user_refresh_token)
        else:
            # Signup new user
            logger.debug(f"About to signup new user: {user_email}")
            tenants_repository.insert(
                {
                    "uuid": get_uuid4(),
                    "tenantId": tenant_id,
                    "name": tenant_name,
                    "email": user_email,
                    "user_id": user_id,
                }
            )
            google_creds_repository.save_creds(user_email, user_access_token, user_refresh_token)

        return JSONResponse(content={"message": "User signup successful"}, status_code=200)
    except Exception as e:
        logger.error(f"Error during user signup: {str(e)}")
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


@v1_router.get(
    "/{tenant_id}/profiles",
    response_model=List[ProfileDTO],
    include_in_schema=False,
    summary="Gets all profiles for a given tenant",
)
async def get_all_profiles(
    request: Request,
    tenant_id: str,
    search: str = Query(None, description="Partial text to search profile names"),
    ownerships_repository: OwnershipsRepository = Depends(ownerships_repository),
    profiles_repository: ProfilesRepository = Depends(profiles_repository),
) -> List[ProfileDTO]:
    """
    Gets all profiles for a given tenant.
    """
    logger.info(f"Received get profiles request, with search: '{search}'")

    profiles_uuid = ownerships_repository.get_all_persons_for_tenant(tenant_id)
    logger.info(f"Got profiles_uuid: {profiles_uuid}")

    profiles_list = profiles_repository.get_profiles_from_list(profiles_uuid, search)
    logger.info(f"Got profiles: {len(profiles_list)}")
    profiles_response_list = [ProfileResponse(profile=profile) for profile in profiles_list]

    logger.debug(f"Profiles: {[profile.profile.name for profile in profiles_response_list]}")
    return profiles_list


@v1_router.get(
    "/{tenant_id}/meetings",
    response_model=MeetingsListResponse,
    summary="Gets all *meeting* that the tenant has profiles participants in",
)
async def get_all_meetings_by_profile_name(
    tenant_id: str,
    # name: str = Query(None, description="Partial text to search profile names"),
    ownerships_repository: OwnershipsRepository = Depends(ownerships_repository),
    persons_repository: PersonsRepository = Depends(persons_repository),
    meetings_repository: MeetingsRepository = Depends(meetings_repository),
) -> MeetingsListResponse:
    """
    Gets all *meeting* that the tenant has profiles participants in.

    Steps:
    1. Get all persons for the tenant.
    2. Get all emails for the persons with name that includes the search text.
    3. Get all meetings with participants that have the emails.

    """
    logger.info(f"Received get profiles request, with tenant: {tenant_id}")

    if not tenant_id:
        logger.error("Tenant ID not provided")
        return JSONResponse(content={"error": "Tenant ID not provided"})

    meetings = meetings_repository.get_all_meetings_by_tenant_id(tenant_id)
    dict_meetings = [meeting.to_dict() for meeting in meetings]
    # sort by meeting.start_time
    dict_meetings.sort(key=lambda x: x["start_time"])
    logger.info(f"About to sent to {tenant_id} meetings: {len(dict_meetings)}")
    return JSONResponse(content=dict_meetings)


# @v1_router.get("/{tenant_id}/{meeting_id}/profiles", response_model=MiniProfilesListResponse)
# def get_all_profile_and_persons_for_meeting(
#     tenant_id: str,
#     meeting_id: str,
#     meetings_repository: MeetingsRepository = Depends(meetings_repository),
#     ownerships_repository: OwnershipsRepository = Depends(ownerships_repository),
#     persons_repository: PersonsRepository = Depends(persons_repository),
#     profiles_repository: ProfilesRepository = Depends(profiles_repository),
#     tenants_repository: TenantsRepository = Depends(tenants_repository),
# ) -> MiniProfilesListResponse:
#     """
#     Get all profile IDs and names for a specific meeting.
#
#     - **tenant_id**: Tenant ID - the right one is 'abcde'
#     - **meeting_id**: Meeting ID
#     """
#     logger.info(f"Received profiles request for meeting: {meeting_id}")
#     meeting = meetings_repository.get_meeting_data(meeting_id)
#     if not meeting:
#         return JSONResponse(content={"error": "Meeting not found"})
#     if meeting.tenant_id != tenant_id:
#         return JSONResponse(content={"error": "Tenant mismatch"})
#     tenant_email = tenants_repository.get_tenant_email(tenant_id)
#     logger.info(f"Tenant email: {tenant_email}")
#     participants_emails = meeting.participants_emails
#     logger.debug(f"Participants emails: {participants_emails}")
#     filtered_participants_emails = email_utils.filter_emails(
#         host_email=tenant_email, participants_emails=participants_emails
#     )
#     logger.info(f"Filtered participants emails: {filtered_participants_emails}")
#     filtered_emails = filtered_participants_emails
#     logger.info(f"Filtered emails: {filtered_emails}")
#     persons = []
#     for email in filtered_emails:
#         person = persons_repository.find_person_by_email(email)
#         if person:
#             persons.append(person)
#     logger.info(f"Got persons for the meeting: {[persons.uuid for persons in persons]}")
#     profiles = []
#     for person in persons:
#         profile = profiles_repository.get_profile_data(person.uuid)
#         logger.info(f"Got profile: {str(profile)[:300]}")
#         if profile:
#             profiles.append(profile)
#     logger.debug(f"Got profiles: {len(profiles)}")
#     persons_without_profiles = [
#         person.to_dict()
#         for person in persons
#         if str(person.uuid) not in [str(profile.uuid) for profile in profiles]
#     ]
#     logger.info(f"Got persons without profiles: {persons_without_profiles}")
#     logger.info(f"Sending profiles: {[profile.uuid for profile in profiles]}")
#     mini_profiles_list = [MiniProfileResponse.from_profile_dto(profile) for profile in profiles]
#     mini_persons_list = [MiniPersonResponse.from_dict(person) for person in persons_without_profiles]
#     return MiniProfilesListResponse(profiles=mini_profiles_list, persons=mini_persons_list)
# return [MiniProfileResponse.from_profile_dto(profiles[i], persons[i]) for i in range(len(profiles))]


@v1_router.get("/{tenant_id}/{meeting_id}/profiles", response_model=List[MiniProfileResponse])
def get_all_profile_for_meeting(
    tenant_id: str,
    meeting_id: str,
    meetings_repository: MeetingsRepository = Depends(meetings_repository),
    ownerships_repository: OwnershipsRepository = Depends(ownerships_repository),
    persons_repository: PersonsRepository = Depends(persons_repository),
    profiles_repository: ProfilesRepository = Depends(profiles_repository),
    tenants_repository: TenantsRepository = Depends(tenants_repository),
) -> List[MiniProfileResponse]:
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
    filtered_participants_emails = email_utils.filter_emails(
        host_email=tenant_email, participants_emails=participants_emails
    )
    logger.info(f"Filtered participants emails: {filtered_participants_emails}")
    filtered_emails = filtered_participants_emails
    logger.info(f"Filtered emails: {filtered_emails}")
    persons = []
    for email in filtered_emails:
        person = persons_repository.find_person_by_email(email)
        if person:
            persons.append(person)
    logger.info(f"Got persons for the meeting: {[persons.uuid for persons in persons]}")
    profiles = []
    for person in persons:
        profile = profiles_repository.get_profile_data(person.uuid)
        logger.info(f"Got profile: {str(profile)[:300]}")
        if profile:
            profiles.append(profile)
    persons_without_profiles = [
        person.to_dict() for person in persons if person.uuid not in [profile.uuid for profile in profiles]
    ]
    logger.info(f"Got persons without profiles: {persons_without_profiles}")
    logger.info(f"Sending profiles: {[profile.uuid for profile in profiles]}")
    return [MiniProfileResponse.from_profile_dto(profiles[i], persons[i]) for i in range(len(profiles))]


@v1_router.get("/{tenant_id}/profiles/{uuid}/attendee-info", response_model=AttendeeInfo)
def get_profile_attendee_info(
    uuid: str,
    tenant_id: str,
    ownerships_repository: OwnershipsRepository = Depends(ownerships_repository),
    profiles_repository: ProfilesRepository = Depends(profiles_repository),
    personal_data_repository: PersonalDataRepository = Depends(personal_data_repository),
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

    # This will Upper Camel Case and Titleize the values in the profile
    profile = ProfileDTO.from_dict(profile.to_dict())

    picture = profile.picture_url
    name = titleize_name(profile.name)
    company = profile.company
    position = profile.position
    links = personal_data_repository.get_social_media_links(uuid)
    logger.info(f"Got links: {links}, type: {type(links)}")
    profile = {
        "picture": picture,
        "name": name,
        "company": company,
        "position": position,
        "social_media_links": SocialMediaLinksList.from_list(links).to_list() if links else [],
    }
    logger.info(f"Attendee info: {profile}")
    return AttendeeInfo(**profile)


@v1_router.get(
    "/{tenant_id}/profiles/{uuid}/strengths",
    response_model=StrengthsListResponse,
    summary="Fetches strengths of a profile",
)
def get_profile_strengths(
    uuid: str,
    tenant_id: str,
    ownerships_repository: OwnershipsRepository = Depends(ownerships_repository),
    profiles_repository: ProfilesRepository = Depends(profiles_repository),
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
        strengths_formatted = "".join([f"\n{strength}\n" for strength in profile.strengths])
        logger.info(f"strengths: {strengths_formatted}")
        return StrengthsListResponse(strengths=profile.strengths)
    return JSONResponse(content={"error": "Could not find profile"})


@v1_router.get(
    "/{tenant_id}/profiles/{uuid}/get-to-know",
    response_model=GetToKnowResponse,
    summary="Fetches 'get-to-know' information of a profile",
)
def get_profile_get_to_know(
    uuid: str,
    tenant_id: str,
    ownerships_repository: OwnershipsRepository = Depends(ownerships_repository),
    profiles_repository: ProfilesRepository = Depends(profiles_repository),
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
    logger.info(f"Got profile: {str(profile)[:300]}")
    if profile:
        formated_get_to_know = "".join(
            [(f"\n{key}: {value}\n") for key, value in profile.get_to_know.items()]
        )
        logger.info(f"Get to know: {formated_get_to_know}")
        return GetToKnowResponse(**profile.get_to_know)
    return JSONResponse(content={"error": "Could not find profile"})


@v1_router.get("/{tenant_id}/profiles/{uuid}/good-to-know", response_model=GoodToKnowResponse)
def get_profile_good_to_know(
    uuid: str,
    tenant_id: str,
    profiles_repository: ProfilesRepository = Depends(profiles_repository),
    ownerships_repository: OwnershipsRepository = Depends(ownerships_repository),
    hobbies_repository: HobbiesRepository = Depends(hobbies_repository),
    companies_repository: CompaniesRepository = Depends(companies_repository),
    persons_repository: PersonsRepository = Depends(persons_repository),
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
        profile_email = persons_repository.get_person_email(uuid)
        logger.info(f"Got profile email: {profile_email}")
        news = companies_repository.get_news_data_by_email(profile_email)
        logger.info(f"Got news: {news}")

        hobbies_uuid = profile.hobbies
        logger.info(f"Got hobbies: {hobbies_uuid}")
        hobbies = [hobbies_repository.get_hobby(str(hobby_uuid)) for hobby_uuid in hobbies_uuid]
        logger.info(f"Got hobbies: {hobbies}")

        connections = profile.connections

        good_to_know = {
            "news": news,
            "hobbies": hobbies,
            "connections": connections,
        }
        formatted_good_to_know = "".join([(f"\n{key}: {value}\n") for key, value in good_to_know.items()])
        logger.info(f"Good to know: {formatted_good_to_know}")
        return GoodToKnowResponse(
            news=news if news else [],
            hobbies=hobbies if hobbies else [],
            connections=connections if connections else [],
        )
    return JSONResponse(content={"error": "Could not find profile"})


@v1_router.get(
    "/{tenant_id}/profiles/{uuid}/work-experience",
    response_model=WorkExperienceResponse,
)
def get_work_experience(
    uuid: str,
    tenant_id: str,
    personal_data_repository: PersonalDataRepository = Depends(personal_data_repository),
    ownerships_repository: OwnershipsRepository = Depends(ownerships_repository),
) -> WorkExperienceResponse:
    """
    Get the work experience of a profile - *Mock version*.

    - **tenant_id**: Tenant ID
    - **uuid**: Profile UUID
    """
    logger.info(f"Got work experience request for profile: {uuid}")

    personal_data = personal_data_repository.get_pdl_personal_data(uuid)

    if not ownerships_repository.check_ownership(tenant_id, uuid):
        return JSONResponse(content={"error": "Profile not found under this tenant"})

    if personal_data:
        experience = personal_data["experience"]
        fixed_experience = job_utils.fix_and_sort_experience_from_pdl(experience)
    else:
        personal_data = personal_data_repository.get_apollo_personal_data(uuid)
        fixed_experience = job_utils.fix_experience_from_apollo_data(personal_data)
    if fixed_experience:
        short_fixed_experience = fixed_experience[:10]
        return JSONResponse(content=(to_custom_title_case(short_fixed_experience)))

    return JSONResponse(content={"error": "Could not find profile"})


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
    "/{tenant_id}/meeting/{meeting_uuid}",
    response_model=MeetingResponse,
)
def get_meeting_info(
    tenant_id: str,
    meeting_uuid: str,
    meetings_repository: MeetingsRepository = Depends(meetings_repository),
    companies_repository: CompaniesRepository = Depends(companies_repository),
) -> JSONResponse:
    """
    Get the meeting information.

    - **tenant_id**: Tenant ID
    - **meeting_id**: Meeting ID
    """
    logger.info(f"Got meeting info request for meeting: {meeting_uuid}")

    meeting = meetings_repository.get_meeting_data(meeting_uuid)
    if not meeting:
        return JSONResponse(content={"error": "Meeting not found"})

    if meeting.tenant_id != tenant_id:
        return JSONResponse(content={"error": "Tenant mismatch"})

    participants = [ParticipantEmail.from_dict(email) for email in meeting.participants_emails]
    host_email_list = [email.email_address for email in participants if email.self]
    host_email = host_email_list[0] if host_email_list else None
    logger.debug(f"Host email: {host_email}")
    filtered_participants_emails = email_utils.filter_emails(host_email, participants)
    logger.info(f"Filtered participants: {filtered_participants_emails}")

    domain_emails = [email.split("@")[1] for email in filtered_participants_emails]
    domain_emails = list(set(domain_emails))
    logger.info(f"Domain emails: {domain_emails}")

    companies = []

    for domain in domain_emails:
        company = companies_repository.get_company_from_domain(domain)
        logger.info(f"Company: {company}")
        company_response = CompanyResponse.from_company_dto(company)
        # company_dict = company.to_dict()
        # if company:
        #     company_dict.pop("uuid")
        #     company_dict.pop("domain")
        #     company_dict.pop("employees")
        #     companies.append(company_dict)
        companies.append(company_response)

    logger.info(f"Companies: {companies}")

    meeting_dict = meeting.to_dict()
    meeting_dict.pop("participants_hash")
    meeting_dict.pop("tenant_id")
    meeting_dict.pop("google_calendar_id")
    meeting_dict["companies"] = companies

    return MeetingResponse.from_dict(meeting_dict)


@v1_router.get(
    "/{tenant_id}/meeting-overview/{meeting_uuid}",
    response_model=Union[
        MiniMeetingOverviewResponse, InternalMeetingOverviewResponse, PrivateMeetingOverviewResponse
    ],  # Use only the Pydantic model here
)
def get_meeting_overview(
    tenant_id: str,
    meeting_uuid: str,
    meetings_repository: MeetingsRepository = Depends(meetings_repository),
    companies_repository: CompaniesRepository = Depends(companies_repository),
    profiles_repository: ProfilesRepository = Depends(profiles_repository),
    persons_repository: PersonsRepository = Depends(persons_repository),
) -> Union[
    MiniMeetingOverviewResponse, InternalMeetingOverviewResponse, PrivateMeetingOverviewResponse, JSONResponse
]:
    """
    Get the meeting information.

    - **tenant_id**: Tenant ID
    - **meeting_id**: Meeting ID
    """
    logger.info(f"Got meeting info request for meeting: {meeting_uuid}")

    meeting = meetings_repository.get_meeting_data(meeting_uuid)
    if not meeting:
        return JSONResponse(content={"error": "Meeting not found"}, status_code=404)

    if meeting.classification.value == MeetingClassification.PRIVATE.value:
        private_meeting = MiniMeeting.from_meeting_dto(meeting)
        logger.info(f"Private meeting: {private_meeting}")
        return PrivateMeetingOverviewResponse(meeting=private_meeting)

    if meeting.classification.value == MeetingClassification.INTERNAL.value:
        mini_meeting = MiniMeeting.from_meeting_dto(meeting)
        logger.info(f"Mini meeting: {mini_meeting}")
        participants_emails = meeting.participants_emails
        participants = []
        for email_object in participants_emails:
            email = email_object.get("email")
            if not email:
                logger.warning(f"Email not found in: {email_object}")
                continue
            person = persons_repository.find_person_by_email(email)
            if person:
                mini_person = MiniPersonResponse.from_dict(person.to_dict())
                logger.debug(f"Person: {mini_person}")
                participants.append(mini_person)
            else:
                mini_person = MiniPersonResponse.from_dict({"uuid": get_uuid4(), "email": email})
                logger.debug(f"Person: {mini_person}")
                participants.append(mini_person)
        internal_meeting_overview = InternalMeetingOverviewResponse(
            meeting=mini_meeting,
            participants=participants,
        )
        logger.info(f"Internal meeting overview: {internal_meeting_overview}")
        return internal_meeting_overview

    if meeting.tenant_id != tenant_id:
        return JSONResponse(content={"error": "Tenant mismatch"}, status_code=400)
    try:
        mini_meeting = MiniMeeting.from_meeting_dto(meeting)
    except Exception as e:
        logger.error(f"Error creating mini meeting: {e}")
        return JSONResponse(content={"error": "Could not process meeting"}, status_code=500)
    logger.info(f"Mini meeting: {mini_meeting}")

    participants = [ParticipantEmail.from_dict(email) for email in meeting.participants_emails]
    host_email_list = [email.email_address for email in participants if email.self]
    host_email = host_email_list[0] if host_email_list else None
    logger.debug(f"Host email: {host_email}")
    filtered_participants_emails = email_utils.filter_emails(host_email, participants)
    logger.info(f"Filtered participants: {filtered_participants_emails}")

    domain_emails = [email.split("@")[1] for email in filtered_participants_emails]
    domain_emails = list(set(domain_emails))
    logger.info(f"Domain emails: {domain_emails}")

    companies = []

    for domain in domain_emails:
        company = companies_repository.get_company_from_domain(domain)
        logger.info(f"Company: {str(company)[:300]}")
        if company:
            companies.append(company)

    if not companies:
        logger.error("No companies found")
        return JSONResponse(
            content={
                "error": "No companies found in this meeting. Might be that we are still process the data."
            },
            status_code=404,
        )

    company = companies[0] if companies else None
    logger.info(f"Company: {str(company)[:300]}")
    if company:
        news = []
        domain = company.domain
        try:
            for new in company.news:
                link = HttpUrl(new.get("link") if new and isinstance(new, dict) else str(new.link))
                if isinstance(new, dict):
                    new["link"] = link
                elif isinstance(new, NewsData):
                    new.link = link
                if domain not in str(link):
                    news.append(new)
            company.news = news[:3]
            logger.debug(f"Company news: {str(company.news)[:300]}")
        except Exception as e:
            logger.error(f"Error processing company news: {e}")
            company.news = []
        mid_company = titleize_values(MidMeetingCompany.from_company_dto(company))

    logger.info(f"Company: {str(mid_company)[:300]}")

    mini_participants = []

    for participant in filtered_participants_emails:
        profile = profiles_repository.get_profile_data_by_email(participant)
        if profile:
            person = PersonDTO.from_dict({"email": participant})
            person.uuid = profile.uuid
            logger.info(f"Person: {person}")
            profile_response = MiniProfileResponse.from_profile_dto(profile, person)
            logger.info(f"Profile: {profile_response}")
            if profile_response:
                mini_participants.append(profile_response)
        # else:
        #     person = persons_repository.find_person_by_email(participant)
        #     if person:
        #         person_response = MiniPersonResponse.from_dict(person.to_dict())
        #     else:
        #         person = PersonDTO.from_dict({"email": participant})
        #         person_response = MiniPersonResponse.from_dict(person.to_dict())
        #     logger.debug(f"Person: {person_response}")
        #     mini_participants.append(person_response)

    if not mini_participants:
        logger.error("No participants found")
        return JSONResponse(content={"error": "No participants found in this meeting."}, status_code=404)

    logger.info(f"Meeting participants: {mini_participants}")

    try:
        mini_overview = MiniMeetingOverviewResponse(
            meeting=mini_meeting,
            company=mid_company,
            participants=mini_participants,
        )
    except Exception as e:
        logger.error(f"Error creating mini overview: {e}")
        return JSONResponse(content={"error": "Error creating mini overview"}, status_code=500)
    logger.info(f"Mini overview: {str(mini_overview)[:300]}")

    return mini_overview


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


def validate_uuid(uuid_string: str):
    try:
        val = uuid.UUID(uuid_string, version=4)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")
    return str(val)
