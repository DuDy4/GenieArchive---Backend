import asyncio
import json
import os
import time
import traceback
import datetime
import requests
import urllib.parse
import uuid


from fastapi import Depends, FastAPI, Request, HTTPException, Query
from fastapi.routing import APIRouter
from common.genie_logger import GenieLogger

from common.utils import env_utils
from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from data.data_common.utils.str_utils import titleize_values, to_custom_title_case, titleize_name

from starlette.responses import PlainTextResponse, RedirectResponse, JSONResponse
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleRequest
from googleapiclient.discovery import build
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from google.oauth2 import id_token
from google.auth import credentials


from data.api.base_models import *
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
    contacts_repository,
    salesforce_event_handler,
    tenants_repository,
    meetings_repository,
    google_creds_repository,
    ownerships_repository,
    persons_repository,
    personal_data_repository,
    hobbies_repository,
    companies_repository,
)

from data.pdl_consumer import PDLClient
from data.apollo_consumer import ApolloConsumer

from data.data_common.events.topics import Topic
from data.data_common.events.genie_event import GenieEvent
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.utils.str_utils import get_uuid4

from data.meetings_consumer import MeetingManager

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


@v1_router.get("/user-info", response_model=UserResponse)
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


@v1_router.post("/successful-login")
async def post_successful_login(
    request: Request,
    google_creds_repository: GoogleCredsRepository = Depends(google_creds_repository),
    tenants_repository: TenantsRepository = Depends(tenants_repository),
):
    """
    Returns a tetant ID - MOCK.
    """
    logger.info("Received JWT data")
    auth_data = await request.json()
    logger.info(f"Received auth data: {auth_data}")
    auth_claims = auth_data["data"]["claims"]
    user_email = auth_claims.get("email")
    user_tenant_id = auth_claims.get("tenantId")
    user_name = auth_claims.get("userId")
    logger.info(f"Fetching google meetings for user email: {user_email}, tenant ID: {user_tenant_id}")
    tenant_data = {"tenantId": user_tenant_id, "name": user_name, "email": user_email}
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
        tenants_repository.create_table_if_not_exists()
        uuid = tenants_repository.exists(data.get("tenantId"), data.get("name"))

        if uuid:
            logger.info(f"User already exists in database")
            salesforce_creds = tenants_repository.get_salesforce_credentials(data.get("tenantId"))
            logger.debug(f"Salesforce creds: {salesforce_creds}")
            return {
                "message": "User already exists in database",
                "salesforce_creds": salesforce_creds,
            }
        uuid = tenants_repository.insert(data)
        logger.debug(f"User account created successfully with uuid: {uuid}")

        # salesforce_creds = tenants_repository.get_salesforce_credentials(
        #     data.get("tenantId")
        # )
        # logger.debug(f"Salesforce creds: {salesforce_creds}")
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

    # persons_uuid = ownerships_repository.get_all_persons_for_tenant(tenant_id)
    # logger.info(f"Got persons_uuid: {persons_uuid}")
    # persons_emails = persons_repository.get_emails_list(persons_uuid, name)
    # logger.info(f"Got persons_emails: {persons_emails}")
    # meetings = meetings_repository.get_meetings_by_participants_emails(persons_emails)
    meetings = meetings_repository.get_all_meetings_by_tenant_id(tenant_id)
    dict_meetings = [meeting.to_dict() for meeting in meetings]
    # sort by meeting.start_time
    dict_meetings.sort(key=lambda x: x["start_time"])
    logger.info(f"About to sent to {tenant_id} meetings: {len(dict_meetings)}")
    return JSONResponse(content=dict_meetings)


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
    filtered_participants_emails = MeetingManager.filter_emails(
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
    if links and len(links) > 0:
        for link in links:
            link.pop("id") if link.get("id") else None
            link.pop("username") if link.get("username") else None
            link["platform"] = link.pop("network") if link.get("network") else None
    profile = {
        "picture": picture,
        "name": name,
        "company": company,
        "position": position,
        "social_media_links": links or [],
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
        return GoodToKnowResponse(**good_to_know)
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
        fixed_experience = PDLClient.fix_and_sort_experience(experience)
    else:
        personal_data = personal_data_repository.get_apollo_personal_data(uuid)
        fixed_experience = ApolloConsumer.fix_experience_from_apollo_data(personal_data)
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
    filtered_participants_emails = MeetingManager.filter_emails(host_email, participants)
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
    response_model=MiniMeetingOverviewResponse,
)
def get_meeting_overview(
    tenant_id: str,
    meeting_uuid: str,
    meetings_repository: MeetingsRepository = Depends(meetings_repository),
    companies_repository: CompaniesRepository = Depends(companies_repository),
    profiles_repository: ProfilesRepository = Depends(profiles_repository),
) -> MiniMeetingOverviewResponse:
    """
    Get the meeting information.

    - **tenant_id**: Tenant ID
    - **meeting_id**: Meeting ID
    """
    logger.info(f"Got meeting info request for meeting: {meeting_uuid}")

    meeting = meetings_repository.get_meeting_data(meeting_uuid)
    if not meeting:
        return JSONResponse(content={"error": "Meeting not found"}, status_code=404)

    if meeting.tenant_id != tenant_id:
        return JSONResponse(content={"error": "Tenant mismatch"}, status_code=400)

    meeting_dict = meeting.to_dict()
    meeting_to_send = {
        "subject": meeting.subject,
        "video_link": meeting.link,
        "guidelines": meeting_dict.get("guidelines") if meeting_dict.get("guidelines") else None,
    }
    mini_meeting = MiniMeeting.from_dict(meeting_to_send)
    logger.info(f"Mini meeting: {mini_meeting}")

    participants = [ParticipantEmail.from_dict(email) for email in meeting.participants_emails]
    host_email_list = [email.email_address for email in participants if email.self]
    host_email = host_email_list[0] if host_email_list else None
    logger.debug(f"Host email: {host_email}")
    filtered_participants_emails = MeetingManager.filter_emails(host_email, participants)
    logger.info(f"Filtered participants: {filtered_participants_emails}")

    domain_emails = [email.split("@")[1] for email in filtered_participants_emails]
    domain_emails = list(set(domain_emails))
    logger.info(f"Domain emails: {domain_emails}")

    companies = []

    for domain in domain_emails:
        company = companies_repository.get_company_from_domain(domain)
        logger.info(f"Company: {company}")
        companies.append(company)

    logger.info(f"Companies: {companies}")
    mid_company = {}
    if companies:
        company = companies[0]
        news = []
        domain = company.domain
        for new in company.news:
            link = HttpUrl(new.get("link") if new and isinstance(new, dict) else str(new.link))
            if isinstance(new, dict):
                new["link"] = link
            elif isinstance(new, NewsData):
                new.link = link
            if domain not in str(link):
                news.append(new)
        company.news = news[:3]
        logger.debug(f"Company news: {company}")
        mid_company = titleize_values(MidMeetingCompany.from_company_dto(company))
    else:
        logger.error("No companies found")
    logger.info(f"Company: {mid_company}")

    mini_participants = []

    for participant in filtered_participants_emails:
        profile = profiles_repository.get_profile_data_by_email(participant)
        if profile:
            profile_response = MiniProfileResponse.from_profile_dto_and_email(profile, participant)
            logger.info(f"Profile: {profile_response}")
            mini_participants.append(profile_response)

    logger.info(f"Meeting participants: {mini_participants}")

    return MiniMeetingOverviewResponse(
        meeting=mini_meeting,
        company=mid_company,
        participants=mini_participants,
    )


@v1_router.get(
    "/{tenant_id}/meeting-overview-mock/{meeting_uuid}",
    response_model=MeetingOverviewResponse,
)
def get_meeting_overview_mock(
    tenant_id: str,
    meeting_uuid: str,
    meetings_repository: MeetingsRepository = Depends(meetings_repository),
    companies_repository: CompaniesRepository = Depends(companies_repository),
    profiles_repository: ProfilesRepository = Depends(profiles_repository),
) -> MeetingOverviewResponse:
    """
    Get the meeting information.

    - **tenant_id**: Tenant ID
    - **meeting_id**: Meeting ID
    """
    meeting_dict = {
        "meeting": {
            "subject": "Quarterly Business Review",
            "video_link": "https://example.com/video",
            "guidelines": {
                "total_duration": "60m",
                "guidelines": [
                    {"text": "Review the financial report", "duration": "15m"},
                    {"text": "Prepare questions for the Q&A session", "duration": "30m"},
                ],
            },
        },
        "company": {
            "name": "Tech Innovators Inc.",
            "overview": "A leading company in tech innovation.",
            "size": "500-1000",
            "industry": "Technology",
            "country": "USA",
            "annual_revenue": "100M-500M",
            "total_funding": "50M",
            "last_raised_at": "2023-06-15",
            "main_costumers": "Fortune 500 companies",
            "main_competitors": "Tech Giants Ltd.",
            "technologies": ["Artificial Intelligence", "Machine Learning", "Cloud Computing"],
            "challenges": [
                {
                    "challenge_name": "Scalability",
                    "reasoning": "The company is expanding rapidly.",
                    "score": 8,
                }
            ],
            "news": [
                {
                    "date": "2024-08-20",
                    "link": "https://example.com/news-article",
                    "media": "TechNews",
                    "title": "Tech Innovators Inc. announces new AI platform",
                    "summary": "The company has launched a new AI platform that will revolutionize the industry.",
                }
            ],
        },
        "participants": [
            {
                "uuid": "123e4567-e89b-12d3-a456-426614174000",
                "name": "John Doe",
                "email": "john.doe@example.com",
                "profile_picture": "https://img.icons8.com/ios-filled/50/user-male-circle.png",
            },
            {
                "uuid": "987e6543-e21b-12d3-a456-426614174001",
                "name": "Jane Smith",
                "email": "jane.smith@example.com",
                "profile_picture": "https://img.icons8.com/ios-filled/50/user-male-circle.png",
            },
        ],
    }
    return MeetingOverviewResponse.from_dict(meeting_dict)


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

    google_credentials = Credentials(
        token=google_credentials["access_token"],
        refresh_token=google_credentials["refresh_token"],
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        token_uri=GOOGLE_TOKEN_URI,
    )

    logger.debug(f"Google credentials before refresh: {google_credentials}")

    access_token = google_credentials.token

    credentials = Credentials(token=access_token)
    service = build("calendar", "v3", credentials=credentials)

    now = datetime.datetime.utcnow().isoformat() + "Z"
    # 'Z' indicates UTC time
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
        if "The credentials do not contain the necessary fields" in error_message:
            logger.error(f"Missing fields in credentials: {e}")
            raise HTTPException(status_code=401, detail="Need to re-login to refresh the access-token")
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
    # for meeting in meetings:
    #     meeting = MeetingDTO.from_google_calendar_event(meeting, tenant_id)
    #     event = GenieEvent(topic=Topic.NEW_MEETING, data=meeting.to_json(), scope="public")
    #     event.send()

    return JSONResponse(content=titleize_values({"events": meetings}))


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
