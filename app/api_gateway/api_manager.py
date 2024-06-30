import json
import os
import secrets
import traceback

from fastapi import Depends, Request, HTTPException
from fastapi.routing import APIRouter
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from loguru import logger
from starlette.responses import PlainTextResponse, RedirectResponse, JSONResponse

# from app_common.utils.api_classes import GoogleSearchAPI, ProfilePicture

#
# profile_picture = ProfilePicture(
#     os.environ.get("GOOGLE_DEVELOPER_API_KEY"),
#     os.environ.get("GOOGLE_CX")
# )

from redis import Redis

from app_common.repositories.tenants_repository import TenantsRepository
from app_common.dependencies.dependencies import tenants_repository
from app_common.utils.salesforce_functions import handle_new_contacts_event

from services.sheet import (
    get_sheet_records,
    create_vc_draft_email,
    get_strengths,
    update_sheet,
    get_strengths_chart,
    upload_to_s3,
    extract_first_json,
    get_vc_member_data,
    fetch_hobbies_images,
    get_hobbies,
    get_news,
)

SELF_URL = os.environ.get("self_url", "https://localhost:3000")
PERSON_URL = os.environ.get("PERSON_URL", "https://localhost:8000")
logger.info(f"Self url: {SELF_URL}")

v1_router = APIRouter(prefix="/v1")

redis_client = Redis(host="localhost", port=6379, db=0)

PROFILE_ID = 0


@v1_router.get("/test-cors")
async def test_cors():
    return {"message": "CORS working"}


@v1_router.post("/signup", response_model=dict)
async def signup(
    request: Request,
    tenants_repository: TenantsRepository = Depends(tenants_repository),
):
    """
    Creates a new user account.
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

        # salesforce_creds = tenants_repository.has_salesforce_creds(data.get("tenantId"))
        salesforce_creds = tenants_repository.get_salesforce_credentials(
            data.get("tenantId")
        )
        logger.debug(f"Salesforce creds: {salesforce_creds}")
        # Add your business logic here
        return {
            "message": f"User account created successfully with uuid: {uuid}",
            "salesforce_creds": salesforce_creds,
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


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


@v1_router.get("/salesforce/auth/{tenantId}", response_class=RedirectResponse)
def oauth_salesforce(tenantId: str) -> RedirectResponse:
    """
    Triggers the salesforce oauth2.0 process
    """
    # Define the number of retries and backoff factor
    retries = Retry(total=5, backoff_factor=1)

    # Create a session
    session = requests.Session()

    # Mount the adapter to handle retries
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Disable SSL verification
    session.verify = False
    response = session.get(
        PERSON_URL + f"/v1/salesforce/auth/{tenantId}", allow_redirects=False
    )
    logger.info(f"Response: {response}")
    logger.info(f"Response status code: {response.status_code}")
    logger.info(f"Response headers: {response.headers}")

    if response.status_code == 307:  # 307 Temporary Redirect
        redirect_url = response.headers.get("Location")
        if redirect_url:
            logger.info(f"Redirecting to: {redirect_url}")
            return RedirectResponse(url=redirect_url)

    logger.error(f"Unexpected response status: {response.status_code}")
    return RedirectResponse(
        url="/error"
    )  # Redirect to an error page or handle accordingly


@v1_router.post("/salesforce/callback", response_class=PlainTextResponse)
async def callback_salesforce(
    request: Request,
    tenants_repository: TenantsRepository = Depends(tenants_repository),
) -> PlainTextResponse:
    """
    Triggers the salesforce oauth2.0 callback process
    """
    # logger.debug(f"Request session: {request.session}")
    # logger.info(f"Received callback from salesforce oauth integration. Company: {request.session['salesforce_company']}"
    # )

    logger.info(f"Received callback from salesforce oauth integration.")
    logger.debug(f"Request: {request}")

    tenant_id = request.query_params.get("state")

    data = await request.json()

    logger.debug(f"Tenant ID: {tenant_id}")

    tenants_repository.update_salesforce_credentials(tenant_id, data)

    return PlainTextResponse(
        f"Successfully authenticated with salesforce for {tenant_id}. \nYou can now close this tab"
    )


@v1_router.get("/salesforce/contacts/{state}", response_class=JSONResponse)
async def get_contacts(
    state: str,
    tenants_repository: TenantsRepository = Depends(tenants_repository),
):
    """
    Fetches and returns contacts from Salesforce.
    """
    tenant_id = state
    logger.info(f"Fetching contacts for tenant: {tenant_id}")

    # Define the number of retries and backoff factor
    retries = Retry(total=5, backoff_factor=1)

    # Create a session
    session = requests.Session()

    # Mount the adapter to handle retries
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Disable SSL verification
    session.verify = False

    response = session.get(
        PERSON_URL + f"/v1/salesforce/contacts/{tenant_id}", allow_redirects=False
    )

    logger.debug(f"Response: {response.json()}")

    # Check if the request was successful
    if response.status_code == 200:
        # Return the response JSON
        return response.json()
    else:
        # Raise an HTTPException if the request was not successful
        raise HTTPException(status_code=response.status_code, detail=response.text)


@v1_router.post("/salesforce/build-profiles/{state}", response_class=JSONResponse)
async def process_profiles(
    request: Request,
    state: str,
):
    """
    Fetches and returns contacts from Salesforce.
    """
    tenant_id = state
    logger.info(f"Fetching contacts for tenant: {tenant_id}")

    contacts = await request.json()

    result = handle_new_contacts_event(contacts)

    logger.debug(f"Result: {result}")
    if result:
        return result
    else:
        # Raise an HTTPException if the request was not successful
        raise {"message": "Failed to process contacts"}


#
# @v1_router.get("/create-vc-mail", response_class=PlainTextResponse)
# def create_vc_mail(
#         id: int = -1,
#         name: str = None
# ) -> RedirectResponse:
#     """
#     Creates a personal mail for a specific vc member based on the VC google sheet
#     """
#     if name:
#         vc_member_data, id = get_vc_member_data(name)
#         if not vc_member_data:
#             return JSONResponse({"error": "VC member not found"})
#     elif id > 0:
#         vc_member_data = get_sheet_records(id)
#     print(vc_member_data)
#
#     # Hobbies
#     hobbies = get_hobbies(vc_member_data)
#     hobbies = extract_first_json(hobbies.replace('\n', ' '))
#     hobbies = hobbies['hobbies']
#     hobby_data = fetch_hobbies_images(hobbies)
#
#     # News
#     news = get_news(vc_member_data)
#     news = extract_first_json(news.replace('\n', ' '))
#     news = news['news_list']
#
#     # Image
#     linkedin_url = vc_member_data['Personal LinkedIn']
#     image_link = None
#     if linkedin_url:
#         linkedin_id = fix_linkedin_url(linkedin_url)
#         res = profile_picture.search(linkedin_id)
#         image_link = extract_profile_picture(res._search_results)
#
#
#     name_and_vc = vc_member_data['Full name'] + "(" + vc_member_data['VC name'] + ")"
#     logger.info(f"Found VC Member: {name_and_vc}")
#     strengths = get_strengths(vc_member_data)
#     strengths = strengths.replace('\n', ' ')
#     strengths = strengths.replace("\'", "`")
#     strengths_score = extract_first_json(strengths)
#     logger.info(f"Strengths for {name_and_vc}: {strengths_score}")
#     escaped_name = vc_member_data['Full name'].replace(" ","_").replace("\n","")
#     spider_chart = get_strengths_chart(strengths_score, escaped_name)
#     logger.info(f"Strengths chart for {name_and_vc}: {spider_chart}")
#     chart_url = upload_to_s3(spider_chart)
#     mail_draft = create_vc_draft_email(vc_member_data, strengths_score)
#     logger.info(f"Draft for {name_and_vc}: {mail_draft}")
#     update_sheet(id, mail_draft, str(strengths_score), chart_url, hobby_data, news, image_link)
#     return PlainTextResponse(mail_draft)
#


@v1_router.get("/vc-profile")
def get_vc_profile(id: int = -1, name: str = None):
    # vc_member_data = get_sheet_records(id)
    if name:
        vc_member_data, id = get_vc_member_data(name)
        if not vc_member_data:
            return JSONResponse({"error": "VC member not found"})
    elif id > 0:
        vc_member_data = get_sheet_records(id)

    vc_member_data = remove_newlines_from_dict(vc_member_data)
    print(vc_member_data)
    name_and_vc = vc_member_data["Full name"] + "(" + vc_member_data["VC name"] + ")"
    logger.info(f"Found VC Member: {name_and_vc}")
    # strengths = vc_member_data['Stregnths (Auto-Generated)'].replace("'", "\"")
    # strengths = strengths.replace("`", "'")
    # strengths_json = json.loads(strengths)
    # vc_member_data['Strengths'] = strengths_json
    vc_member_data["Strengths"] = transform_to_json(
        vc_member_data["Stregnths (Auto-Generated)"]
    )
    if vc_member_data["Hobby URLs"]:
        vc_member_data["Hobbies Data"] = transform_to_json(vc_member_data["Hobby URLs"])
    else:
        vc_member_data["Hobbies Data"] = []
    if vc_member_data["News Data"]:
        vc_member_data["News Data"] = transform_to_json(vc_member_data["News Data"])
    else:
        vc_member_data["News Data"] = []
    if vc_member_data["Connections"]:
        vc_member_data["Connections"] = transform_to_json(vc_member_data["Connections"])
    return vc_member_data


def remove_newlines_from_dict(input_dict):
    return {
        key: value.replace("\n", "") if isinstance(value, str) else value
        for key, value in input_dict.items()
    }


def transform_to_json(data):
    data = data.replace("'", '"').replace("`", "'")
    return json.loads(data)


def fix_linkedin_url(linkedin_url: str) -> str:
    """
    Converts a full LinkedIn URL to a shortened URL.

    Args:
        linkedin_url (str): The full LinkedIn URL.

    Returns:
        str: The shortened URL.
    """
    linkedin_url = linkedin_url.replace(
        "http://www.linkedin.com/in/", "linkedin.com/in/"
    )
    linkedin_url = linkedin_url.replace(
        "https://www.linkedin.com/in/", "linkedin.com/in/"
    )
    linkedin_url = linkedin_url.replace("http://linkedin.com/in/", "linkedin.com/in/")
    linkedin_url = linkedin_url.replace("https://linkedin.com/in/", "linkedin.com/in/")

    if linkedin_url[-1] == "/":
        linkedin_url = linkedin_url[:-1:]
    return linkedin_url
