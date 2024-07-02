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


@v1_router.get("/profile/{uuid}", response_model=dict)
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
    response = session.get(PERSON_URL + f"/v1/profile/{uuid}")

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


@v1_router.get("/salesforce/callback", response_class=PlainTextResponse)
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
    logger.debug(f"Request_url: {request.url}")

    tenant_id = request.query_params.get("state")

    logger.debug(f"Tenant ID: {tenant_id}")

    url = str(request.url)
    retries = Retry(total=5, backoff_factor=1)

    # Create a session
    session = requests.Session()

    # Mount the adapter to handle retries
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Disable SSL verification
    session.verify = False
    response = session.get(
        PERSON_URL + f"/v1/salesforce/callback?state={tenant_id}&url={url}",
        allow_redirects=False,
    )

    logger.debug(f"Response: {response}")

    data = response.json()

    logger.debug(f"Data: {data}")

    tenants_repository.update_salesforce_credentials(tenant_id, data)

    return PlainTextResponse(
        f"Successfully authenticated with salesforce for {tenant_id}. \nYou can now close this tab"
    )


@v1_router.delete("/salesforce/{state}", response_class=JSONResponse)
async def delete_salesforce_creds(
    state: str,
    tenants_repository: TenantsRepository = Depends(tenants_repository),
):
    """
    Fetches and returns contacts from Salesforce.
    """
    tenant_id = state
    logger.debug(f"Received delete credentials request for tenant: {tenant_id}")
    logger.info(f"about to delete credentials for tenant: {tenant_id}")

    tenants_repository.delete_salesforce_credentials(tenant_id)

    result = tenants_repository.get_salesforce_credentials(tenant_id)
    logger.debug(f"Result: {result}")

    response = send_request_to_person_api(
        "DELETE", PERSON_URL + f"/v1/salesforce/{tenant_id}"
    )

    logger.debug(f"Response: {response.json()}")
    if not result:
        return {"message": "Successfully deleted credentials"}
    else:
        return {"message": "Failed to delete credentials"}


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

    logger.debug(f"Fetch {len(response.json())} contacts")

    # Check if the request was successful
    if response.status_code == 200:
        # Return the response JSON
        return response.json()
    else:
        # Raise an HTTPException if the request was not successful
        raise HTTPException(status_code=response.status_code, detail=response.text)


@v1_router.post("/salesforce/build-profiles/{state}", response_class=PlainTextResponse)
async def process_profiles(
    request: Request,
    state: str,
):
    """
    Fetches and returns contacts from Salesforce.
    """
    tenant_id = state
    logger.info(f"Fetching contacts for tenant: {tenant_id}")

    contact_ids = await request.json()

    logger.debug(f"Contact IDs: {contact_ids}")

    response = send_request_to_person_api(
        "POST", PERSON_URL + f"/v1/salesforce/handle-contacts/{tenant_id}", contact_ids
    )

    result = response.text
    logger.debug(f"Result: {result}")
    if result:
        return result
    else:
        # Raise an HTTPException if the request was not successful
        raise {"message": "Failed to process contacts"}
    # return PlainTextResponse("Success")


@v1_router.get("/profiles/{tenant_id}", response_class=JSONResponse)
async def get_all_profiles(
    request: Request,
    tenant_id: str,
    tenants_repository: TenantsRepository = Depends(tenants_repository),
):
    """
    Fetches and returns all profiles for a specific tenant.
    """
    logger.info(f"Received get profiles request")

    response = send_request_to_person_api(
        "GET", PERSON_URL + f"/v1/profiles/{tenant_id}", {}
    )

    profiles = response.json()

    return JSONResponse(content=profiles)


def send_request_to_person_api(request_type: str, url: str, data: json = None):
    retries = Retry(total=5, backoff_factor=1)
    # Create a session
    session = requests.Session()
    # Mount the adapter to handle retries
    session.mount("https://", HTTPAdapter(max_retries=retries))
    # Disable SSL verification
    session.verify = False
    if request_type == "GET":
        response = session.get(url, allow_redirects=False)
        return response
    elif request_type == "POST":
        response = session.post(url, json=data, allow_redirects=False)
        return response
    elif request_type == "DELETE":
        response = session.delete(url, allow_redirects=False)
        return response
    return {"message": "Invalid request type"}
