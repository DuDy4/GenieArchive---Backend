import os

from fastapi import Depends, Request
from fastapi.routing import APIRouter
from loguru import logger
from starlette.responses import PlainTextResponse, RedirectResponse
from starlette_context import context
from app_common.data_transfer_objects.person import PersonDTO
from app_common.repositories.persons_repository import PersonsRepository
from app_common.data_transfer_objects.interaction import InteractionDTO
from app_common.repositories.interactions_repository import InteractionsRepository
from app_common.dependencies.dependencies import (
    persons_repository,
    interactions_repository,
    salesforce_users_repository,
)

from redis import Redis


# from app.app_common.repositories.persons_repository import PersonsRepository
# from app.app_common.dependencies.dependencies import persons_repository

from services.salesforce import (
    get_authorization_url,
    handle_callback,
    create_salesforce_client,
    SalesforceAgent,
)

SELF_URL = os.environ.get("self_url", "https://localhost:8444")
logger.info(f"Self url: {SELF_URL}")

v1_router = APIRouter(prefix="/v1")

redis_client = Redis(host="localhost", port=6379, db=0)


PROFILE_ID = 0


def get_id():
    global PROFILE_ID
    PROFILE_ID += 1
    return PROFILE_ID - 1


@v1_router.get("/profiles/{uuid}", response_model=dict)
def get_profile(
    uuid: str,
):
    """
    Fetches and returns a specific profile.
    """
    logger.info("Got profile request")
    return {
        "id": get_id(),
        "uuid": uuid,
        "name": "Asaf Savich",
        "Company": "DefinitelyNotKubiya.ai",
    }


@v1_router.post("/profiles/", response_model=str)
async def insert_new_profile(request: Request):
    """
    Fetches and returns a specific profile.
    """
    request_body = await request.json()
    logger.info(f"Request body {request_body}")
    uuid = request_body.get("uuid")
    name = request_body.get("name")
    company = request_body.get("company")
    logger.info("Got profile POST request")
    return f"Added new profile: {uuid}: {name} who works at {company}"


@v1_router.delete("/profiles/{uuid}", response_model=str)
def delete_profile(
    uuid: str,
):
    """
    Fetches and returns a specific profile.
    """
    logger.info("Got profile DELETE request")
    return f"Deleted profile: {uuid}"


@v1_router.put("/profiles/{uuid}", response_model=str)
async def update_profile(uuid: str, request: Request):
    """
    Fetches and returns a specific profile.
    """
    request_body = await request.json()
    logger.info(f"Request body {request_body}")
    name = request_body.get("name")
    company = request_body.get("company")
    if not name or not company:
        return "Name and company are required"
    logger.info("Got profile PUT request")
    return f"Updated profile: {uuid}: {name} who works at {company}"


@v1_router.get("/salesforce/auth/{company}", response_class=RedirectResponse)
def oauth_salesforce(request: Request, company: str) -> RedirectResponse:
    """
    Triggers the salesforce oauth2.0 process
    """
    logger.debug(f"request.session before start: {request.session}")
    # request.session.clear()
    # logger.debug(f"St.session before start: {request.session}")
    logger.info(f"Beginning salesforce oauth integration for {company}")
    context["salesforce_company"] = company
    logger.debug(f"Context: {context['salesforce_company']}")
    # logger.info(f"Saved to session: {request.session["salesforce_company"]}")
    authorization_url = get_authorization_url(company) + f"&company={company}"
    logger.info(f"Redirect url: {authorization_url}")
    return RedirectResponse(url=authorization_url)


@v1_router.get("/salesforce/callback", response_class=PlainTextResponse)
def callback_salesforce(
    request: Request,
    # sf_users_repository = Depends(salesforce_users_repository)
) -> PlainTextResponse:
    """
    Triggers the salesforce oauth2.0 callback process
    """
    # logger.debug(f"Request session: {request.session}")
    # logger.info(f"Received callback from salesforce oauth integration. Company: {request.session['salesforce_company']}"
    # )
    company = context.get("salesforce_company")
    logger.info(
        f"Received callback from salesforce oauth integration. Company: {company}"
    )
    token_data = handle_callback(company, str(request.url))

    return PlainTextResponse(
        f"Successfully authenticated with Salesforce for {company}. \nYou can now close this tab"
    )


@v1_router.post("/person", response_class=PlainTextResponse)
async def insert_new_person(
    request: Request, person_repository: PersonsRepository = Depends(persons_repository)
) -> PlainTextResponse:
    """
    Triggers the salesforce oauth2.0 callback process
    """
    logger.info(f"Received person post request")
    request_body = await request.json()
    logger.info(f"Request_body: {request_body}")
    person = PersonDTO.from_dict(request_body["person"])
    logger.info(f"person: {person}")
    try:
        person_id = person_repository.insert_person(person)
    except Exception as e:
        logger.error(f"Failed to insert person: {e}")
        return PlainTextResponse(f"Failed to insert person, because: {e}")
    if person_id:
        logger.info(f"Person was inserted successfully with id: {person_id}")
        return PlainTextResponse(
            f"Person was inserted successfully with id: {person_id}"
        )
    else:
        logger.error(f"Failed to insert person")
        return PlainTextResponse(f"Failed to insert person")


@v1_router.get("/salesforce/{company}/contact", response_class=PlainTextResponse)
async def get_all_contact(
    request: Request,
    company: str,
    sf_users_repository=Depends(salesforce_users_repository),
) -> PlainTextResponse:
    """
    Triggers the salesforce oauth2.0 callback process
    """
    logger.info(f"Received get contacts request")

    refresh_token = sf_users_repository.get_refresh_token(company)
    logger.info(f"refresh_token: {refresh_token}")
    salesforce_client = create_salesforce_client(company, refresh_token)
    logger.info(f"salesforce_client: {salesforce_client}")

    salesforce_agent = SalesforceAgent(salesforce_client, sf_users_repository)
    contacts = await salesforce_agent.get_contacts()
    logger.info(f"contacts: {contacts}")
