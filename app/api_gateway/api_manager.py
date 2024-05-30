import os

from fastapi import Depends, Request
from fastapi.routing import APIRouter
from loguru import logger
from starlette.responses import PlainTextResponse, RedirectResponse
from app_common.data_transfer_objects.person import PersonDTO
from app_common.repositories.persons_repository import PersonsRepository
from app_common.data_transfer_objects.interaction import InteractionDTO
from app_common.repositories.interactions_repository import InteractionsRepository
from app_common.dependencies.dependencies import (
    persons_repository,
    interactions_repository,
)


# from app.app_common.repositories.persons_repository import PersonsRepository
# from app.app_common.dependencies.dependencies import persons_repository

from services.salesforce import get_authorization_url, handle_callback

SELF_URL = os.environ.get("self_url", "https://localhost:8444")
logger.info(f"Self url: {SELF_URL}")

v1_router = APIRouter(prefix="/v1")

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


@v1_router.get("/auth/salesforce/{company}", response_class=RedirectResponse)
def oauth_salesforce(request: Request, company: str) -> RedirectResponse:
    """
    Triggers the salesforce oauth2.0 process
    """
    logger.info(f"Beginning salesforce oauth integration for {company}")
    request.session["salesforce_company"] = company
    result = get_authorization_url(company)
    logger.info(f"Redirect url: {result}")
    return RedirectResponse(url=result)


@v1_router.get("/callback/salesforce", response_class=PlainTextResponse)
def callback_salesforce(
    request: Request,
) -> PlainTextResponse:
    """
    Triggers the salesforce oauth2.0 callback process
    """
    logger.info(
        f"Received callback from salesforce oauth integration. Company: {request.session['salesforce_company']}"
    )
    handle_callback(request.session["salesforce_company"], str(request.url))
    return PlainTextResponse(
        f"Successfully authenticated with Salesforce for {request.session['salesforce_company']}. \nYou can now close this tab"
    )


@v1_router.post("/person", response_class=PlainTextResponse)
async def insert_new_person(
    request: Request, person_repository: PersonsRepository = Depends(persons_repository)
) -> PlainTextResponse:
    """
    Insert a new person to database
    """
    logger.info(f"Received person post request")
    request_body = await request.json()
    logger.info(f"Request_body: {request_body}")
    person = PersonDTO.from_dict(request_body["person"])
    logger.info(f"person: {person}")

    person_id = person_repository.insert_person(person)
    if person_id:
        logger.info(f"Person was inserted successfully with id: {person_id}")
        return PlainTextResponse(
            f"Person was inserted successfully with id: {person_id}"
        )
    else:
        logger.error(f"Failed to insert person")
        return PlainTextResponse(f"Failed to insert person")


@v1_router.post("/interaction", response_class=PlainTextResponse)
async def insert_new_interaction(
    request: Request,
    interactions_repository: InteractionsRepository = Depends(interactions_repository),
) -> PlainTextResponse:
    """
    Insert a new interaction to database
    """
    logger.info(f"Received interaction post request")
    request_body = await request.json()
    logger.info(f"Request_body: {request_body}")
    interaction = InteractionDTO.from_dict(request_body["interaction"])
    logger.info(f"Interaction: {interaction}")

    interaction_id = interactions_repository.insert(interaction)
    if interaction_id:
        logger.info(f"Interaction was inserted successfully with id: {interaction_id}")
        return PlainTextResponse(
            f"Interaction was inserted successfully with id: {interaction_id}"
        )
    else:
        logger.error(f"Failed to insert interaction")
        return PlainTextResponse(f"Failed to insert interaction")
