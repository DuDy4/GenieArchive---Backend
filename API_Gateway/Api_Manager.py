import os
import json

from fastapi import Request
from fastapi.routing import APIRouter
from loguru import logger

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
