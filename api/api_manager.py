import os
import secrets


from fastapi import Depends, Request
from fastapi.routing import APIRouter
from loguru import logger
from starlette.responses import PlainTextResponse, RedirectResponse
from common.repositories.profiles_repository import ProfilesRepository
from common.dependencies.dependencies import profiles_repository
from common.events.topics import Topic

from redis import Redis

SELF_URL = os.environ.get("PERSON_URL", "https://localhost:8000")
logger.info(f"Self url: {SELF_URL}")

v1_router = APIRouter(prefix="/v1")

redis_client = Redis(host="localhost", port=6379, db=0)


@v1_router.get("/profiles/{uuid}", response_model=dict)
def get_profile(
    uuid: str,
    profiles_repository: ProfilesRepository = Depends(profiles_repository),
):
    """
    Fetches and returns a specific profile.
    """
    logger.info("Got profile request")
    profile = profiles_repository.get_profile_data(uuid)
    logger.info(f"Got profile: {profile}")
    if profile:
        return profile.to_dict()
    else:
        return {"error": "Profile not found"}


@v1_router.get("/topics", response_model=dict)
def get_all_topics():
    """
    Fetches and returns a specific profile.
    """
    logger.info("Got topic request")
    return Topic
