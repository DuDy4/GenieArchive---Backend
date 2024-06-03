import json
import sys
import os

import uvicorn
from fastapi import FastAPI, Depends, Request
from fastapi.routing import APIRouter
from fastapi.responses import PlainTextResponse, RedirectResponse

from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ai.langsmith.langsmith_loader import Langsmith
from common.utils.json_utils import json_to_python
from common.events.topics import Topic
from common.events.genie_consumer import GenieConsumer
from common.repositories.personal_data_repository import PersonalDataRepository
from common.dependencies.dependencies import profiles_repository


PERSON_PORT = os.environ.get("PERSON_PORT", 8005)


class Person(GenieConsumer):
    def __init__(self):
        super().__init__(topics=[Topic.NEW_CONTACT])
        self.langsmith = Langsmith()

    async def process_event(self, event):
        print(
            f"Processing event on topic {event.properties.get(b'topic').decode('utf-8')}"
        )
        response = self.langsmith.run_prompt_test(event.body_as_str())
        print(f"Response: {response}")
        return response


v1_router = APIRouter(prefix="/v1")
app = FastAPI()


@v1_router.get("/profile/{uuid}")
async def get_profile(
    request: Request,
    uuid: str,
    profiles_repository: PersonalDataRepository = Depends(profiles_repository),
):
    try:
        profile = profiles_repository.get_personal_data(uuid)
        return profile
    except Exception as e:
        logger.error(f"Failed to get profile: {e}")


@v1_router.post("/profile/")
async def get_profile(
    request: Request,
    profiles_repository: PersonalDataRepository = Depends(profiles_repository),
):
    request_body = await request.json()
    # logger.debug(f"Request body: {request_body}")

    uuid = request_body.get("uuid")
    name = request_body.get("name")
    personal_data = request_body.get("personal_data")
    try:
        profiles_repository.insert(uuid, name, json.dumps(personal_data))
        logger.info("Inserted profile into database")
    except Exception as e:
        logger.error(f"Failed to get profile: {e}")


app.include_router(v1_router)


if __name__ == "__main__":
    person = Person()
    uvicorn.run(
        "person:app",
        host="0.0.0.0",
        port=PERSON_PORT,
        ssl_keyfile="../key.pem",
        ssl_certfile="../cert.pem",
    )
    print("Running person service")
    person.run()
