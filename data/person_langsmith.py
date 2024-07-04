import json
import sys
import os

from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ai.langsmith.langsmith_loader import Langsmith
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.events.genie_consumer import GenieConsumer


PERSON_PORT = os.environ.get("PERSON_PORT", 8005)


class LangsmithConsumer(GenieConsumer):
    def __init__(self):
        super().__init__(
            topics=[Topic.NEW_PERSONAL_DATA],
            consumer_group="langsmithconsumergroup_dan",
        )
        self.langsmith = Langsmith()

    async def process_event(self, event):
        logger.info(f"Person processing event: {event}")
        logger.info(
            f"Processing event on topic {event.properties.get(b'topic').decode('utf-8')}"
        )
        event_body = event.body_as_str()
        logger.info(f"Event body: {event_body}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        personal_data = event_body.get("personal_data")
        response = self.langsmith.run_prompt_test(str(personal_data))
        logger.info(f"Response: {response}")
        person = event_body.get("person")
        logger.debug(f"Person: {person}")

        data_to_send = {"person": person, "profile": response}

        event = GenieEvent(Topic.NEW_PROCESSED_PROFILE, data_to_send, "public")
        event.send()
        return data_to_send


#
# v1_router = APIRouter(prefix="/v1")
# app = FastAPI()

#
# @v1_router.get("/profile/{uuid}")
# async def get_profile(
#     request: Request,
#     uuid: str,
#     profiles_repository: PersonalDataRepository = Depends(profiles_repository),
# ):
#     try:
#         profile = profiles_repository.get_personal_data(uuid)
#         return profile
#     except Exception as e:
#         logger.error(f"Failed to get profile: {e}")
#
#
# @v1_router.post("/profile/")
# async def get_profile(
#     request: Request,
#     profiles_repository: PersonalDataRepository = Depends(profiles_repository),
# ):
#     request_body = await request.json()
#     # logger.debug(f"Request body: {request_body}")
#
#     uuid = request_body.get("uuid")
#     name = request_body.get("name")
#     personal_data = request_body.get("personal_data")
#     try:
#         profiles_repository.insert(uuid, name, json.dumps(personal_data))
#         logger.info("Inserted profile into database")
#     except Exception as e:
#         logger.error(f"Failed to get profile: {e}")
#
#
# app.include_router(v1_router)


if __name__ == "__main__":
    langsmith_consumer = LangsmithConsumer()
    langsmith_consumer.run()
