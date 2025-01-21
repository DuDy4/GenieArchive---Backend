import asyncio
import json
import os
import sys

from data.data_common.events.genie_event_batch_manager import EventHubBatchManager
from data.data_common.services.artifacts_service import ArtifactSerivce

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.data_transfer_objects.artifact_dto import ArtifactDTO
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.repositories.personal_data_repository import PersonalDataRepository
from data.data_common.dependencies.dependencies import persons_repository, personal_data_repository

from data.data_common.data_transfer_objects.person_dto import PersonDTO

from common.genie_logger import GenieLogger

logger = GenieLogger()

CONSUMER_GROUP = "profile_params_consumer_group"


class ProfileParamsConsumer(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[
                Topic.PERSONAL_NEWS_ARE_UP_TO_DATE,
                Topic.NEW_PERSON_ARTIFACT,
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.persons_repository: PersonsRepository = persons_repository()
        self.artifacts_service: ArtifactSerivce = ArtifactSerivce()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        match topic:
            case Topic.PERSONAL_NEWS_ARE_UP_TO_DATE:
                logger.info("Handling new profiling")
                await self.begin_profiling(event)
            case Topic.NEW_PERSON_ARTIFACT:
                logger.info("Handling new artifact")
                await self.handle_new_artifact(event)
            case _:
                logger.error(f"Should not have reached here: {topic}, consumer_group: {CONSUMER_GROUP}")

    async def begin_profiling(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person_uuid = event_body.get("person_uuid") if event_body.get("person_uuid") else event_body.get("person_id")
        if not person_uuid:
            logger.error(f"No person data found for person {person_uuid}")
            raise Exception("Got event with uuid but no person data")
        person = self.persons_repository.get_person(person_uuid)
        if not person:
            logger.error(f"No person found for person {person_uuid}")
            raise Exception("No person found for person_uuid")
        artifacts = self.artifacts_service.get_self_written_posts(person_uuid, person.linkedin_url)
        for artifact in artifacts:
            event_batch = EventHubBatchManager()
            event = GenieEvent(
                topic=Topic.NEW_PERSON_ARTIFACT,
                data={"profile_uuid": person_uuid, "artifact" : artifact.to_dict()},
            )
            event_batch.queue_event(event)

        await event_batch.send_batch()
        return {"status": "success"}

        
    async def handle_new_artifact(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        artifact = ArtifactDTO.from_dict(event_body.get("artifact"))
        profile_uuid = event_body.get("profile_uuid")
        person = self.persons_repository.get_person(profile_uuid)
        await self.artifacts_service.calculate_artifact_scores(artifact, person)
        event = GenieEvent(
            topic=Topic.ARTIFACT_SCORES_CALCULATED,
            data={"profile_uuid": profile_uuid, "artifact_uuid": artifact.uuid},
        )
        return {"status": "success"}
    
    async def artifact_calculated(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        artifact_uuid = event_body.get("artifact_uuid")
        profile_uuid = event_body.get("profile_uuid")
        artifact = self.artifacts_service.get_artifact(artifact_uuid)
        if not artifact:
            logger.error(f"No artifact found for artifact {artifact_uuid}")
            raise Exception("No artifact found for artifact_uuid")
        person = self.persons_repository.get_person(profile_uuid)
        if not person:
            logger.error(f"No person found for person {profile_uuid}")
            raise Exception("No person found for profile_uuid")
        await self.artifacts_service.calculate_artifact_scores(artifact, person)
        return {"status": "success"}
        


if __name__ == "__main__":
    profile_params_consumer = ProfileParamsConsumer()
    try:
        asyncio.run(profile_params_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
