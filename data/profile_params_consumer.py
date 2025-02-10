import asyncio
import datetime
import json
import os
import sys
from pydantic import ValidationError

from ai.langsmith.langsmith_loader import Langsmith
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.data_transfer_objects.work_history_dto import WorkHistoryArtifactDTO
from data.data_common.events.genie_event_batch_manager import EventHubBatchManager
from data.data_common.repositories.personal_data_repository import PersonalDataRepository
from data.data_common.services.artifacts_service import ArtifactsService

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.data_transfer_objects.artifact_dto import ArtifactDTO
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

from data.data_common.repositories.persons_repository import PersonsRepository


from common.genie_logger import GenieLogger

logger = GenieLogger()

CONSUMER_GROUP = "profile_params_consumer_group"


class ProfileParamsConsumer(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[
                Topic.NEW_PERSONAL_NEWS,
                Topic.NEW_PERSONAL_DATA,
                Topic.NEW_PERSON_ARTIFACT,
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.persons_repository: PersonsRepository = PersonsRepository()
        self.personal_data_repository: PersonalDataRepository = PersonalDataRepository()
        self.artifacts_service: ArtifactsService = ArtifactsService()
        self.langsmith = Langsmith()


    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        match topic:
            case Topic.NEW_PERSONAL_NEWS:
                logger.info("Handling new profiling")
                await self.begin_profiling(event)
            case Topic.NEW_PERSON_ARTIFACT:
                logger.info("Handling new artifact")
                await self.handle_new_artifact(event)
            case Topic.NEW_PERSONAL_DATA:
                logger.info("Handling new personal data")
                await self.handle_work_history(event)
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
        artifacts = self.artifacts_service.get_self_written_posts(person_uuid, person.linkedin)
        if artifacts:
            event_batch = EventHubBatchManager()
            for artifact in artifacts:
                if self.artifacts_service.check_existing_artifact(artifact):
                    logger.info(f"Artifact {artifact.uuid} already exists")
                    continue
                event = GenieEvent(
                    topic=Topic.NEW_PERSON_ARTIFACT,
                    data={"profile_uuid": person_uuid, "artifact" : artifact.to_dict()},
                )
                event_batch.queue_event(event)

            await event_batch.send_batch()
        else:
            logger.info(f"No artifacts found for person {person_uuid}")
        return {"status": "success"}

        
    async def handle_new_artifact(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        artifact_dict = event_body.get("artifact")
        if not artifact_dict:
            logger.error(f"No artifact data found for event {event_body}")
            raise Exception("No artifact data found")
        artifact = ArtifactDTO.from_dict(artifact_dict)
        profile_uuid = event_body.get("profile_uuid")
        person = self.persons_repository.get_person(profile_uuid)
        calculate_task = asyncio.create_task(self.artifacts_service.calculate_artifact_scores(artifact, person))
        await calculate_task
        event = GenieEvent(
            topic=Topic.ARTIFACT_SCORES_CALCULATED,
            data={"profile_uuid": profile_uuid, "artifact_uuid": artifact.uuid},
        )
        event.send()
        return {"status": "success"}
    
    
    async def artifact_calculated(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
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
        await self.artifacts_service.calculate_overall_params(person.email, profile_uuid)
        return {"status": "success"}
    

    async def calculate_overall_params(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        profile_uuid = event_body.get("profile_uuid")
        person = self.persons_repository.get_person(profile_uuid)
        if not person:
            logger.error(f"No person found for person {profile_uuid}")
            raise Exception("No person found for profile_uuid")
        param_averages = self.artifacts_service.calculate_overall_params(person.email, profile_uuid)
        logger.info(f"Calculated overall params for profile {person.email}: {param_averages}")
        return {"status": "success"}

    async def handle_work_history(self, event):
        event_body = event.body_as_str()
        logger.info(f"Event body: {str(event_body)[:300]}")
        event_body = json.loads(event_body)
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person = event_body.get("person")
        if not person:
            logger.error(f"No person data found for person {person}")
            raise Exception("No person data found")
        person = PersonDTO.from_dict(person)
        personal_data = event_body.get("personal_data")
        profile_uuid = person.uuid
        if not profile_uuid:
            logger.error(f"No person data found for person {profile_uuid}")
            raise Exception("No person data found")
        if not personal_data:
            personal_data = self.personal_data_repository.get_pdl_personal_data(profile_uuid)
            if not personal_data:
                personal_data = self.personal_data_repository.get_apollo_personal_data(profile_uuid)
                if not personal_data:
                    logger.error(f"No personal data found for person {profile_uuid}")
                    raise Exception("No personal data found")
        if not person:
            logger.error(f"No person found for person {profile_uuid}")
            raise Exception("No person found for profile_uuid")
        work_history = personal_data.get("experience") if personal_data.get("experience") else personal_data.get("employment_history")
        if not work_history:
            logger.error(f"No work history found for person {person.email}")
            raise Exception("No work history found")
        work_history_tasks = []
        for work_history_element in work_history:
            work_history_artifact = None
            try:
                work_history_artifact = WorkHistoryArtifactDTO.from_pdl_element(work_history_element, profile_uuid, personal_data.get("linkedin_url"))
            except ValidationError as error:
                logger.error(f"Error creating work history artifact: {error}")
                try:
                    work_history_artifact = WorkHistoryArtifactDTO.from_apollo_element(work_history_element, profile_uuid, personal_data.get("linkedin_url"))
                except ValidationError as error:
                    logger.error(f"Error creating work history artifact: {error}")
                    continue
            finally:
                if not work_history_artifact:
                    continue
            if self.artifacts_service.exists_work_history_element(work_history_artifact):
                logger.info(f"Work history element {work_history_artifact.uuid} already exists")
                continue
            if not work_history_artifact.text:
                work_history_artifact.text = await self.langsmith.get_work_history_post(work_history_artifact.to_dict())
            self.artifacts_service.artifacts_repository.save_artifact(work_history_artifact)
            calculate_task = asyncio.create_task(self.artifacts_service.calculate_artifact_scores(work_history_artifact, person, isWorkHistory=False))
            work_history_tasks.append({"task": calculate_task, "artifact": work_history_artifact})
        tasks = [task.get("task") for task in work_history_tasks]
        artifacts = [task.get("artifact") for task in work_history_tasks]
        await asyncio.gather(*tasks)
        for artifact in artifacts:
            event = GenieEvent(
                topic=Topic.ARTIFACT_SCORES_CALCULATED,
                data={"profile_uuid": profile_uuid, "artifact_uuid": artifact.uuid},
            )
            event.send()
        return {"status": "success"}


if __name__ == "__main__":
    consumer = ProfileParamsConsumer()
    try:
        asyncio.run(consumer.main())  # ✅ Use GenieConsumer’s event loop management
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, shutting down...")
        asyncio.run(consumer.stop())  # ✅ Graceful shutdown