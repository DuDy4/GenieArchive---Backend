import asyncio
from datetime import datetime
import json
import os
import sys

from data.data_common.events.genie_event_batch_manager import EventHubBatchManager
from data.data_common.services.artifacts_service import ArtifactsService
from pydantic import HttpUrl
from pydantic_core import Url

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.data_transfer_objects.artifact_dto import ArtifactDTO, ArtifactSource, ArtifactType
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
                Topic.NEW_PERSONAL_NEWS,
                Topic.NEW_PERSON_ARTIFACT,
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.persons_repository: PersonsRepository = persons_repository()
        self.artifacts_service: ArtifactsService = ArtifactsService()

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
        artifact = ArtifactDTO.from_dict(event_body.get("artifact"))
        profile_uuid = event_body.get("profile_uuid")
        person = self.persons_repository.get_person(profile_uuid)
        await self.artifacts_service.calculate_artifact_scores(artifact, person)
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
        self.artifacts_service.calculate_overall_params(person.email, profile_uuid)
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
        self.artifacts_service.calculate_overall_params(person.email, profile_uuid)
        return {"status": "success"}
        


if __name__ == "__main__":
    profile_params_consumer = ProfileParamsConsumer()
    try:
        # profile_uuid = "00a64c11-da7d-45dc-bde5-dd6e30e5f0d2"
        # artifact_uuid = "0aa0de26-7af6-482b-8432-0734b2751b25"
        # logger.set_tenant_id('org_RPLWQRTI8t7EWU1L')
        # logger.set_user_id('google-oauth2|102736324632194671211')
        # # artifact = ArtifactDTO(uuid='0aa0de26-7af6-482b-8432-0734b2751b25', artifact_type=ArtifactType.POST, source=ArtifactSource.LINKEDIN, profile_uuid='00a64c11-da7d-45dc-bde5-dd6e30e5f0d2', artifact_url=HttpUrl('https://www.linkedin.com/feed/update/urn:li:activity:7073924589675757569/'), text='⭐️Microsoft for Startups - All Founders 2023⭐️\n                  📍June 25th - Save the Date! \n\n💡All Founders by Microsoft for Startups, Israel’s largest founders’ gathering was created to inspire and educate entrepreneurs. \n💡The event focuses on the essential role of the founder, whether they are a first-time founder, serial entrepreneur, or a technical founder. \n💡This event will feature the biggest names in the industry and celebrate our ability as an ecosystem to empower one another to achieve more! 👩🏼\u200d🎓🧕🏻🧑🏻\u200d💼👩🏽\u200d🏭\n\nSpeakers include:\nHans Yang Annie Pearl @Sarah Bird Michal Braverman-Blumenstyk Tomer Simon, PhD Roee Adler  Eyal Brill  Shimon Tolts  @Einat Orr Ron Reiter  Sivan Shamri Dahan  Dahan Yorai Fainmesser Amiram Shachar Gili Raanan  Gadi Evron \nRaz Bachar Meital Shamia  Adir Ron Nitzan Gal Yoav Shlesinger\n\n #startups #entrepreneurs #founders #microsoft #event', summary='⭐️Microsoft for Startups - All Founders 2023⭐️\n                  📍June 25th - Save the Date! \n\n💡All ', published_date=datetime(2023, 6, 12, 0, 0), created_at=datetime(2025, 1, 23, 13, 44, 32, 444295), metadata={'date': '2023-06-12', 'link': 'https://www.linkedin.com/feed/update/urn:li:activity:7073924589675757569/', 'text': '⭐️Microsoft for Startups - All Founders 2023⭐️\n                  📍June 25th - Save the Date! \n\n💡All Founders by Microsoft for Startups, Israel’s largest founders’ gathering was created to inspire and educate entrepreneurs. \n💡The event focuses on the essential role of the founder, whether they are a first-time founder, serial entrepreneur, or a technical founder. \n💡This event will feature the biggest names in the industry and celebrate our ability as an ecosystem to empower one another to achieve more! 👩🏼\u200d🎓🧕🏻🧑🏻\u200d💼👩🏽\u200d🏭\n\nSpeakers include:\nHans Yang Annie Pearl @Sarah Bird Michal Braverman-Blumenstyk Tomer Simon, PhD Roee Adler  Eyal Brill  Shimon Tolts  @Einat Orr Ron Reiter  Sivan Shamri Dahan  Dahan Yorai Fainmesser Amiram Shachar Gili Raanan  Gadi Evron \nRaz Bachar Meital Shamia  Adir Ron Nitzan Gal Yoav Shlesinger\n\n #startups #entrepreneurs #founders #microsoft #event', 'likes': 69, 'media': 'LinkedIn', 'title': '⭐️Microsoft for Startups - All Founders 2023⭐️\n                  📍June 25th - Save the Date! \n\n💡All ', 'images': ['https://media.licdn.com/dms/image/v2/D4D22AQH_o6gWx_SOIA/feedshare-shrink_2048_1536/feedshare-shrink_2048_1536/0/1686555048588?e=1740614400&v=beta&t=cIlD-q5N-Z-lo2VOZ-e2FpV_xJrASXvBWRGk20Cf3YM'], 'summary': None, 'reshared': 'https://www.linkedin.com/in/amit7200'})
        # event = GenieEvent(
        #     topic=Topic.ARTIFACT_SCORES_CALCULATED,
        #     data={"profile_uuid": profile_uuid, "artifact_uuid": artifact_uuid},
        # )
        # event = event.prepare_event()
        # asyncio.run(profile_params_consumer.calculate_overall_params(event))
        asyncio.run(profile_params_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
