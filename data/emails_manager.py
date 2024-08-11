import json
import os
import sys
import asyncio

from loguru import logger

from common.utils import env_utils
from data.data_common.dependencies.dependencies import profiles_repository
from data.data_common.repositories.profiles_repository import ProfilesRepository
from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.topics import Topic

CONSUMER_GROUP = "emailmanagerconsumergroup"

APP_URL = env_utils.get("APP_URL", "http://localhost:1234")


class EmailManager(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[Topic.FINISHED_NEW_PROFILE],
            consumer_group=CONSUMER_GROUP,
        )
        self.profiles_repository = profiles_repository()

    async def process_event(self, event):
        logger.info(f"EmailManager processing event: {event}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        # Should use Topic class

        match topic:
            case Topic.FINISHED_NEW_PROFILE:
                logger.info("Handling new salesforce contact")
                await self.handle_finished_new_profile(event)

    async def handle_finished_new_profile(self, event):
        event_body = event.body_as_str()
        profile = ProfileDTO.from_json(json.loads(event_body))
        logger.info(f"Profile: {profile}, type: {type(profile)}")
        self.send_before_the_meeting_link_email(profile)

    def send_before_the_meeting_link_email(self, profile):
        name = profile.name
        name = "-".join(name.split(" "))
        link = f"{APP_URL}/profiles/{name}/before-the-meeting"
        logger.info(f"Link: {link}")
        # Send email


if __name__ == "__main__":
    email_consumer = EmailManager()
    try:
        asyncio.run(email_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
