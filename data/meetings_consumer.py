import asyncio
import json
import os
from typing import List

from loguru import logger

from common.utils import env_utils
from data.api.base_models import ParticipantEmail
from data.data_common.dependencies.dependencies import meetings_repository
from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

import requests

CONSUMER_GROUP = "meeting_manager_consumer_group"

APP_URL = env_utils.get("APP_URL", "http://localhost:1234")


async def fetch_public_domains():
    response = requests.get(
        "https://gist.githubusercontent.com/ammarshah/f5c2624d767f91a7cbdc4e54db8dd0bf/raw/660fd949eba09c0b86574d9d3aa0f2137161fc7c/all_email_provider_domains.txt"
    )

    domain_list = response.text.split("\n")
    domain_dict = {}
    for domain in domain_list:
        domain_dict[domain] = True
    # logger.info(domain_dict)
    return domain_dict


PUBLIC_DOMAIN = asyncio.run(fetch_public_domains())


class MeetingManager(GenieConsumer):
    def __init__(
        self,
    ):
        super().__init__(
            topics=[Topic.NEW_MEETING],
            consumer_group=CONSUMER_GROUP,
        )
        self.meeting_repository = meetings_repository()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        meeting = MeetingDTO.from_json(json.loads(event.body_as_str()))
        self.meeting_repository.save_meeting(meeting)
        logger.debug(f"Meeting: {meeting}, type: {type(meeting)}")
        emails_to_process = MeetingManager.filter_email_objects(
            meeting.participants_emails
        )
        logger.info(f"Emails to process: {emails_to_process}")
        for email in emails_to_process:
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                data=json.dumps(
                    {"tenant_id": meeting.tenant_id, "email": email.get("email")}
                ),
                scope="public",
            )
            event.send()

    @staticmethod
    def filter_email_objects(participants_emails):
        """
        Filter emails of:
        1. is the organizer.
        2. has the same domain as the organizer.
        3. has a public domain.
        """
        final_list = []

        host_email_list = [
            email.get("email") for email in participants_emails if email.get("self")
        ]
        host_email = host_email_list[0] if host_email_list else None
        if not host_email:
            return final_list
        host_domain = host_email.split("@")[1]
        logger.info(f"Host email: {host_email}")
        for email in participants_emails:
            email_domain = email.get("email").split("@")[1]
            if email_domain == host_domain:
                continue
            elif email_domain in PUBLIC_DOMAIN:
                continue
            else:
                final_list.append(email)
        logger.info(f"Final list: {final_list}")
        return final_list

    @staticmethod
    def filter_emails(host_email: str, participants_emails: List[ParticipantEmail]):
        """
        Filter emails of:
        1. is the organizer.
        2. has the same domain as the organizer.
        3. has a public domain.
        """
        final_list = []
        host_domain = host_email.split("@")[1]
        logger.info(f"Host email: {host_email}")
        for email_object in participants_emails:
            if isinstance(email_object, ParticipantEmail):
                email = email_object.email_address
            elif isinstance(email_object, dict):
                email = email_object.get("email")
            else:
                email = email_object
            email_domain = email.split("@")[1]
            if email_domain == host_domain:
                continue
            elif email_domain in PUBLIC_DOMAIN:
                continue
            else:
                final_list.append(email)
        logger.info(f"Final list: {final_list}")
        return final_list
