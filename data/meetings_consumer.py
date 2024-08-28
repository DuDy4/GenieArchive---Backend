import asyncio
import json
import os
from typing import List


from common.utils import env_utils
from data.api.base_models import ParticipantEmail
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.dependencies.dependencies import (
    meetings_repository,
    personal_data_repository,
    companies_repository,
)
from data.data_common.repositories.meetings_repository import MeetingsRepository
from ai.langsmith.langsmith_loader import Langsmith
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

import requests
from common.genie_logger import GenieLogger

logger = GenieLogger()

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
            topics=[
                Topic.NEW_MEETING,
                Topic.NEW_MEETINGS_TO_PROCESS,
                Topic.PDL_UP_TO_DATE_ENRICHED_DATA,
                Topic.PDL_UPDATED_ENRICHED_DATA,
                Topic.APOLLO_UPDATED_ENRICHED_DATA,
                Topic.APOLLO_UP_TO_DATE_ENRICHED_DATA,
                Topic.COMPANY_NEWS_UPDATED,
                Topic.COMPANY_NEWS_UP_TO_DATE,
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.meeting_repository = meetings_repository()
        self.personal_data_repository = personal_data_repository()
        self.companies_repository = companies_repository()
        self.langsmith = Langsmith()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        # Should use Topic class

        match topic:
            case Topic.NEW_MEETINGS_TO_PROCESS:
                logger.info("Handling new salesforce contact")
                await self.handle_new_meetings_to_process(event)
            case Topic.NEW_MEETING:
                logger.info("Handling new person")
                await self.handle_new_meeting(event)
            case Topic.PDL_UP_TO_DATE_ENRICHED_DATA:
                logger.info("Handling PDL up to date enriched data")
                await self.handle_new_personal_data(event)
            case Topic.PDL_UPDATED_ENRICHED_DATA:
                logger.info("Handling PDL updated enriched data")
                await self.handle_new_personal_data(event)
            case Topic.APOLLO_UP_TO_DATE_ENRICHED_DATA:
                logger.info("Handling Apollo up to date enriched data")
                await self.handle_new_personal_data(event)
            case Topic.APOLLO_UPDATED_ENRICHED_DATA:
                logger.info("Handling Apollo updated enriched data")
                await self.handle_new_personal_data(event)
            case Topic.COMPANY_NEWS_UPDATED:
                logger.info("Handling company news updated")
                await self.handle_new_company_data(event)
            case Topic.COMPANY_NEWS_UP_TO_DATE:
                logger.info("Handling company news up to date")
                await self.handle_new_company_data(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_new_meetings_to_process(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        event_body = json.loads(event.body_as_str())
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        meetings = event_body.get("meetings")
        tenant_id = event_body.get("tenant_id")
        for meeting in meetings:
            logger.debug(f"Meeting: {meeting}, type: {type(meeting)}")
            if isinstance(meeting, str):
                meeting = json.loads(meeting)
            meeting = MeetingDTO.from_google_calendar_event(meeting, tenant_id)
            meeting_in_database = self.meeting_repository.get_meeting_by_google_calendar_id(
                meeting.google_calendar_id
            )
            if meeting_in_database:
                if self.check_same_meeting(meeting, meeting_in_database):
                    logger.info("Meeting already in database")
                    continue
            self.meeting_repository.save_meeting(meeting)
            participant_emails = meeting.participants_emails
            try:
                self_email = [email for email in participant_emails if email.get("self")][0].get("email")
            except IndexError:
                logger.error(f"Could not find self email in {participant_emails}")
                continue
            emails_to_process = self.filter_emails(self_email, participant_emails) + [self_email]
            logger.info(f"Emails to process: {emails_to_process}")
            for email in emails_to_process:
                event = GenieEvent(
                    topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                    data=json.dumps({"tenant_id": meeting.tenant_id, "email": email}),
                    scope="public",
                )
                event.send()
                event = GenieEvent(
                    topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
                    data=json.dumps({"tenant_id": meeting.tenant_id, "email": email}),
                    scope="private",
                )
                event.send()

    async def handle_new_meeting(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        meeting = MeetingDTO.from_json(json.loads(event.body_as_str()))
        logger.debug(f"Meeting: {meeting}")
        meeting_in_database = self.meeting_repository.get_meeting_by_google_calendar_id(
            meeting.google_calendar_id
        )
        if self.check_same_meeting(meeting, meeting_in_database):
            logger.info("Meeting already in database")
            return
        self.meeting_repository.save_meeting(meeting)
        emails_to_process = MeetingManager.filter_email_objects(meeting.participants_emails)
        logger.info(f"Emails to process: {emails_to_process}")
        for email in emails_to_process:
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                data=json.dumps({"tenant_id": meeting.tenant_id, "email": email.get("email")}),
                scope="public",
            )
            event.send()
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
                data=json.dumps({"tenant_id": meeting.tenant_id, "email": email.get("email")}),
                scope="private",
            )
            event.send()

    async def handle_new_personal_data(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        event_body = json.loads(event.body_as_str())
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        tenant_id = event_body.get("tenant_id")
        logger.debug(f"Tenant ID: {tenant_id}")
        person = event_body.get("person")
        if not person:
            logger.error("No person in event")
            return
        person = json.loads(person) if isinstance(person, str) else person
        logger.debug(f"Person: {person}, type: {type(person)}")
        person = PersonDTO.from_dict(person)
        logger.debug(f"Person: {person}")

        meetings = self.meeting_repository.get_meetings_without_agenda_by_email(person.email)
        if not meetings:
            logger.error(f"No meetings found for {person.email}")
            return
        for meeting in meetings:
            self_email_list = [email for email in meeting.participants_emails if email.get("self")]
            self_email = self_email_list[0].get("email") if self_email_list else None
            if not self_email:
                logger.error(f"No self email found in {meeting.participants_emails}")
                continue
            self_company = self.companies_repository.get_company_from_domain(self_email.split("@")[1])
            if not self_company:
                logger.error(f"No company found for {self_email}")
                continue

            personal_data = self.personal_data_repository.get_pdl_personal_data(person.uuid)
            if not personal_data:
                personal_data = self.personal_data_repository.get_apollo_personal_data(person.uuid)
            if not personal_data:
                logger.error(
                    f"CRITICAL ERROR - no personal data were found after an event that announced they were updated"
                )
                continue

            meetings_goals = self.langsmith.run_prompt_get_meeting_goals(
                personal_data=personal_data, my_company_data=self_company
            )
            if not meetings_goals:
                logger.error(f"No meeting goals found for {person.email}")
                continue
            logger.info(f"Meetings goals: {meetings_goals}")
            self.meeting_repository.save_meeting_goals(meeting.uuid, meetings_goals)

    async def handle_new_company_data(self, event):
        pass

    @staticmethod
    def filter_email_objects(participants_emails):
        """
        Filter emails of:
        1. is the organizer.
        2. has the same domain as the organizer.
        3. has a public domain.
        """
        final_list = []

        host_email_list = [email.get("email") for email in participants_emails if email.get("self")]
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

    def check_same_meeting(self, meeting: MeetingDTO, meeting_in_database: MeetingDTO):
        logger.debug(f"About to check if meetings are the same: {meeting}, {meeting_in_database}")
        if not meeting_in_database:
            return False
        if meeting.start_time != meeting_in_database.start_time:
            return False
        # logger.debug(f"Meeting start times are the same")
        if meeting.end_time != meeting_in_database.end_time:
            return False
        # logger.debug(f"Meeting end times are the same")
        if meeting.location != meeting_in_database.location:
            return False
        # logger.debug(f"Meeting locations are the same")
        if meeting.subject != meeting_in_database.subject:
            return False
        # logger.debug(f"Meeting subjects are the same")
        if meeting.link != meeting_in_database.link:
            return False
        # logger.debug(f"Meeting links are the same")
        if meeting.participants_hash != meeting_in_database.participants_hash:
            return False
        # logger.debug(f"Meeting participants hashes are the same")
        return True


if __name__ == "__main__":
    meetings_consumer = MeetingManager()
    try:
        asyncio.run(meetings_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
