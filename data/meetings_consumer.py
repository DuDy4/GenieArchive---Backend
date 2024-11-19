import asyncio
import json
import os
import sys
import traceback
from datetime import datetime
import pytz

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from common.utils import env_utils, email_utils
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.dependencies.dependencies import (
    meetings_repository,
    personal_data_repository,
    companies_repository,
    tenants_repository,
    profiles_repository,
    ownerships_repository,
)
from ai.langsmith.langsmith_loader import Langsmith
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO, AgendaItem, MeetingClassification
from data.data_common.events.genie_consumer import GenieConsumer
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.events.genie_event_batch_manager import EventHubBatchManager
from data.api_services.embeddings import GenieEmbeddingsClient


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
                Topic.NEW_PROCESSED_PROFILE,
                Topic.NEW_MEETING_GOALS,
                Topic.NEW_EMBEDDED_DOCUMENT,
            ],
            consumer_group=CONSUMER_GROUP,
        )
        self.meetings_repository = meetings_repository()
        self.personal_data_repository = personal_data_repository()
        self.companies_repository = companies_repository()
        self.tenant_repository = tenants_repository()
        self.profiles_repository = profiles_repository()
        self.ownerships_repository = ownerships_repository()
        self.langsmith = Langsmith()
        self.embeddings_client = GenieEmbeddingsClient()

    async def process_event(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        topic = event.properties.get(b"topic").decode("utf-8")
        logger.info(f"Processing event on topic {topic}")
        # Should use Topic class

        match topic:
            case Topic.NEW_MEETINGS_TO_PROCESS:
                logger.info("Handling new meetings to process")
                await self.handle_new_meetings_to_process(event)
            case Topic.NEW_MEETING:
                logger.info("Handling new person")
                await self.handle_new_meeting(event)
            case Topic.PDL_UP_TO_DATE_ENRICHED_DATA:
                logger.info("Handling PDL up to date enriched data")
                await self.create_goals_from_new_personal_data(event)
            case Topic.PDL_UPDATED_ENRICHED_DATA:
                logger.info("Handling PDL updated enriched data")
                await self.create_goals_from_new_personal_data(event)
            case Topic.APOLLO_UP_TO_DATE_ENRICHED_DATA:
                logger.info("Handling Apollo up to date enriched data")
                await self.create_goals_from_new_personal_data(event)
            case Topic.APOLLO_UPDATED_ENRICHED_DATA:
                logger.info("Handling Apollo updated enriched data")
                await self.create_goals_from_new_personal_data(event)
            case Topic.COMPANY_NEWS_UPDATED:
                logger.info("Handling company news updated")
                await self.create_goals_from_new_company_data(event)
            case Topic.COMPANY_NEWS_UP_TO_DATE:
                logger.info("Handling company news up to date")
                await self.create_goals_from_new_company_data(event)
            case Topic.NEW_PROCESSED_PROFILE:
                logger.info("Handling new processed profile")
                await self.create_agenda_from_profile(event)
            case Topic.NEW_MEETING_GOALS:
                logger.info("Handling new meeting goals")
                await self.create_agenda_from_goals(event)
            case Topic.NEW_EMBEDDED_DOCUMENT:
                logger.info("Handling new embedded document")
                await self.handle_new_embedded_document(event)
            case _:
                logger.info(f"Unknown topic: {topic}")

    async def handle_new_meetings_to_process(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        event_body = json.loads(event.body_as_str())
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        meetings = event_body.get("meetings")

        tenant_id = event_body.get("tenant_id")
        self_email = self.tenant_repository.get_tenant_email(tenant_id)
        emails_to_send_events = []
        meetings_dto_to_check_deletion = []  # List of meetings to check if they need to be deleted

        # Convert the meeting start times to timezone-aware datetime and sort meetings by start time
        def get_start_time(meeting):
            start_time = meeting["start"].get("dateTime")
            if start_time:
                # Convert the time to a timezone-aware datetime object
                return datetime.fromisoformat(start_time).astimezone(pytz.UTC)
            return None

        # Sort meetings by the start time
        meetings = sorted(meetings, key=lambda m: get_start_time(m) or datetime.max.replace(tzinfo=pytz.UTC))
        tasks = [self.handle_meeting_to_process(meeting, emails_to_send_events, meetings_dto_to_check_deletion, tenant_id) for meeting in meetings]
        await asyncio.gather(*tasks)
        event_batch = EventHubBatchManager()

        logger.info(f"Emails to send events: {emails_to_send_events}")
        event = GenieEvent(
            topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
            data={"tenant_id": tenant_id, "email": self_email},
        )
        event_batch.queue_event(event)

        for email in emails_to_send_events:
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
                data={"tenant_id": tenant_id, "email": email},
            )
            event_batch.queue_event(event)
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                data={"tenant_id": tenant_id, "email": email},
            )
            event_batch.queue_event(event)
        await event_batch.send_batch()
        self.handle_check_meetings_to_delete(meetings_dto_to_check_deletion, tenant_id)
        return {"status": "success"}

    async def handle_meeting_to_process(self, meeting: MeetingDTO, emails_to_send_events, meetings_dto_to_check_deletion, tenant_id):
        logger.debug(f"Meeting: {str(meeting)[:300]}, type: {type(meeting)}")
        if isinstance(meeting, str):
            meeting = json.loads(meeting)
        meeting = MeetingDTO.from_google_calendar_event(meeting, tenant_id)
        meetings_dto_to_check_deletion.append(
            meeting
        )  # Save the meeting to the list of meetings that we later verify if they need to be deleted
        meeting_in_database = self.meetings_repository.get_meeting_by_google_calendar_id(
            meeting.google_calendar_id, tenant_id
        )
        if meeting_in_database:
            if tenant_id != meeting_in_database.tenant_id:
                logger.info(f"Meeting exists in another tenant: {meeting_in_database.tenant_id}")

            elif self.check_same_meeting(meeting, meeting_in_database):
                logger.info("Meeting already in database")
                return
        self.meetings_repository.save_meeting(meeting)
        if meeting.classification.value != MeetingClassification.EXTERNAL.value:
            logger.info(f"Meeting is {meeting.classification.value}. skipping")
            return
        participant_emails = meeting.participants_emails
        try:
            self_email = [email for email in participant_emails if email.get("self")][0].get("email")
        except IndexError:
            logger.error(f"Could not find self email in {participant_emails}")
            return
        self_domain = self_email.split("@")[1] if "@" in self_email else None
        if self_domain:
            additional_domains = self.companies_repository.get_additional_domains(self_email.split("@")[1])
            emails_to_process = email_utils.filter_emails_with_additional_domains(self_email, participant_emails, additional_domains)
        else:
            emails_to_process = email_utils.filter_emails(self_email, participant_emails)
        logger.info(f"Emails to process: {emails_to_process}")
        emails_to_send_events.extend(emails_to_process)

    async def handle_new_meeting(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        meeting = MeetingDTO.from_json(json.loads(event.body_as_str()))
        logger.debug(f"Meeting: {str(meeting)[:300]}")
        meeting_in_database = self.meetings_repository.get_meeting_by_google_calendar_id(
            meeting.google_calendar_id, meeting.tenant_id
        )
        if self.check_same_meeting(meeting, meeting_in_database):
            logger.info("Meeting already in database")
            return
        self.meetings_repository.save_meeting(meeting)
        participant_emails = meeting.participants_emails
        try:
            self_email = [email for email in participant_emails if email.get("self")][0].get("email")
        except IndexError:
            logger.error(f"Could not find self email in {participant_emails}")
            return
        self_domain = self_email.split("@")[1] if "@" in self_email else None
        if self_domain:
            additional_domains = self.companies_repository.get_additional_domains(self_email.split("@")[1])
            emails_to_process = email_utils.filter_emails_with_additional_domains(self_email, participant_emails, additional_domains)
        else:
            emails_to_process = email_utils.filter_emails(self_email, participant_emails)
        logger.info(f"Emails to process: {emails_to_process}")
        for email in emails_to_process:
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                data={"tenant_id": meeting.tenant_id, "email": email.get("email")},
            )
            event.send()
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
                data={"tenant_id": meeting.tenant_id, "email": email.get("email")},
            )
            event.send()
        event = GenieEvent(
            topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
            data={"tenant_id": meeting.tenant_id, "email": self_email},
        )
        event.send()
        event = GenieEvent(
            topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
            data={"tenant_id": meeting.tenant_id, "email": self_email},
        )
        event.send()
        return {"status": "success"}

    async def create_goals_from_new_personal_data(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        event_body = json.loads(event.body_as_str())
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        tenant_id = event_body.get("tenant_id")
        if not tenant_id:
            tenant_id = logger.get_tenant_id()
        logger.debug(f"Tenant ID: {tenant_id}")
        seller_email = self.tenant_repository.get_tenant_email(tenant_id)
        person = event_body.get("person")
        if not person:
            logger.error("No person in event")
            return
        person = json.loads(person) if isinstance(person, str) else person
        logger.debug(f"Person: {person}, type: {type(person)}")
        person = PersonDTO.from_dict(person)
        logger.debug(f"Person: {person}")

        meetings = self.meetings_repository.get_meetings_without_goals_by_email(person.email)
        if not meetings:
            logger.error(f"No meetings found for {person.email}")
            return
        for meeting in meetings:
            self_email_list = [email for email in meeting.participants_emails if email.get("self")]
            self_email = self_email_list[0].get("email") if self_email_list else None
            if not self_email:
                logger.error(f"No self email found in for meeting: {meeting.uuid}, {meeting.subject}")
                self_email = self.tenant_repository.get_tenant_email(meeting.tenant_id)
                if not self_email:
                    logger.error(f"CRITICAL ERROR: No self email found for tenant: {meeting.tenant_id}")
                    continue
            self_company = self.companies_repository.get_company_from_domain(self_email.split("@")[1])
            if not self_company:
                logger.error(f"No company found for {self_email}. Waiting for company data...")
                continue

            personal_data = self.personal_data_repository.get_pdl_personal_data(person.uuid)
            if not personal_data:
                personal_data = self.personal_data_repository.get_apollo_personal_data(person.uuid)
            if not personal_data:
                logger.error(
                    f"CRITICAL ERROR - no personal data were found after an event that announced they were updated"
                )
                continue
            seller_context = None
            if seller_email:
                seller_context = self.embeddings_client.search_materials_by_prospect_data(
                    seller_email, personal_data
                )
                seller_context = " || ".join(seller_context) if seller_context else None
            logger.info(f"About to run ask langsmith for meeting goals for meeting {meeting.uuid}")
            meetings_goals = await self.langsmith.run_prompt_get_meeting_goals(
                personal_data=personal_data, my_company_data=self_company, seller_context=seller_context
            )
            if not meetings_goals:
                logger.error(f"No meeting goals found for {person.email}")
                continue
            logger.info(f"Meetings goals: {meetings_goals}")
            self.meetings_repository.save_meeting_goals(meeting.uuid, meetings_goals)
            event = GenieEvent(
                topic=Topic.NEW_MEETING_GOALS,
                data={"meeting_uuid": meeting.uuid, "seller_context": seller_context},
            )
            event.send()
        logger.info(f"Finished processing meetings goals for new personal data: {person.email}")
        return {"status": "success"}

    async def create_goals_from_new_company_data(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        event_body = json.loads(event.body_as_str())
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        company_uuid = event_body.get("company_uuid")
        force_refresh_goals = event_body.get("force_refresh_goals")
        tenant_id = event_body.get("tenant_id")
        if not company_uuid:
            logger.error("No company uuid in event")
            return
        company = self.companies_repository.get_company(company_uuid)
        if not company:
            logger.error(f"No company found for {company_uuid}")
            return

        meetings_list = (
            self.meetings_repository.get_meetings_without_goals_by_company_domain(company.domain)
            if not force_refresh_goals
            else self.meetings_repository.get_all_future_external_meetings_for_tenant(tenant_id)
        )
        if not meetings_list:
            logger.error(f"No meetings found for {company.domain}")
            return
        logger.debug(f"Meetings without goals for {company.domain}: {meetings_list}")
        for meeting in meetings_list:
            if not force_refresh_goals and self.meetings_repository.get_meeting_goals(meeting.uuid):
                logger.info(f"Meeting {meeting.uuid} already has goals")
                continue
            participant_emails = meeting.participants_emails
            self_email_list = [email for email in participant_emails if email.get("self")]
            self_email = self_email_list[0].get("email") if self_email_list else None
            if not self_email:
                logger.error(f"No self email found in for meeting: {meeting.uuid}, {meeting.subject}")
                self_email = self.tenant_repository.get_tenant_email(meeting.tenant_id)
            self_domain = self_email.split("@")[1] if self_email else None
            if self_domain != company.domain:
                logger.info(
                    f"Self email domain {self_domain} does not match company domain {company.domain}."
                    f" Skipping this meeting..."
                )
                continue
            self_domain = self_email.split("@")[1] if "@" in self_email else None
            if self_domain:
                additional_domains = self.companies_repository.get_additional_domains(self_email.split("@")[1])
                filtered_emails = email_utils.filter_emails_with_additional_domains(self_email, participant_emails, additional_domains)
            else:
                filtered_emails = email_utils.filter_emails(self_email, participant_emails)
            logger.info(f"Filtered emails: {filtered_emails}")
            for email in filtered_emails:
                personal_data = self.personal_data_repository.get_pdl_personal_data_by_email(email)
                if not personal_data:
                    personal_data = self.personal_data_repository.get_apollo_personal_data_by_email(email)
                if not personal_data:
                    logger.error(f"No personal data found for {email}")
                    continue
                seller_context = self.embeddings_client.search_materials_by_prospect_data(
                    self_email, personal_data
                )
                seller_context = " || ".join(seller_context) if seller_context else None
                logger.debug(f"Got personal data for {email}: {str(personal_data)[:300]}")
                logger.info(f"About to run ask langsmith for meeting goals for meeting {meeting.uuid}")
                meetings_goals = await self.langsmith.run_prompt_get_meeting_goals(
                    personal_data=personal_data, my_company_data=company, seller_context=seller_context
                )
                logger.info(f"Meetings goals: {meetings_goals}")
                self.meetings_repository.save_meeting_goals(meeting.uuid, meetings_goals)
                logger.info(f"Meeting goals saved for {meeting.uuid}")
                event = GenieEvent(
                    topic=Topic.NEW_MEETING_GOALS,
                    data=json.dumps(
                        {
                            "meeting_uuid": meeting.uuid,
                            "seller_context": seller_context,
                            "force_refresh_agenda": force_refresh_goals,
                        }
                    ),
                )
                event.send()
                break  # Only process one email per meeting - need to implement couple attendees in the future
        logger.info(f"Finished processing meetings for new company data: {company.domain}")
        return {"status": "success"}

    async def create_agenda_from_profile(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        event_body = json.loads(event.body_as_str())
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        person = event_body.get("person")
        profile = event_body.get("profile")
        if not person:
            logger.error("No person in event")
            return
        if not profile:
            logger.error("No profile in event")
            return
        strengths = profile.get("strengths")
        if not strengths:
            logger.error("No strengths in profile")
            return

        meetings_list = self.meetings_repository.get_meetings_with_goals_without_agenda_by_email(
            person.get("email")
        )
        logger.info(f"Meetings without agenda for {person.get('email')}: {len(meetings_list)}")
        for meeting in meetings_list:
            if meeting.agenda:
                logger.error(f"Should not have got here: Meeting {meeting.uuid} already has an agenda")
                continue
            meeting_goals = self.meetings_repository.get_meeting_goals(meeting.uuid)
            if not meeting_goals:
                logger.error(f"No meeting goals found for {meeting.uuid}")
                continue
            logger.debug(f"Meeting goals: {meeting_goals}")
            meeting_details = meeting.to_dict()
            tenant_id = logger.get_tenant_id()
            seller_context = None
            if tenant_id:
                seller_email = self.tenant_repository.get_tenant_email(tenant_id)
                seller_context = self.embeddings_client.search_materials_by_prospect_data(
                    seller_email, profile
                )
                seller_context = " || ".join(seller_context) if seller_context else None
            logger.info("About to run ask langsmith for guidelines")
            agendas = await self.langsmith.run_prompt_get_meeting_guidelines(
                customer_strengths=strengths,
                meeting_details=meeting_details,
                meeting_goals=meeting_goals,
                seller_context=seller_context,
            )
            logger.info(f"Meeting agenda: {agendas}")
            try:
                agendas = [AgendaItem.from_dict(agenda) for agenda in agendas]
            except AttributeError as e:
                logger.error(f"Error converting agenda to AgendaItem: {e}")
                continue
            meeting.agenda = agendas
            self.meetings_repository.save_meeting(meeting)
            event = GenieEvent(
                topic=Topic.UPDATED_AGENDA_FOR_MEETING,
                data=json.dumps(meeting.to_dict()),
            )
            event.send()
        logger.info(f"Finished processing meetings for new profile: {person.get('email')}")
        return {"status": "success"}

    async def create_agenda_from_goals(self, event):
        logger.info(f"Person processing event: {str(event)[:300]}")
        event_body = json.loads(event.body_as_str())
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        meeting_uuid = event_body.get("meeting_uuid")
        seller_context = event_body.get("seller_context")
        seller_context = " || ".join(seller_context) if seller_context else None
        force_refresh_agenda = event_body.get("force_refresh_agenda")
        if not meeting_uuid:
            logger.error("No meeting uuid in event")
            return
        meeting = self.meetings_repository.get_meeting_data(meeting_uuid)
        if not meeting:
            logger.error(f"No meeting found for {meeting_uuid}")
            return
        if not force_refresh_agenda and meeting.agenda:
            logger.error(f"Meeting {meeting.uuid} already has an agenda")
            return
        meeting_goals = self.meetings_repository.get_meeting_goals(meeting.uuid)
        if not meeting_goals:
            logger.error(f"No meeting goals found for {meeting.uuid}")
            return
        participant_emails = meeting.participants_emails
        self_email_list = [email for email in participant_emails if email.get("self")]
        self_email = self_email_list[0].get("email") if self_email_list else None
        if not self_email:
            logger.error(f"No self email found in for meeting: {meeting.uuid}, {meeting.subject}")
            self_email = self.tenant_repository.get_tenant_email(meeting.tenant_id)
        self_domain = self_email.split("@")[1] if "@" in self_email else None
        if self_domain:
            additional_domains = self.companies_repository.get_additional_domains(self_email.split("@")[1])
            filtered_emails = email_utils.filter_emails_with_additional_domains(self_email, participant_emails, additional_domains)
        else:
            filtered_emails = email_utils.filter_emails(self_email, participant_emails)
        logger.info(f"Filtered emails: {filtered_emails}")
        for email in filtered_emails:
            profile = self.profiles_repository.get_profile_data_by_email(email)
            if not profile:
                logger.error(f"No profile found for {email}")
                continue
            strengths = profile.strengths
            if not strengths:
                logger.error(f"No strengths found in profile for {email}")
                continue
            meeting_details = meeting.to_dict()
            logger.info("About to run ask langsmith for guidelines")
            agendas = await self.langsmith.run_prompt_get_meeting_guidelines(
                customer_strengths=strengths,
                meeting_details=meeting_details,
                meeting_goals=meeting_goals,
                seller_context=seller_context,
            )
            logger.info(f"Meeting agenda: {agendas}")

            try:
                agendas = [AgendaItem.from_dict(agenda) for agenda in agendas]
            except AttributeError as e:
                logger.error(f"Error converting agenda to AgendaItem: {e}")
                traceback.print_exc()
                continue
            meeting.agenda = agendas
            self.meetings_repository.save_meeting(meeting)
            event = GenieEvent(
                topic=Topic.UPDATED_AGENDA_FOR_MEETING,
                data=json.dumps(meeting.to_dict()),
            )
            event.send()
            break
        logger.info(f"Finished processing meeting agenda for {meeting.uuid}")
        return {"status": "success"}

    def check_same_meeting(self, meeting: MeetingDTO, meeting_in_database: MeetingDTO):
        logger.debug(
            f"About to check if meetings are the same: {str(meeting)[:300]}, db:{str(meeting_in_database)[:300]}"
        )
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
        return True

    def handle_check_meetings_to_delete(self, meetings_imported: list[MeetingDTO], tenant_id: str):
        logger.info(f"Checking for meetings to delete")
        last_date_imported = meetings_imported[-1].start_time
        if not last_date_imported:
            logger.error(f"No last date imported found")
            return
        logger.info(f"Last date imported: {last_date_imported}")
        meetings_from_database = (
            self.meetings_repository.get_all_meetings_by_tenant_id_that_should_be_imported(
                len(meetings_imported), tenant_id
            )
        )
        if not meetings_from_database:
            logger.info("No meetings found")
            return {"status": "Failed to find meetings"}
        logger.info(f"Meetings to check for deletion: {len(meetings_from_database)}")
        meetings_google_ids = [meeting.google_calendar_id for meeting in meetings_imported]
        for meeting in meetings_from_database:
            if meeting.google_calendar_id in meetings_google_ids:
                meetings_google_ids.remove(meeting.google_calendar_id)
                continue
            else:
                logger.info(
                    f"Meeting {meeting.uuid} not found in imported meetings. Checking if it should be deleted"
                )
                if meeting.start_time < last_date_imported:
                    logger.info(f"Meeting {meeting.uuid} should be deleted")
                    meeting.classification = MeetingClassification.DELETED
                    self.meetings_repository.save_meeting(meeting)
                    logger.info(f"Meeting {meeting.uuid} deleted")
        logger.info(f"Finished checking for meetings to delete")

    async def handle_new_embedded_document(self, event):
        """
        This function is triggered when a new document is embedded.
        It will gather all future external meetings for the tenant.
        """
        logger.info(f"Person processing event: {str(event)[:300]}")
        event_body = json.loads(event.body_as_str())
        if isinstance(event_body, str):
            event_body = json.loads(event_body)
        tenant_id = event_body.get("tenant_id")
        if not tenant_id:
            tenant_id = logger.get_tenant_id()
        logger.info(f"Tenant ID: {tenant_id}")
        user_email = self.tenant_repository.get_tenant_email(tenant_id)
        if not user_email:
            logger.error(f"No user email found for tenant {tenant_id}")
            return
        company = self.companies_repository.get_company_from_domain(user_email.split("@")[1])
        if not company:
            logger.error(f"No company found for {user_email}")
            return
        meetings = self.meetings_repository.get_all_future_external_meetings_for_tenant(tenant_id)
        if not meetings:
            logger.error(f"No meetings found for {tenant_id}")
            return
        person_ids = self.ownerships_repository.get_all_persons_for_tenant(tenant_id)
        if person_ids:
            unique_person_ids = list(set(person_ids))
            for person_id in unique_person_ids:
                GenieEvent(
                    topic=Topic.NEW_PERSON_CONTEXT,
                    data={"tenant_id": tenant_id, "company_uuid": company.uuid, "person_id": person_id},
                ).send()

        event = GenieEvent(
            topic=Topic.COMPANY_NEWS_UP_TO_DATE,
            data={"tenant_id": tenant_id, "company_uuid": company.uuid, "force_refresh_goals": True},
        )
        event.send()
        logger.info(f"Finished processing meetings for new embedded document: {tenant_id}")
        return {"status": "success"}


if __name__ == "__main__":
    meetings_consumer = MeetingManager()
    try:
        asyncio.run(meetings_consumer.main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
