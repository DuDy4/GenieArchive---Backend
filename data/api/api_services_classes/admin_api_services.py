import asyncio
import json
from errno import EHOSTDOWN

from common.utils import email_utils
from data.data_common.data_transfer_objects.meeting_dto import (
    MeetingDTO,
    evaluate_meeting_classification,
    AgendaItem,
    MeetingClassification,
)
from data.api_services.embeddings import GenieEmbeddingsClient
from ai.langsmith.langsmith_loader import Langsmith

from data.data_common.dependencies.dependencies import (
    tenants_repository,
    google_creds_repository,
    persons_repository,
    ownerships_repository,
    meetings_repository,
    profiles_repository,
    companies_repository,
    personal_data_repository,
)
from common.genie_logger import GenieLogger
import uuid
from fastapi import HTTPException

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

logger = GenieLogger()


class AdminApiService:
    def __init__(self):
        self.tenants_repository = tenants_repository()
        self.google_creds_repo = google_creds_repository()
        self.persons_repository = persons_repository()
        self.ownerships_repository = ownerships_repository()
        self.meetings_repository = meetings_repository()
        self.profiles_repository = profiles_repository()
        self.companies_repository = companies_repository()
        self.personal_data_repository = personal_data_repository()
        self.embeddings_client = GenieEmbeddingsClient()
        self.langsmith = Langsmith()

    def sync_profile(self, person_uuid):
        self.validate_uuid(person_uuid)
        person = self.persons_repository.get_person(person_uuid)
        if not person:
            logger.error(f"Person not found: {person_uuid}")
            return {"error": "Person not found"}
        logger.info(f"Got person: {person}")
        if person.linkedin:
            event = GenieEvent(Topic.PDL_NEW_PERSON_TO_ENRICH, person.to_json(), "public")
            event.send()
        else:
            logger.error(f"Person does not have a LinkedIn URL")
            return {"error": "Person does not have a LinkedIn URL"}
        return {"message": "Profile sync initiated for " + person.email}

    def sync_email(self, person_uuid):
        self.validate_uuid(person_uuid)
        person = self.persons_repository.get_person(person_uuid)
        if not person:
            logger.error(f"Person not found: {person_uuid}")
            return {"error": "Person not found"}
        logger.info(f"Got person: {person}")
        tenants = self.ownerships_repository.get_tenants_for_person(person_uuid)
        if not tenants or len(tenants) == 0:
            logger.error(f"Person does not have any tenants: {person_uuid}")
            return {"error": "Person does not have any tenants"}
        logger.set_tenant_id(tenants[0])
        event = GenieEvent(
            topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
            data=json.dumps({"tenant_id": tenants[0], "email": person.email}),
        )
        event.send()
        return {"message": "Email sync initiated for " + person.email}

    def validate_uuid(self, uuid_string):
        try:
            val = uuid.UUID(uuid_string, version=4)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid UUID format")
        return str(val)

    def fetch_all_tenants(self):
        all_tenants = self.tenants_repository.get_all_tenants()
        response = {"admin": True, "tenants": [tenant.to_dict() for tenant in all_tenants]}
        return response

    def process_meeting_from_scratch(self, meeting: MeetingDTO):
        participant_emails = meeting.participants_emails
        try:
            self_email = [email for email in participant_emails if email.get("self")][0].get("email")
        except IndexError:
            logger.error(f"Could not find self email in {participant_emails}")
            return
        emails_to_process = email_utils.filter_emails(self_email, participant_emails)
        logger.info(f"Emails to process: {emails_to_process}")
        for email in emails_to_process:
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_ADDRESS_TO_PROCESS,
                data={"tenant_id": meeting.tenant_id, "email": email},
            )
            event.send()
            event = GenieEvent(
                topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN, data={"tenant_id": meeting.tenant_id, "email": email}
            )
            event.send()
        event = GenieEvent(
            topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
            data={"tenant_id": meeting.tenant_id, "email": self_email},
        )
        event.send()
        logger.info("processed meeting from scratch - finished")
        return {"status": "success"}

    def process_agenda_to_all_meetings(self, number_of_meetings=10):
        all_meetings = self.meetings_repository.get_all_external_meetings_without_agenda()
        logger.info(f"Found {len(all_meetings)} meetings without agenda")
        all_meetings = all_meetings[:number_of_meetings]
        logger.info(f"Processing {len(all_meetings)} meetings")
        for meeting in all_meetings:
            logger.debug(f"Processing meeting {meeting.uuid}, with agenda: {meeting.agenda}")
            if meeting.agenda:
                logger.debug("Meeting has agenda")
            else:
                meeting_goals = self.meetings_repository.get_meeting_goals(meeting.uuid)
                logger.debug(f"Meeting goals: {meeting_goals}")
                if meeting_goals:
                    event = GenieEvent(topic=Topic.NEW_MEETING_GOALS, data={"meeting_uuid": meeting.uuid})
                    event.send()
                else:
                    logger.debug("Meeting has no goals")
                    self.process_meeting_from_scratch(meeting)
            logger.debug("Processing complete")
        logger.info("Finished processing all meetings")
        all_meetings = self.meetings_repository.get_all_external_meetings_without_agenda()
        logger.info(f"After processing, found {len(all_meetings)} meetings without agenda")
        return {"status": "success"}

    def process_classification_to_all_meetings(self):
        meetings = self.meetings_repository.get_all_meetings_without_classification()
        for meeting in meetings:
            logger.debug(f"Processing meeting {meeting}")
            classification = evaluate_meeting_classification(meeting.participants_emails)
            meeting.classification = classification
            logger.info(f"Updated meeting {meeting.uuid} with classification {meeting.classification}")
            self.meetings_repository.save_meeting(meeting)
            logger.info(f"Updated meeting {meeting.uuid} with classification {classification}")
        return {"status": f"success. evaluated classification for {len(meetings)} meetings"}

    def process_new_classification_to_all_meetings(self):
        meetings = self.meetings_repository.get_all_meetings()
        for meeting in meetings:
            logger.debug(f"Processing meeting {meeting}")
            classification = evaluate_meeting_classification(meeting.participants_emails)
            meeting.classification = classification
            self.meetings_repository.save_meeting(meeting)
            logger.info(f"Updated meeting {meeting.uuid} with classification {classification}")

    def process_missing_profiles_from_existing_personal_data(self):
        profiles_uuid = self.profiles_repository.get_missing_profiles()
        logger.info(f"Found {len(profiles_uuid)} profiles without profile")
        for profile_uuid in profiles_uuid:
            logger.info(f"Syncing profile for {profile_uuid}")
            result = self.sync_profile(profile_uuid)
            if result.get("error"):
                logger.error(f"Error syncing profile: {result}")
                email = self.persons_repository.get_person(profile_uuid).email
                result = self.sync_email(profile_uuid)
                if result.get("error"):
                    logger.error(f"Error syncing email: {result}")
                else:
                    logger.info(f"Email synced for {email}")

    def process_single_meeting_agenda(self, meeting_uuid: str):
        meeting = self.meetings_repository.get_meeting_data(meeting_uuid)
        if not meeting:
            logger.error(f"Meeting not found: {meeting_uuid}")
            return {"error": "Meeting not found"}
        if meeting.agenda:
            logger.error(f"Meeting already has an agenda: {meeting_uuid}")
            return {"error": "Meeting already has an agenda"}
        if meeting.classification.value != MeetingClassification.EXTERNAL.value:
            logger.error(f"Meeting is not external: {meeting_uuid}")
            return {"error": "Meeting is not external"}
        participant_emails = meeting.participants_emails
        if not participant_emails:
            logger.error(f"No participant emails found for meeting: {meeting.uuid}")
            return {"error": "No participant emails found"}
        host_email = self.tenants_repository.get_tenant_email(meeting.tenant_id)
        if not host_email or "@" not in host_email:
            logger.error(f"No host email found for tenant: {meeting.tenant_id}")
            return {"error": "No host email found"}
        self_email = [email for email in participant_emails if email.get("self")][0].get("email")
        if not self_email or self_email != host_email:
            logger.error(f"Host email not found in participant emails: {participant_emails}")
            return {"error": "Host email not found"}
        goals = self.meetings_repository.get_meeting_goals(meeting_uuid)
        if goals:
            return self.handle_meeting_with_goals(meeting, self_email, goals)
        else:
            return self.handle_meeting_without_goals(meeting, self_email)

    def handle_meeting_with_goals(self, meeting: MeetingDTO, self_email: str, goals: list):
        participant_emails = meeting.participants_emails
        emails = email_utils.filter_emails(self_email, participant_emails)
        profiles = self.profiles_repository.get_profiles_by_email_list(emails)
        if not profiles:
            logger.error(f"No strengths found for any participant for meeting: {meeting.uuid}")
            return {"error": f"No strengths found for any participant for meeting: {meeting.uuid}"}
        profile = profiles[0]
        seller_context = None
        if self_email:
            seller_context = self.embeddings_client.search_materials_by_prospect_data(self_email, profile)
            seller_context = " || ".join(seller_context) if seller_context else None
        logger.info(f"About to run ask langsmith for meeting guidelines for meeting {meeting.uuid}")
        meeting_guidelines = asyncio.run(
            self.langsmith.run_prompt_get_meeting_guidelines(profile, meeting, goals, seller_context)
        )
        if not meeting_guidelines:
            logger.error(f"Failed to create meeting guidelines for meeting {meeting.uuid}")
            return {"error": f"Failed to create meeting guidelines for meeting {meeting.uuid}"}
        logger.info(f"Meeting guidelines: {meeting_guidelines}")
        try:
            agendas = [AgendaItem.from_dict(agenda) for agenda in meeting_guidelines]
        except AttributeError as e:
            logger.error(f"Error converting agenda to AgendaItem: {e}")
            return {"error": f"Error converting agenda to AgendaItem: {e}"}
        meeting.agenda = agendas
        self.meetings_repository.save_meeting(meeting)
        event = GenieEvent(
            topic=Topic.UPDATED_AGENDA_FOR_MEETING,
            data=json.dumps(meeting.to_dict()),
        )
        event.send()
        return {"status": f"Created Agenda for meeting {meeting.uuid}"}

    def handle_meeting_without_goals(self, meeting: MeetingDTO, self_email: str):
        self_company = self.companies_repository.get_company_from_domain(email_utils.get_domain(self_email))
        if not self_company:
            logger.error(f"Company not found for email: {self_email}")
            return {"error": "Company not found"}
        target_personal_data = None
        for email_obj in meeting.participants_emails:
            email = email_obj.get("email")
            if not email:
                logger.error(f"No email found in participant email object: {email_obj}")
                continue
            if email == self_email:
                continue
            personal_data = self.personal_data_repository.get_any_personal_data_by_email(email)
            if personal_data:
                target_personal_data = personal_data
                break
        if not target_personal_data:
            logger.error(f"No target personal data for any participant for meeting: {meeting.uuid}")
            return {"error": "No target personal data found"}
        # Else, we have a target personal data and company data
        seller_context = None
        if self_email:
            seller_context = self.embeddings_client.search_materials_by_prospect_data(
                self_email, target_personal_data
            )
            seller_context = " || ".join(seller_context) if seller_context else None
        logger.info(f"About to run ask langsmith for meeting goals for meeting {meeting.uuid}")
        meetings_goals = asyncio.run(
            self.langsmith.run_prompt_get_meeting_goals(
                personal_data=target_personal_data,
                my_company_data=self_company,
                seller_context=seller_context,
            )
        )
        if not meetings_goals:
            logger.error(f"No meeting goals found for {meeting.uuid}")
            return {"error": "No meeting goals found"}
        logger.info(f"Meetings goals: {meetings_goals}")
        self.meetings_repository.save_meeting_goals(meeting.uuid, meetings_goals)
        event = GenieEvent(
            topic=Topic.NEW_MEETING_GOALS,
            data={"meeting_uuid": meeting.uuid, "seller_context": seller_context},
        )
        event.send()
        return {"status": f"Created Meeting goals. Proceeding to Create agenda"}
