import json

from common.utils import email_utils
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO, evaluate_meeting_classification
from data.data_common.dependencies.dependencies import (
    tenants_repository,
    google_creds_repository,
    persons_repository,
    ownerships_repository,
    meetings_repository,
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
        return {"status": "success"}

    def process_classification_to_all_meetings(self):
        meetings = self.meetings_repository.get_all_meetings_without_classification()
        for meeting in meetings:
            logger.debug(f"Processing meeting {meeting}")
            classification = evaluate_meeting_classification(meeting.participants_emails)
            meeting.classification = classification
            self.meetings_repository.save_meeting(meeting)
            logger.info(f"Updated meeting {meeting.uuid} with classification {classification}")

    def process_new_classification_to_all_meetings(self):
        meetings = self.meetings_repository.get_all_meetings()
        for meeting in meetings:
            logger.debug(f"Processing meeting {meeting}")
            classification = evaluate_meeting_classification(meeting.participants_emails)
            meeting.classification = classification
            self.meetings_repository.save_meeting(meeting)
            logger.info(f"Updated meeting {meeting.uuid} with classification {classification}")
