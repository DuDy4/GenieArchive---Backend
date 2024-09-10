import sys
import os

from common.utils import email_utils
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.genie_logger import GenieLogger

from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO, evaluate_meeting_classification
from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.dependencies.dependencies import meetings_repository

logger = GenieLogger()

meetings_repository = meetings_repository()


def process_meeting_from_scratch(meeting: MeetingDTO):
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
            scope="public",
        )
        event.send()
        event = GenieEvent(
            topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
            data={"tenant_id": meeting.tenant_id, "email": email},
            scope="public",
        )
        event.send()
    event = GenieEvent(
        topic=Topic.NEW_EMAIL_TO_PROCESS_DOMAIN,
        data={"tenant_id": meeting.tenant_id, "email": self_email},
        scope="public",
    )
    event.send()
    logger.info("processed meeting from scratch - finished")
    return {"status": "success"}


def process_agenda_to_all_meetings(number_of_meetings=10):
    all_meetings = meetings_repository.get_all_external_meetings_without_agenda()
    all_meetings = all_meetings[:number_of_meetings]
    logger.info(f"Processing {len(all_meetings)} meetings")
    for meeting in all_meetings:
        logger.debug(f"Processing meeting {meeting.uuid}, with agenda: {meeting.agenda}")
        if meeting.agenda:
            logger.debug("Meeting has agenda")
        else:
            meeting_goals = meetings_repository.get_meeting_goals(meeting.uuid)
            logger.debug(f"Meeting goals: {meeting_goals}")
            if meeting_goals:
                event = GenieEvent(
                    topic=Topic.NEW_MEETING_GOALS,
                    data={
                        "meeting_uuid": meeting.uuid,
                    },
                    scope="public",
                )
                event.send()
            else:
                logger.debug("Meeting has no goals")
                process_meeting_from_scratch(meeting)
        logger.debug("Processing complete")


def process_classification_to_all_meetings():
    meetings = meetings_repository.get_all_meetings_without_classification()
    for meeting in meetings:
        logger.debug(f"Processing meeting {meeting}")
        classification = evaluate_meeting_classification(meeting.participants_emails)
        meeting.classification = classification
        meetings_repository.save_meeting(meeting)
        logger.info(f"Updated meeting {meeting.uuid} with classification {classification}")


