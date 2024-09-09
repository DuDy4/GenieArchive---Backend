import sys
import os

from common.utils import email_utils
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.genie_logger import GenieLogger

from data.data_common.data_transfer_objects.meeting_dto import (
    MeetingDTO,
    AgendaItem,
    Guidelines,
    MeetingClassification,
    evaluate_meeting_classification,
)
from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.dependencies.dependencies import (
    get_db_connection,
    meetings_repository,
)

logger = GenieLogger()

meetings_repository = meetings_repository()

meetings = meetings_repository.get_all_meetings_without_classification()
for meeting in meetings:
    logger.debug(f"Processing meeting {meeting}")
    classification = evaluate_meeting_classification(meeting.participants_emails)
    meeting.classification = classification
    meetings_repository.save_meeting(meeting)
    logger.info(f"Updated meeting {meeting.uuid} with classification {classification}")

logger.info("success")
