import sys
import os

from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.events.topics import Topic
from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.repositories.profiles_repository import ProfilesRepository

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.dependencies.dependencies import (
    meetings_repository,
)

from common.genie_logger import GenieLogger
logger = GenieLogger()

# TENANT_ID = 'org_RPLWQRTI8t7EWU1L'
SELLER_EMAIL = 'asaf@genieai.ai'
EMAIL = "asaf.savich@trywonder.ai"
GOOGLE_CALENDAR_ID = "d02e2941"

def test_meetings():
    logger.set_tenant_id(TENANT_ID)
    logger.set_email(SELLER_EMAIL)
    meeting_repository = meetings_repository()
    for meeting in meetings:
        meeting_dto = MeetingDTO.from_dict(meeting)
        if not meeting_repository.exists(meeting_dto.google_calendar_id):
            meeting_repository.insert_meeting(meeting_dto)
        assert meeting_repository.exists(meeting_dto.google_calendar_id)
        print("Meetings test passed")
    data_to_send = {
        "tenant_id": TENANT_ID,
        "meetings": meetings
    }
    event = GenieEvent(
        topic=Topic.NEW_MEETINGS_TO_PROCESS,
        data=data_to_send,
        scope="public",
    )
    event.send()


meetings = [
    {
        "uuid": GOOGLE_CALENDAR_ID,
        "google_calendar_id": GOOGLE_CALENDAR_ID,
        "tenant_id": TENANT_ID,
        "link": "https://meet.google.com/bla-bla-bla",
        "subject": "Intro Me <> You",
        "participants_emails": [{"email":SELLER_EMAIL, "self" : True}, {"email": EMAIL}],
        "attendees" : [{"email":SELLER_EMAIL, "self" : True}, {"email": EMAIL}],
        "start_time": "2024-11-27T17:00:00+03:00",
        "start" : {"date" : "2024-11-27T17:00:00+03:00"},
        "end_time": "2024-11-27T17:30:00+03:00",
        "end" : {"date" : "2024-11-27T17:45:00+03:00"},
        "agenda": []
    },
]

def clean_persons():
    meetings_repository = meetings_repository()
    # profiles_repository = ProfilesRepository(conn=conn)
    # persons_repository = PersonsRepository(conn=conn)
    for meeting in meetings:
        meeting_dto = MeetingDTO.from_dict(meeting)
        if meetings_repository.exists(meeting_dto.google_calendar_id):
            print("Deleting meeting")
            meetings_repository.hard_delete(meeting_dto.google_calendar_id)
            # if persons_repository.exists_properties(PersonDTO.from_dict({"email":EMAIL})):
            #     print("Deleting person")
            #     profiles_repository.delete_by_email(EMAIL)
        else:
            print("Meeting not found")
        assert not meetings_repository.exists(meeting_dto.google_calendar_id)
        print("Meetings test passed")
    


# clean_persons()
# test_meetings()

