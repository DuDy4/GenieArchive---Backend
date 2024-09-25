import sys
import os

from data.data_common.events.topics import Topic

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.dependencies.dependencies import (
    get_db_connection,
)

TENANT_ID = 'org_RPLWQRTI8t7EWU1L'
def test_meetings():
    conn = get_db_connection()
    meetings_repository = MeetingsRepository(conn=conn)
    for meeting in meetings:
        meeting_dto = MeetingDTO.from_dict(meeting)
        if not meetings_repository.exists(meeting_dto.google_calendar_id):
            meetings_repository.insert_meeting(meeting_dto)
        assert meetings_repository.exists(meeting_dto.google_calendar_id)
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
        "uuid": "65b5afe83",
        "google_calendar_id": "d02e293",
        "tenant_id": TENANT_ID,
        "link": "https://meet.google.com/bla-bla-bla",
        "subject": "Intro Me <> You",
        "participants_emails": [{"email":"asaf@genieai.ai", "self" : True}, {"email":"steve@apple.com"}],
        "attendees" : [{"email":"asaf@genieai.ai", "self" : True}, {"email":"steve@apple.com"}],
        "start_time": "2024-07-27T17:00:00+03:00",
        "start" : {"date" : "2024-09-27T17:00:00+03:00"},
        "end_time": "2024-07-27T17:30:00+03:00",
        "end" : {"date" : "2024-09-27T17:45:00+03:00"},
        "agenda": []
    },
    # {
    #     "uuid": "65b5afe9",
    #     "google_calendar_id": "d02e30",
    #     "tenant_id": "TestOwner",
    #     "link": "https://meet.google.com/bla-bla-bla2",
    #     "subject": "Second intro Me <> You",
    #     "participants_emails": ["asaf@genieai.ai"],
    #     "start_time": "2024-07-24T16:00:00+03:00",
    #     "end_time": "2024-07-24T17:30:00+03:00",
    # },
    # {
    #     "uuid": "65b5afd0",
    #     "google_calendar_id": "d02e31",
    #     "tenant_id": "TestOwner",
    #     "link": "https://meet.google.com/bla-bla-bla3",
    #     "subject": "Hackathon",
    #     "participants_emails": ["asaf@genieai.ai"],
    #     "start_time": "2024-07-30",
    #     "end_time": "2024-07-31",
    # },
]

test_meetings()
