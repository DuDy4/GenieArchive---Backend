import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.dependencies.dependencies import (
    get_db_connection,
)


def test_meetings():
    conn = get_db_connection()
    meetings_repository = MeetingsRepository(conn=conn)
    for meeting in meetings:
        meeting_dto = MeetingDTO.from_dict(meeting)
        if not meetings_repository.exists(meeting_dto.google_calendar_id):
            meetings_repository.insert_meeting(meeting_dto)
        assert meetings_repository.exists(meeting_dto.google_calendar_id)
        print("Meetings test passed")


meetings = [
    {
        "uuid": "65b5afe8",
        "google_calendar_id": "d02e29",
        "tenant_id": "TestOwner",
        "link": "https://meet.google.com/bla-bla-bla",
        "subject": "Intro Me <> You",
        "participants_emails": ["asaf@genieai.ai"],
        "start_time": "2024-07-27T17:00:00+03:00",
        "end_time": "2024-07-27T17:30:00+03:00",
    },
    {
        "uuid": "65b5afe9",
        "google_calendar_id": "d02e30",
        "tenant_id": "TestOwner",
        "link": "https://meet.google.com/bla-bla-bla2",
        "subject": "Second intro Me <> You",
        "participants_emails": ["asaf@genieai.ai"],
        "start_time": "2024-07-24T16:00:00+03:00",
        "end_time": "2024-07-24T17:30:00+03:00",
    },
    {
        "uuid": "65b5afd0",
        "google_calendar_id": "d02e31",
        "tenant_id": "TestOwner",
        "link": "https://meet.google.com/bla-bla-bla3",
        "subject": "Hackathon",
        "participants_emails": ["asaf@genieai.ai"],
        "start_time": "2024-07-30",
        "end_time": "2024-07-31",
    },
]

test_meetings()
