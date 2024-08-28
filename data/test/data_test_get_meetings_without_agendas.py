import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.dependencies.dependencies import meetings_repository
from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


meetings_repository = meetings_repository()
email = "dan.shevel@genieai.ai"
meetings_list = meetings_repository.get_meetings_without_goals_by_email(email)
print(f"Meetings without goals for email: {email}")
[print(str(meeting)) for meeting in meetings_list]
[print(str(meeting.participants_emails)) for meeting in meetings_list]

company_meetings_list = meetings_repository.get_meetings_without_goals_by_company_domain("mabl.com")

print("Meetings without goals for mabl.com:")
[print(str(meeting)) for meeting in company_meetings_list]
