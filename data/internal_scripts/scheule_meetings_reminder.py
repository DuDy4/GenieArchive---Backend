from datetime import datetime, timezone
from common.genie_logger import GenieLogger
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO
from data.data_common.dependencies.dependencies import meetings_repository


logger = GenieLogger()
meetings_repository = meetings_repository()


def get_all_meetings_without_reminders():
    meetings = meetings_repository.get_all_meetings_without_reminders()
    return meetings


def filter_future_meetings(meetings: list[MeetingDTO]) -> list[MeetingDTO]:
    """Filter meetings to include only present and future meetings."""
    now = datetime.now(timezone.utc)
    return [
        meeting for meeting in meetings
        if datetime.fromisoformat(meeting.start_time).astimezone(timezone.utc) >= now
    ]


def update_meeting_reminder_timestamp(meetings: list[MeetingDTO]):
    # First filter for future meetings
    future_meetings = filter_future_meetings(meetings)

    # Then sort and take first 5
    future_meetings = sorted(future_meetings, key=lambda x: x.start_time)
    future_meetings = future_meetings

    for meeting in future_meetings:
        logger.info(
            f"Updating reminder timestamp for meeting: {meeting.subject}, "
            f"Start Time (UTC): {meeting.start_time}, "
            f"Classification: {meeting.classification.value}"
        )
        meetings_repository.save_meeting(meeting)

    return future_meetings


if __name__ == "__main__":
    meetings = get_all_meetings_without_reminders()
    updated_meetings = update_meeting_reminder_timestamp(meetings)
    logger.info(
        f"Updated reminders for {len(updated_meetings)} future meetings"
    )