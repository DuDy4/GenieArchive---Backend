from datetime import datetime, timezone
from dateutil import parser
from data.data_common.dependencies.dependencies import meetings_repository
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from common.genie_logger import GenieLogger

meetings_repository = meetings_repository()
logger = GenieLogger()

def run():
    logger.info("Running send_reminder_email task")
    current_local_time = datetime.now()
    current_utc_time = datetime.now(timezone.utc)
    logger.info(f"Current local time: {current_local_time}")
    logger.info(f"Current UTC time: {current_utc_time}")

    # Fetch meetings where reminders need to be sent
    meetings_to_send_reminders = meetings_repository.get_meetings_to_send_reminders()
    logger.info(f"Number of meetings to send reminders: {len(meetings_to_send_reminders)}")

    next_meeting, start_time_utc, next_meeting_reminder_time_utc = meetings_repository.get_next_meeting()
    if next_meeting:
        # Parse `start_time` with its timezone info
        next_meeting_time = parser.isoparse(next_meeting.start_time)

        # Ensure conversion to UTC
        next_meeting_time_utc = next_meeting_time.astimezone(timezone.utc)
        logger.info(f"Next meeting: {next_meeting.subject}, start_time: {next_meeting.start_time}")
        logger.info(f"Start time from db: {start_time_utc}")
        logger.info(f"Next meeting start time in UTC: {next_meeting_time_utc}")
        logger.info(f"Next meeting reminder time in UTC: {next_meeting_reminder_time_utc}")


        # Check if the next meeting is due for a reminder
        if next_meeting_time_utc <= current_utc_time:
            logger.warning("The next meeting's start time has already passed or is now. It should not be recognized as the next meeting for reminders.")
    else:
        logger.info("No upcoming meetings found in the repository.")

    for meeting in meetings_to_send_reminders:
        # Convert and log the reminder schedule to check if it matches the current time
        reminder_schedule = next_meeting_reminder_time_utc

        logger.info(f"Evaluating meeting for reminder: {meeting.subject}, Start Time (UTC): {meeting.start_time}, Classification: {meeting.classification.value}")

        logger.info(f"Sending reminder for meeting: {meeting.subject}")
        event = GenieEvent(
            topic=Topic.NEW_UPCOMING_MEETING,
            data={"meeting_uuid": meeting.uuid},
        )
        try:
            event.send()
            logger.info(f"Reminder sent successfully for meeting: {meeting.subject}, meeting UUID: {meeting.uuid}")
        except Exception as e:
            logger.error(f"Failed to send reminder for meeting: {meeting.subject}, meeting UUID: {meeting.uuid}. Error: {str(e)}")

    logger.info("Completed send_reminder_email task")

if __name__ == "__main__":
    run()
