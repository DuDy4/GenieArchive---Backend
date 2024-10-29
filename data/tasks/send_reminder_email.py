from datetime import datetime
from data.data_common.dependencies.dependencies import meetings_repository
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from common.genie_logger import GenieLogger

meetings_repository = meetings_repository()

logger = GenieLogger()

def run():
    logger.info("Running send_reminder_email task")
    meetings_to_send_reminders = meetings_repository.get_meetings_to_send_reminders()
    logger.info(f"Meetings to send reminders: {len(meetings_to_send_reminders)}")
    for meeting in meetings_to_send_reminders:
        logger.info(f"meeting: {meeting.subject}, start_time: {meeting.start_time}, classification: {meeting.classification.value}")
        event = GenieEvent(
            topic=Topic.NEW_UPCOMING_MEETING,
            data={"meeting_uuid": meeting.uuid},
        )
        event.send()
    logger.info("Finished send_reminder_email task")

# Run every 15 minutes
cron_expression = {'minute': '*/15'}
