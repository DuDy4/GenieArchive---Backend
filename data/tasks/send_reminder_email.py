from datetime import datetime, timezone
from data.data_common.dependencies.dependencies import meetings_repository
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from common.genie_logger import GenieLogger

meetings_repository = meetings_repository()
logger = GenieLogger()

def run():
    logger.info("Running send_reminder_email task")
    logger.info("Current local time: " + str(datetime.now()))
    logger.info("Current UTC time: " + str(datetime.now(timezone.utc)))

    # Fetch meetings where reminders need to be sent
    meetings_to_send_reminders = meetings_repository.get_meetings_to_send_reminders()
    logger.info(f"Number of meetings to send reminders: {len(meetings_to_send_reminders)}")

    for meeting in meetings_to_send_reminders:
        logger.info(f"Sending reminder for meeting: {meeting.subject}, "
                    f"Start Time (UTC): {meeting.start_time}, "
                    f"Classification: {meeting.classification.value}")

        event = GenieEvent(
            topic=Topic.NEW_UPCOMING_MEETING,
            data={"meeting_uuid": meeting.uuid},
        )
        event.send()

    logger.info("Completed send_reminder_email task")

if __name__ == "__main__":
    run()
