from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
import logging
import time
from data.tasks import send_reminder_email, upload_profile_picture_to_blob

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SchedulerService:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def add_task(self, task_module, interval_minutes=15, immediate=False):
        """
        Schedule the task with fixed intervals and optionally run immediately.

        :param task_module: The task module to run
        :param interval_minutes: Interval in minutes for running the task
        :param immediate: Whether to run the task immediately on startup
        """
        if immediate:
            self.scheduler.add_job(task_module.run, DateTrigger(run_date=datetime.now()))

        cron_trigger = CronTrigger(minute=f"*/{interval_minutes}")
        self.scheduler.add_job(task_module.run, cron_trigger, id=task_module.__name__, replace_existing=True)

        logger.info(f"Scheduled task '{task_module.__name__}' for every {interval_minutes} minutes.")

    def stop(self):
        self.scheduler.shutdown()

if __name__ == "__main__":
    scheduler_service = SchedulerService()
    # Add the send_reminder_email task with immediate start and a 15-minute interval
    scheduler_service.add_task(send_reminder_email, interval_minutes=15, immediate=True)

    # Add the other_task to run every hour
    scheduler_service.add_task(upload_profile_picture_to_blob, interval_minutes=60, immediate=True)

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler_service.stop()
        logger.info("Scheduler stopped.")
