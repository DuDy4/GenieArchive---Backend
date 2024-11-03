from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
import logging
import time
from data.tasks import send_reminder_email  # Import the task module

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SchedulerService:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def add_task(self, task_module, immediate=False):
        """
        Schedule the task with fixed intervals and optionally run immediately.

        :param task_module: The task module to run
        :param immediate: Whether to run the task immediately on startup
        """
        # Run the task immediately on startup if `immediate` is True
        if immediate:
            self.scheduler.add_job(task_module.run, DateTrigger(run_date=datetime.now()))

        # Schedule the task for every 15 minutes at fixed intervals
        cron_trigger = CronTrigger(minute="*/5")
        self.scheduler.add_job(task_module.run, cron_trigger, id=task_module.__name__, replace_existing=True)

        logger.info(f"Scheduled task '{task_module.__name__}' for every 15 minutes at fixed intervals.")

    def stop(self):
        self.scheduler.shutdown()

if __name__ == "__main__":
    scheduler_service = SchedulerService()
    # Add the send_reminder_email task with immediate start and fixed interval
    scheduler_service.add_task(send_reminder_email, immediate=True)

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler_service.stop()
        logger.info("Scheduler stopped.")