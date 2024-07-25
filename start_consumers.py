import asyncio

from loguru import logger

from data.pdl_consumer import PDLConsumer
from data.person_langsmith import LangsmithConsumer
from data.persons_manager import PersonManager
from data.emails_manager import EmailManager
from data.meetings_consumer import MeetingManager
from data.hunter_domain_consumer import HunterDomainConsumer
from data.slack_consumer import SlackConsumer


async def run_consumers():
    # Create instances of each consumer
    langsmith_consumer = LangsmithConsumer()
    pdl_consumer = PDLConsumer()
    person_manager = PersonManager()
    email_manager = EmailManager()
    meeting_manager = MeetingManager()
    hunter_domain_consumer = HunterDomainConsumer()
    slack_consumer = SlackConsumer()

    # Start each consumer in its own task
    tasks = [
        asyncio.create_task(person_manager.start()),
        asyncio.create_task(langsmith_consumer.start()),
        asyncio.create_task(pdl_consumer.start()),
        asyncio.create_task(email_manager.start()),
        asyncio.create_task(meeting_manager.start()),
        asyncio.create_task(hunter_domain_consumer.start()),
        asyncio.create_task(slack_consumer.start()),
    ]

    # Wait for all tasks to complete (they won't, since consumers run indefinitely)
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(run_consumers())
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, stopping consumers.")
