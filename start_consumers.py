import asyncio
from loguru import logger
from data.pdl_consumer import PDLConsumer
from data.person_langsmith import LangsmithConsumer
from data.persons_manager import PersonManager
from data.emails_manager import EmailManager
from data.meetings_consumer import MeetingManager
from data.hunter_domain_consumer import HunterDomainConsumer

# from data.slack_consumer import SlackConsumer

consumers = [
    PersonManager(),
    LangsmithConsumer(),
    PDLConsumer(),
    EmailManager(),
    MeetingManager(),
    HunterDomainConsumer(),
    # SlackConsumer(),
]


async def run_consumers():
    # Create instances of each consumer

    # Start each consumer in its own task
    tasks = [asyncio.create_task(consumer.start()) for consumer in consumers]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Consumers have been cancelled.")
    finally:
        await cleanup(consumers)


async def cleanup(consumers):
    logger.info("Cleaning up consumers.")
    for consumer in consumers:
        await consumer.stop()
    logger.info("All consumers cleaned up.")


if __name__ == "__main__":
    try:
        asyncio.run(run_consumers())
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, stopping consumers.")
        asyncio.run(cleanup(consumers))
