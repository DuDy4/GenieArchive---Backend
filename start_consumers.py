import asyncio
from loguru import logger
from data.pdl_consumer import PDLConsumer
from data.person_langsmith import LangsmithConsumer
from data.persons_manager import PersonManager
from data.emails_manager import EmailManager
from data.meetings_consumer import MeetingManager
from data.hunter_domain_consumer import HunterDomainConsumer


# from data.slack_consumer import SlackConsumer


async def run_consumers():
    # Create instances of each consumer
    consumers = [
        PersonManager(),
        LangsmithConsumer(),
        PDLConsumer(),
        EmailManager(),
        MeetingManager(),
        HunterDomainConsumer(),
        # SlackConsumer(),
    ]

    # Start each consumer in its own task
    tasks = [asyncio.create_task(consumer.start()) for consumer in consumers]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Consumers have been cancelled.")
    finally:
        await cleanup(consumers, tasks)


async def cleanup(consumers, tasks):
    logger.info("Cleaning up consumers.")
    for consumer in consumers:
        await consumer.stop()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("All consumers cleaned up.")


if __name__ == "__main__":
    try:
        asyncio.run(run_consumers())
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, stopping consumers.")
        tasks = asyncio.all_tasks()
        for task in tasks:
            task.cancel()
        asyncio.run(cleanup(tasks))
