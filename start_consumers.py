import asyncio

from loguru import logger

from data.pdl import PDLConsumer
from data.person_langsmith import LangsmithConsumer
from data.persons_manager import PersonManager


async def run_consumers():
    # Create instances of each consumer
    langsmith_consumer = LangsmithConsumer()
    pdl_consumer = PDLConsumer()
    person_manager = PersonManager()

    # Start each consumer in its own task
    tasks = [
        asyncio.create_task(langsmith_consumer.start()),
        asyncio.create_task(pdl_consumer.start()),
        asyncio.create_task(person_manager.start()),
    ]

    # Wait for all tasks to complete (they won't, since consumers run indefinitely)
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(run_consumers())
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, stopping consumers.")
