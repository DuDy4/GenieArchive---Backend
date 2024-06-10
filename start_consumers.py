import asyncio

from loguru import logger

from data.pdl import PDLConsumer
from data.persons_manager import PersonManager
from app.services.SalesforceConsumer import SalesforceConsumer
from data.person_langsmith import Person


async def run_all_consumers(consumers):
    await asyncio.gather(*(consumer.start() for consumer in consumers))


def start_consumers():
    """
    Starts the consumers for the Salesforce and PersonManager.
    """
    # salesforce_consumer = SalesforceConsumer()
    # logger.info(f"SFConsumer's topics: {salesforce_consumer.topics}")
    person_manager = PersonManager()
    person = Person()
    pdl_consumer = PDLConsumer()

    # asyncio.run(run_all_consumers([person_manager, person, pdl_consumer]))
    # person_manager.run()
    person.run()
    # pdl_consumer.run()


if __name__ == "__main__":
    logger.info("Starting consumers")
    start_consumers()
