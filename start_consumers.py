import asyncio
from data.persons_manager import PersonManager
from app.services.SalesforceConsumer import SalesforceConsumer
from data.person import Person


async def run_all_consumers(consumers):
    await asyncio.gather(*(consumer.start() for consumer in consumers))


def start_consumers():
    """
    Starts the consumers for the Salesforce and PersonManager.
    """
    salesforce_consumer = SalesforceConsumer()
    person_manager = PersonManager()
    person = Person()

    asyncio.run(run_all_consumers([salesforce_consumer, person_manager, person]))


if __name__ == "__main__":
    start_consumers()
