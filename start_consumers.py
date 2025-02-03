import asyncio
import os
import sys

from dotenv import load_dotenv
os.environ["HEALTH_CHECK_PORT"] = ""
from data.pdl_consumer import PDLConsumer
from data.person_langsmith import LangsmithConsumer
from data.persons_manager import PersonManager
from data.meetings_consumer import MeetingManager
from data.slack_consumer import SlackConsumer
from data.data_common.events.genie_consumer import GenieConsumer
from data.apollo_consumer import ApolloConsumer
from data.company_consumer import CompanyConsumer
from data.sales_material_consumer import SalesMaterialConsumer
from data.profile_params_consumer import ProfileParamsConsumer
from common.genie_logger import GenieLogger
from azure.monitor.opentelemetry import configure_azure_monitor

load_dotenv()
configure_azure_monitor()
logger = GenieLogger()

consumers = [
    PersonManager(),
    LangsmithConsumer(),
    PDLConsumer(),
    MeetingManager(),
    SlackConsumer(),
    ApolloConsumer(),
    CompanyConsumer(),
    ProfileParamsConsumer(),
    SalesMaterialConsumer()
]


async def run_consumers():
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
        try:
            await consumer.stop()
        except Exception as e:
            logger.error(f"Error stopping consumer: {e}")

    try:
        await GenieConsumer.cleanup()
    except Exception as e:
        logger.error(f"Error in GenieConsumer cleanup: {e}")

    # Close any remaining aiohttp sessions and cancel tasks
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    for task in tasks:
        task.cancel()

    # Wait for all tasks to complete
    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("All consumers cleaned up.")


async def main():
    try:
        await run_consumers()
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, stopping consumers.")
        await cleanup(consumers)
        # Give a moment for other closures to complete
        await asyncio.sleep(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
