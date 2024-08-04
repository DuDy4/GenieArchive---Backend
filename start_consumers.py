import asyncio
import sys

from loguru import logger
from data.pdl_consumer import PDLConsumer
from data.person_langsmith import LangsmithConsumer
from data.persons_manager import PersonManager
from data.emails_manager import EmailManager
from data.meetings_consumer import MeetingManager
from data.hunter_domain_consumer import HunterDomainConsumer
from data.slack_consumer import SlackConsumer
from data.data_common.events.genie_consumer import GenieConsumer

consumers = [
    PersonManager(),
    LangsmithConsumer(),
    PDLConsumer(),
    EmailManager(),
    MeetingManager(),
    HunterDomainConsumer(),
    SlackConsumer(),
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
    tasks = asyncio.all_tasks()
    for task in tasks:
        if not task.done():
            task.cancel()

    # Wait for all tasks to complete
    await asyncio.gather(*tasks, return_exceptions=True)

    # Force close any remaining sessions and connectors
    for task in tasks:
        if "ClientSession" in str(task) or "TCPConnector" in str(task):
            try:
                obj = task.get_coro().cr_frame.f_locals.get("self")
                if hasattr(obj, "close"):
                    await obj.close()
                logger.info(f"Forcibly closed: {obj}")
            except Exception as e:
                logger.error(f"Error closing {obj}: {e}")

    logger.info("All consumers cleaned up.")


async def main():
    try:
        await run_consumers()
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, stopping consumers.")
    finally:
        await cleanup(consumers)
        # Give a moment for other closures to complete
        await asyncio.sleep(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        # Force close the event loop
        loop = asyncio.get_event_loop()
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
