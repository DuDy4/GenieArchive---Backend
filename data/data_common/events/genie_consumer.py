import os
import asyncio
import traceback

import aiohttp
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub import TransportType
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from loguru import logger


class GenieConsumer:
    def __init__(self, topics, consumer_group="$Default"):
        connection_str = os.environ.get("EVENTHUB_CONNECTION_STRING", "")
        eventhub_name = os.environ.get("EVENTHUB_NAME", "")
        storage_connection_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
        blob_container_name = os.environ.get("BLOB_CONTAINER_NAME", "")
        checkpoint_store = BlobCheckpointStore.from_connection_string(
            storage_connection_str, blob_container_name
        )
        self.consumer = EventHubConsumerClient.from_connection_string(
            conn_str=connection_str,
            consumer_group=consumer_group,
            eventhub_name=eventhub_name,
            checkpoint_store=checkpoint_store,
            transport_type=TransportType.AmqpOverWebsocket,  # Optional: use AMQP over WebSockets if necessary for network conditions
        )
        self.topics = topics

    async def on_event(self, partition_context, event):
        logger.info(f"Received event")
        topic = event.properties.get(b"topic")
        logger.info(f"Event topic: {topic}")
        logger.info(f"Event decoded: {topic.decode('utf-8')}")
        logger.debug(f"Topics: {self.topics}")
        try:
            if topic and (topic.decode("utf-8") in self.topics):
                # logger.info(f"About to process event: {event}")
                event_result = await self.process_event(event)
                logger.info(f"Event processed. Body: {event_result}")
        except Exception as e:
            logger.info("Exception occurred:", e)
            logger.info("Detailed traceback information:")
            traceback.print_exc()
        await partition_context.update_checkpoint(event)

    async def process_event(self, event):
        """Override this method in subclasses to define event processing logic."""
        raise NotImplementedError("Must be implemented in subclass")

    async def start(self):
        logger.info(
            f"Starting consumer for topics: {self.topics} on group: {self.consumer._consumer_group}"
        )
        try:
            async with self.consumer:
                await self.consumer.receive(
                    on_event=self.on_event, starting_position="-1", prefetch=1
                )
        except asyncio.CancelledError:
            logger.warning("Consumer cancelled, closing consumer.")
            await self.consumer.close()
        except Exception as e:
            logger.error(f"Error occurred while running consumer: {e}")
            logger.error("Detailed traceback information:")
            traceback.print_exc()
            await self.consumer.close()

    def run(self):
        try:
            asyncio.run(self.start())
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt, stopping consumer.")
        finally:
            asyncio.run(self.cleanup())

    async def cleanup(self):
        # Cancel any remaining tasks
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()

        # Wait for tasks to be cancelled
        pending = asyncio.as_completed(tasks)
        for task in pending:
            try:
                await task
            except asyncio.CancelledError:
                continue

        # Close the aiohttp ClientSession and Connector
        await self.consumer.close()

        # Close any open aiohttp ClientSession instances
        # await aiohttp.ClientSession.close_all()

        # Close any open aiohttp Connector instances
        # aiohttp.connector.BaseConnector.close_all()
