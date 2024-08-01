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
        self.consumer_group = consumer_group
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
        self._shutdown_event = asyncio.Event()
        self.session = None  # Placeholder for aiohttp.ClientSession

    async def on_event(self, partition_context, event):
        topic = event.properties.get(b"topic")
        try:
            if topic and (topic.decode("utf-8") in self.topics):
                logger.info(
                    f"TOPIC={topic.decode('utf-8')} | About to process event: {str(event)[:300]}"
                )
                event_result = await self.process_event(event)
                logger.info(f"Event processed. Result: {event_result}")
            else:
                logger.info(
                    f"Skipping topic [{topic.decode('utf-8')}]. Consumer group: {self.consumer_group}"
                )
        except Exception as e:
            logger.error(f"Exception occurred: {e}")
            logger.error("Detailed traceback information:")
            traceback.print_exc()
        await partition_context.update_checkpoint(event)

    async def process_event(self, event):
        """Override this method in subclasses to define event processing logic."""
        raise NotImplementedError("Must be implemented in subclass")

    async def start(self):
        logger.info(
            f"Starting consumer for topics: {self.topics} on group: {self.consumer_group}"
        )
        async with aiohttp.ClientSession() as session:
            self.session = session
            logger.info("aiohttp.ClientSession created")
            try:
                async with self.consumer:
                    await self.consumer.receive(
                        on_event=self.on_event, starting_position="-1", prefetch=1
                    )
                    await self._shutdown_event.wait()
            except asyncio.CancelledError:
                logger.warning("Consumer cancelled, closing consumer.")
                await self.stop()
            except Exception as e:
                logger.error(f"Error occurred while running consumer: {e}")
                logger.error("Detailed traceback information:")
                traceback.print_exc()
                await self.stop()

    async def stop(self):
        self._shutdown_event.set()
        await self.consumer.close()
        if self.session and not self.session.closed:
            await self.session.close()  # Ensure aiohttp.ClientSession is closed
            logger.info("aiohttp.ClientSession closed in stop")
        logger.info("Consumer stopped and resources released.")

    async def cleanup(self):
        await self.consumer.close()
        if self.session and not self.session.closed:
            await self.session.close()  # Ensure aiohttp.ClientSession is closed
            logger.info("aiohttp.ClientSession closed in cleanup")
        logger.info("Cleanup completed, consumer closed.")
