import os
import asyncio
import traceback
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from loguru import logger


class GenieConsumer:
    def __init__(self, topics):
        connection_str = os.environ.get("EVENTHUB_CONNTECTION_STRING", "")
        eventhub_name = os.environ.get("EVENTHUB_NAME", "")
        consumer_group = "$Default"
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
        )
        self.topics = topics

    async def on_event(self, partition_context, event):
        topic = event.properties.get(b"topic")
        try:
            if topic and topic.decode("utf-8") in self.topics:
                event_result = await self.process_event(event)
                logger.info(f"Event processed. Result: {event_result}")
        except Exception as e:
            logger.info("Exception occurred:", e)
            logger.info("Detailed traceback information:")
            traceback.print_exc()
        await partition_context.update_checkpoint(event)

    async def process_event(self, event):
        """Override this method in subclasses to define event processing logic."""
        raise NotImplementedError("Must be implemented in subclass")

    async def start(self):
        async with self.consumer:
            await self.consumer.receive(on_event=self.on_event, starting_position="-1")

    def run(self):
        asyncio.run(self.start())
