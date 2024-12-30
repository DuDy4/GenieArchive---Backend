import asyncio

from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
from common.genie_logger import GenieLogger
from common.utils.event_utils import extract_object_id
from data.data_common.dependencies.dependencies import statuses_repository
from data.data_common.events.genie_event import GenieEvent

logger = GenieLogger()
from common.utils import env_utils

load_dotenv()

connection_str = env_utils.get("EVENTHUB_CONNECTION_STRING", "")
eventhub_name = env_utils.get("EVENTHUB_NAME", "")
producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)


class EventHubBatchManager:
    def __init__(self):
        self.producer = producer
        self.events = []
        self.batch = None
        self.batch_task = None  # Initialize batch_task as None
        self.statuses_repository = statuses_repository()

        try:
            # Safely get the running loop and start the batch task if a loop exists
            loop = asyncio.get_running_loop()
            self.batch_task = loop.create_task(self.start_batch())
        except RuntimeError:
            # No running loop; log and defer task creation
            logger.warning("No running event loop. Batch task creation deferred.")

    async def ensure_batch_task(self):
        """Ensure the batch task is started."""
        if not self.batch_task:
            logger.info("Starting batch task now as it was previously deferred.")
            self.batch_task = asyncio.create_task(self.start_batch())  # Create the task if not already created
        await self.batch_task

    async def start_batch(self):
        """Start a new batch asynchronously."""
        try:
            self.batch = await asyncio.to_thread(self.producer.create_batch)
            logger.info(f"Batch created successfully: {self.batch}")
        except Exception as e:
            logger.error(f"Failed to create batch: {e}")
            raise

    def queue_event(self, event: GenieEvent):
        """Queue an event for eventual batch processing."""
        event_data = EventData(body=event.data)
        event_data.properties = {
            "topic": event.topic,
            "scope": event.scope,
            "ctx_id": event.ctx_id,
            "tenant_id": event.tenant_id,
        }
        if event.cty_id:
            event_data.properties["cty_id"] = event.cty_id
        self.events.append(event_data)
        logger.info(f"Event queued [TOPIC={event.topic}]")

    async def add_events_to_batch(self):
        """Wait for batch creation and add queued events."""
        await self.ensure_batch_task()  # Ensure batch task is initialized and completed

        for event in self.events:
            try:
                self.batch.add(event)
                logger.info(f"Event added to batch [TOPIC={event.properties.get('topic')}]")
            except ValueError as e:
                logger.error(f"Failed to add event to batch: {e}")
                raise

        # Clear the events list after adding to the batch
        self.events = []

    async def send_batch(self):
        """Send the current batch after ensuring all events are added."""
        await self.add_events_to_batch()

        if not self.batch:
            raise RuntimeError("No batch to send. Ensure batch creation succeeded.")

        statuses_task = asyncio.create_task(self.update_status(self.events[:]))

        await asyncio.to_thread(self.producer.send_batch, self.batch)
        logger.info("Batch sent successfully.")
        self.batch = None
        self.batch_task = None  # Reset for future batches
        await statuses_task

    async def update_status(self, events):
        for event in events:
            object_id, object_type = extract_object_id(event.data)
            if not object_id:
                continue
            self.statuses_repository.start_status(ctx_id=event.ctx_id, object_id=object_id, object_type=object_type,
                                                  tenant_id=event.tenant_id, previous_event_topic=event.previous_topic,
                                                  next_event_topic=event.topic)
        logger.info("Statuses updated successfully.")


