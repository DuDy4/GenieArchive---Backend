from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
from common.genie_logger import GenieLogger
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
        self.batch = None

    def start_batch(self):
        """Start a new batch, replacing any existing batch."""
        if self.batch is not None:
            logger.warning("Batch already exists. Replacing it with a new one.")
        self.batch = self.producer.create_batch()
        logger.info("Batch created successfully.")

    def add_event(self, event: GenieEvent):
        """Add an event to the current batch."""
        if not self.batch:
            self.start_batch()

        event_data = EventData(body=event.data)
        event_data.properties = {
            "topic": event.topic,
            "scope": event.scope,
            "ctx_id": event.ctx_id,
            "tenant_id": event.tenant_id,
        }

        try:
            self.batch.add(event_data)
            logger.info(f"Event added to batch [TOPIC={event.topic}]")
        except ValueError as e:
            logger.error(f"Failed to add event to batch: {e}")
            raise

    def send_batch(self):
        """Send the current batch."""
        if not self.batch:
            raise RuntimeError("No batch to send. Call start_batch() and add events first.")

        self.producer.send_batch(self.batch)
        logger.info("Batch sent successfully.")
        self.batch = None