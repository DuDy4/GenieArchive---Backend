import json
import os
import asyncio

from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
from common.genie_logger import GenieLogger
logger = GenieLogger()

from common.utils import env_utils

load_dotenv()

connection_str = env_utils.get("EVENTHUB_CONNECTION_STRING", "")
eventhub_name = env_utils.get("EVENTHUB_NAME", "")
producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_str, eventhub_name=eventhub_name
)


class GenieEvent:
    def __init__(self, topic, data: str | dict, scope, ctx_id=None):
        self.topic = topic
        self.data: str = self.ensure_json_format(data)
        self.scope = scope
        self.ctx_id = ctx_id

    def send(self):

        event_data_batch = producer.create_batch()
        event = EventData(body=self.data)
        event.properties = {"topic": self.topic, "scope": self.scope}
        logger.info(f"Events sent successfully [TOPIC={self.topic};SCOPE={self.scope}]")
        event_data_batch.add(event)

        # Send the batch
        producer.send_batch(event_data_batch)
        logger.info(f"Batch sent successfully [TOPIC={self.topic}]")

    def ensure_json_format(self, data):
        """
        Ensure that data is a valid JSON string. If it's a dictionary, convert it to a JSON string.
        """
        return json.dumps(self.convert_to_json(data))

    def convert_to_json(self, data):
        if isinstance(data, dict):
            try:
                return json.dumps(data)
            except (TypeError, ValueError) as e:
                logger.error(f"Failed to convert data to JSON: {e}")
                raise
        elif isinstance(data, str):
            try:
                json.loads(data)
                return data
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON string: {e}")
                raise
        else:
            logger.error("Data must be a dictionary or a JSON string")
            raise TypeError("Data must be a dictionary or a JSON string")
