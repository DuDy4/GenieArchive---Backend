import json
import os

from azure.eventhub import EventHubProducerClient, EventData
from common.genie_logger import GenieLogger
from data.data_common.dependencies.dependencies import statuses_repository
from common.utils.event_utils import extract_object_id
from common.utils import env_utils

logger = GenieLogger()



connection_str = env_utils.get("EVENTHUB_CONNECTION_STRING", "")
eventhub_name = env_utils.get("EVENTHUB_NAME", "")
producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)


class GenieEvent:
    def __init__(self, topic, data: str | dict, scope="public", ctx_id=None, cty_id=None):
        self.topic = topic
        self.data: str = self.ensure_json_format(data)
        self.scope = scope
        ctx_id = ctx_id if ctx_id else logger.get_ctx_id()
        self.ctx_id = ctx_id
        cty_id = cty_id if cty_id else logger.get_cty_id()
        self.cty_id = cty_id if cty_id else None
        self.tenant_id = logger.get_tenant_id() or (json.loads(data).get("tenant_id") if isinstance(data, str) else data.get("tenant_id"))
        self.previous_topic = logger.get_topic() or (json.loads(data).get("previous_topic") if isinstance(data, str) else data.get("previous_topic"))
        self.statuses_repository = statuses_repository()

    def send(self):
        event_data_batch = producer.create_batch()
        event = EventData(body=self.data)
        event.properties = {"topic": self.topic, "scope": self.scope, "ctx_id": self.ctx_id, "tenant_id" : self.tenant_id}
        if self.previous_topic:
            event.properties["previous_topic"] = self.previous_topic
        if self.cty_id:
            event.properties["cty_id"] = self.cty_id
        logger.info(f"Events sent successfully [TOPIC={self.topic};SCOPE={self.scope};TENANT_ID={self.tenant_id}]")
        event_data_batch.add(event)

        # Send the batch
        send_timeout = 60  # Set timeout to 60 seconds
        producer.send_batch(event_data_batch, timeout=send_timeout)
        logger.info(f"Batch sent successfully [TOPIC={self.topic}]")
        producer.close()

        object_id, object_type = extract_object_id(self.data)
        if not object_id:
            return
        self.statuses_repository.start_status(ctx_id=self.ctx_id, object_id=object_id, object_type=object_type,
                                              tenant_id=self.tenant_id, previous_event_topic=self.previous_topic,
                                              next_event_topic=self.topic)

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


