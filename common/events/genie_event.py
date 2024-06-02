import os
import asyncio

from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
from common.events.topics import Topic

load_dotenv()

connection_str = os.environ.get("EVENTHUB_CONNTECTION_STRING", "")
eventhub_name = os.environ.get("EVENTHUB_NAME", "")
producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)

class GenieEvent:
    def __init__(self, topic, data, scope):
        self.topic = topic
        self.data = data
        self.scope = scope


    def send(self):
        event_data_batch = producer.create_batch()

        event = EventData(body=self.data)
        print(f"Events sent successfully [TOPIC={self.topic};SCOPE={self.scope}]")
        event_data_batch.add(event)

        # Send the batch
        producer.send_batch(event_data_batch)
        print(f"Events sent successfully [TOPIC={self.topic}]")

    