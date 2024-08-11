import os

from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
from common.utils import env_utils
from topics import Topic

load_dotenv()

connection_str = env_utils.get("EVENTHUB_CONNTECTION_STRING", "")
eventhub_name = env_utils.get("EVENTHUB_NAME", "")
producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_str, eventhub_name=eventhub_name
)


def send_event():
    event_data_batch = producer.create_batch()

    # Create EventData objects and set properties separately
    event1 = EventData(body="Hello World from topic1!")
    event1.properties = {"topic": "topic1"}

    event2 = EventData(body="Hello World from topic2!")
    event2.properties = {"topic": Topic.NEW_CONTACT, "scope": "public"}

    # Add events to the batch
    event_data_batch.add(event1)
    event_data_batch.add(event2)

    # Send the batch
    producer.send_batch(event_data_batch)
    print("Events sent successfully")


send_event()
producer.close()
# asyncio.run(send_event(event_data_batch))
