import os
import asyncio

from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

load_dotenv()

connection_str = os.environ.get("EVENTHUB_CONNTECTION_STRING", "")
eventhub_name = os.environ.get("EVENTHUB_NAME", "")
producer = EventHubProducerClient.from_connection_string(conn_str=connection_str, eventhub_name=eventhub_name)

# def register_event(event_data_batch, event_data):   
#     event_data_batch.add(event_data)


# async def send_event(event_data_batch):
#     async with producer:
#         await producer.send_batch(event_data_batch)

# event_data_batch = producer.create_batch()

# Add events with a custom property 'topic'
# event_data_batch.add(EventData(body='Hello World from topic1!').properties.update({'topic': 'topic1'}))
# event_data_batch.add(EventData(body='Hello World from topic2!').properties.update({'topic': 'topic2'}))

def send_event():
    event_data_batch = producer.create_batch()

    # Create EventData objects and set properties separately
    event1 = EventData(body='Hello World from topic1!')
    event1.properties = {'topic': 'topic1'}

    event2 = EventData(body='Hello World from topic2!')
    event2.properties = {'topic': 'topic2'}

    # Add events to the batch
    event_data_batch.add(event1)
    event_data_batch.add(event2)

    # Send the batch
    producer.send_batch(event_data_batch)
    print("Events sent successfully")

send_event()
producer.close()
#asyncio.run(send_event(event_data_batch))
