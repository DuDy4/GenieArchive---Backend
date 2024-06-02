import os
import asyncio

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from dotenv import load_dotenv

load_dotenv()

connection_str = os.environ.get("EVENTHUB_CONNTECTION_STRING", "")
eventhub_name = os.environ.get("EVENTHUB_NAME", "")
consumer_group = '$Default'  # name of the default consumer group
storage_connection_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
blob_container_name = os.environ.get("BLOB_CONTAINER_NAME", "")

checkpoint_store = BlobCheckpointStore.from_connection_string(storage_connection_str, blob_container_name)
consumer = EventHubConsumerClient.from_connection_string(
    conn_str=connection_str, 
    consumer_group=consumer_group, 
    eventhub_name=eventhub_name, 
    checkpoint_store=checkpoint_store
)

async def on_event(partition_context, event):
    # Filter events based on the 'topic' property
    print("Received event: {}".format(event.body_as_str()))
    print("Received event: {}".format(event.properties))
    topic = event.properties.get(b'topic')
    if topic and topic.decode('utf-8') == 'topic1':
        print("Received event in topic 1: {}".format(event.body_as_str()))
    await partition_context.update_checkpoint(event)

async def main():
    async with consumer:
        await consumer.receive(on_event=on_event, starting_position="-1")


asyncio.run(main())
