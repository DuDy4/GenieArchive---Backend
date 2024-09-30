import os
import asyncio
import sys
import traceback

import aiohttp
import httpx
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub import TransportType
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from common.genie_logger import GenieLogger
from azure.monitor.opentelemetry import configure_azure_monitor
from dotenv import load_dotenv
from fastapi import FastAPI
from threading import Thread
import uvicorn

load_dotenv()
configure_azure_monitor()
logger = GenieLogger()

from common.utils import env_utils


class GenieConsumer:
    active_clients = set()

    def __init__(self, topics, consumer_group="$Default"):
        self.consumer_group = consumer_group
        connection_str = env_utils.get("EVENTHUB_CONNECTION_STRING", "")
        eventhub_name = env_utils.get("EVENTHUB_NAME", "")
        storage_connection_str = env_utils.get("AZURE_STORAGE_CONNECTION_STRING", "")
        blob_container_name = env_utils.get("BLOB_CONTAINER_NAME", "")
        checkpoint_store = BlobCheckpointStore.from_connection_string(
            storage_connection_str, blob_container_name
        )
        self.consumer = EventHubConsumerClient.from_connection_string(
            conn_str=connection_str,
            consumer_group=consumer_group,
            eventhub_name=eventhub_name,
            checkpoint_store=checkpoint_store,
            transport_type=TransportType.AmqpOverWebsocket,
        )
        self.topics = topics
        self._shutdown_event = asyncio.Event()
        self.is_healthy = True  

        health_check_port = env_utils.get("HEALTH_CHECK_PORT")
        if health_check_port:
            self.start_health_check_server()

    def start_health_check_server(self):
        app = FastAPI()

        @app.get("/health")
        async def health_check():
            if self.is_healthy:
                return {"status": "healthy"}
            else:
                return {"status": "unhealthy"}, 500

        # Run FastAPI in a separate thread
        thread = Thread(target=self.run_server, args=(app,))
        thread.daemon = True
        thread.start()

    
    def run_server(self, app):
        # Get the port from an environment variable, defaulting to 8000 if not set
        health_check_port = env_utils.get("HEALTH_CHECK_PORT")
        if health_check_port:
            logger.info(f"Starting health check server on port {health_check_port}")
            uvicorn.run(app, host="0.0.0.0", port=int(health_check_port))


    async def on_event(self, partition_context, event):
        topic = event.properties.get(b"topic")
        try:
            if topic and (topic.decode("utf-8") in self.topics):
                decoded_topic = topic.decode("utf-8")
                if b"ctx_id" in event.properties:
                    ctx_id = event.properties.get(b"ctx_id")
                    if ctx_id:
                        decoded_ctx_id = ctx_id.decode("utf-8")
                        logger.bind_context(decoded_ctx_id)
                if b"tenant_id" in event.properties:
                    tenant_id = event.properties.get(b"tenant_id")
                    if tenant_id:
                        decoded_tenant_id = tenant_id.decode("utf-8")
                        logger.set_tenant_id(decoded_tenant_id)
                logger.set_topic(decoded_topic)
                logger.info(f"TOPIC={decoded_topic} | About to process event: {str(event)[:300]}")
                event_result = await self.process_event(event)
                logger.info(f"Event processed. Result: {event_result}")
            else:
                topic = topic.decode("utf-8") if topic else None
                logger.info(f"Skipping topic [{topic}]. Consumer group: {self.consumer_group}")
        except Exception as e:
            logger.error(f"Exception occurred: {e}")
            logger.error("Detailed traceback information:")
            traceback.print_exc()
        await partition_context.update_checkpoint(event)

    async def process_event(self, event):
        """Override this method in subclasses to define event processing logic."""
        raise NotImplementedError("Must be implemented in subclass")

    async def start(self):
        logger.info(f"Starting consumer for topics: {self.topics} on group: {self.consumer_group}")
        async with httpx.AsyncClient() as client:
            self.client = client
            GenieConsumer.active_clients.add(client)
            try:
                async with self.consumer:
                    await self.consumer.receive(on_event=self.on_event, starting_position="-1", prefetch=1)
                    await self._shutdown_event.wait()
            except asyncio.CancelledError:
                logger.warning("Consumer cancelled, closing consumer.")
            except Exception as e:
                logger.error(f"Error occurred while running consumer: {e}")
                logger.error("Detailed traceback information:")
                traceback.print_exc()
            finally:
                await self.stop()

    async def close_client(self):
        if hasattr(self, "client") and self.client and not self.client.is_closed:
            await self.client.aclose()
            logger.info("httpx.AsyncClient closed")

    async def close_aiohttp_sessions(self):
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if isinstance(attr, aiohttp.ClientSession):
                if not attr.closed:
                    await attr.close()
                    logger.info(f"Closed aiohttp ClientSession: {attr_name}")

    async def stop(self):
        self._shutdown_event.set()
        if hasattr(self, "consumer"):
            await self.consumer.close()
        if hasattr(self, "client"):
            await self.close_client()
        await self.close_aiohttp_sessions()
        logger.info("Consumer stopped and resources released.")

    @classmethod
    async def cleanup(cls):
        for consumer in list(cls.active_clients):
            if isinstance(consumer, GenieConsumer):
                await consumer.stop()
        cls.active_clients.clear()
        logger.info("Cleanup completed, all consumers closed.")

    async def main(self):
        try:
            await self.start()
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt, stopping consumers.")
            await self.cleanup()
