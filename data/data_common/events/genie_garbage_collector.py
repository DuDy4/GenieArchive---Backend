import asyncio
import traceback
import httpx
from data.data_common.events.genie_consumer import GenieConsumer
from common.genie_logger import GenieLogger


logger = GenieLogger()


class GenieGarbageCollector(GenieConsumer):
    def __init__(self, consumer_group="$Default"):
        """
        Initialize the GenieGarbageCollector.
        """
        # Pass wildcard `*` for topics to process all events in the consumer group
        super().__init__(topics=["*"], consumer_group=consumer_group)

    async def process_event(self, event):
        """
        Log the event as garbage collected and mark it as processed.
        """
        topic = event.properties.get(b"topic")
        topic = topic.decode("utf-8") if topic else "unknown"
        logger.info(f"Garbage collected event from topic: {topic}")
        return f"Event from topic {topic} garbage collected"

    async def start(self):
        """
        Override start to ensure all events are handled without topic filtering.
        """
        logger.info("Starting GenieGarbageCollector to process all events.")
        async with httpx.AsyncClient() as client:
            self.client = client
            GenieConsumer.active_clients.add(client)
            try:
                async with self.consumer:
                    await self.consumer.receive(
                        on_event=self.on_event,
                        starting_position="@latest",  # Start from the latest events
                        prefetch=5  # Adjust prefetch for performance if needed
                    )
                    await self._shutdown_event.wait()
            except asyncio.CancelledError:
                logger.warning("GenieGarbageCollector cancelled, closing consumer.")
            except Exception as e:
                logger.error(f"Error occurred while running GenieGarbageCollector: {e}")
                logger.error("Detailed traceback information:")
                traceback.print_exc()
            finally:
                await self.stop()

    async def on_event(self, partition_context, event):
        """
        Process each event and update checkpoint to confirm handling.
        """
        topic = event.properties.get(b"topic", b"unknown").decode("utf-8")
        try:
            logger.info(f"Processing event from topic: {topic}")
            await self.process_event(event)
        except Exception as e:
            logger.error(f"Exception occurred while processing event from topic {topic}: {e}")
            logger.error("Detailed traceback information:")
            traceback.print_exc()
        finally:
            # Confirm the event is handled
            await partition_context.update_checkpoint(event)
            logger.info(f"Event {topic} marked as handled, in consumer group: {self.consumer_group}")

# Example usage
if __name__ == "__main__":
    collector = GenieGarbageCollector(consumer_group="$Default")

    async def run_collector():
        await collector.main()

    asyncio.run(run_collector())
