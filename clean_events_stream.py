from data.data_common.events.genie_garbage_collector import GenieGarbageCollector
import asyncio

event_groups = [
    'apollo_consumer_group',
    'company_consumer_group',
    'email_manager_consumer_group',
    'langsmithconsumergroup',
    'meeting_manager_consumer_group',
    'news_consumer_group',
    'pdlconsumergroup',
    'personmanagerconsumergroup',
    'sales_material_consumer_group',
    'slack_consumer_group'
]

async def run_garbage_collectors():
    tasks = []
    for consumer_group in event_groups:
        garbage_collector = GenieGarbageCollector(consumer_group=consumer_group)
        tasks.append(garbage_collector.start())
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(run_garbage_collectors())