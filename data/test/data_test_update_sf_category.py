from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from common.genie_logger import GenieLogger

logger = GenieLogger()

logger.bind_context()
logger.set_user_id('test_user_id')
logger.set_tenant_id('test_tenant_id')



event = GenieEvent(
    topic=Topic.FINISHED_NEW_PROFILE,
    data={'profile_uuid': 'b31a78ae-b4cf-4e34-9bc4-288f7b4d8f06'}
)



event.send()