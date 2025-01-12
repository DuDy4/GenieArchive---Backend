from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


event = GenieEvent(
    topic=Topic.FINISHED_NEW_PROFILE,
    data={'profile_uuid': 'b31a78ae-b4cf-4e34-9bc4-288f7b4d8f06'}
)

event.send()