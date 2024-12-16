from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.repositories.persons_repository import PersonsRepository

persons_repository = PersonsRepository()

event = GenieEvent(topic=Topic.NEW_PERSONAL_NEWS, data={"person_id": "47ffa6b4-1c22-41cd-9724-e62d04701a6a", "tenant_id": "org_N1U4UsHtTfESJPYB", "force": True})
event.send()

