from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.repositories.persons_repository import PersonsRepository

persons_repository = PersonsRepository()

person = persons_repository.get_person("6b9c81f0-c648-4dfe-ada2-be4160748c4f")
event = GenieEvent(topic=Topic.NEW_PERSONAL_NEWS, data={"person_id": "994a340c-6710-4826-9752-4e88fdceadfa", "tenant_id": "org_vaeQAccrEsB4EAks", "force": True})
event.send()

