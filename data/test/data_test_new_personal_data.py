from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.repositories.persons_repository import PersonsRepository

persons_repository = PersonsRepository()

person = persons_repository.get_person("6b9c81f0-c648-4dfe-ada2-be4160748c4f")
event = GenieEvent(topic=Topic.NEW_PERSONAL_DATA, data={"person": person.to_dict(), "tenant_id": "org_vaeQAccrEsB4EAks"})
event.send()

