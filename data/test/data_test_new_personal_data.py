from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.repositories.personal_data_repository import PersonalDataRepository

persons_repository = PersonsRepository()
personal_data_repository = PersonalDataRepository()

uuid = "e0527656-6c01-4a1f-862e-58b4758ba399"
person = persons_repository.get_person(uuid)
personal_data = personal_data_repository.get_pdl_personal_data(uuid)
user_id = "google-oauth2|117881894742800328091"
tenant_id = "org_N1U4UsHtTfESJPYB"

event = GenieEvent(
    topic=Topic.NEW_PERSONAL_DATA,
    data={
        "person": person.to_dict(),
        "personal_data": personal_data,
        "user_id": user_id,
        "tenant_id": tenant_id
    })
event.send()

