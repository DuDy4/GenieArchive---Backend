import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.dependencies.dependencies import meetings_repository, persons_repository

from common.genie_logger import GenieLogger

meetings_repository = meetings_repository()
persons_repository = persons_repository()

logger = GenieLogger()

logger.set_tenant_id("org_N1U4UsHtTfESJPYB")
logger.bind_context()

def test_new_updated_pdl_data(user_id, person_uuid):
    person = persons_repository.get_person(person_uuid)
    data_to_send = {
        "user_id": user_id,
        "person": person.to_dict(),
    }
    event = GenieEvent(topic=Topic.APOLLO_UP_TO_DATE_ENRICHED_DATA, data=data_to_send)
    event.send()


test_new_updated_pdl_data("google-oauth2|117881894742800328091", "20455e8d-2ea8-45e3-af2c-f1370ba438e2")
