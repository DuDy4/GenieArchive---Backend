import sys
import os

from data.test.data_test_meetings import data_to_transfer

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic
from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.dependencies.dependencies import meetings_repository, persons_repository

meetings_repository = meetings_repository()
persons_repository = persons_repository()


def test_new_updated_pdl_data(tenant_id, person_uuid):
    person = persons_repository.get_person(person_uuid)
    data_to_send = {
        "tenant_id": tenant_id,
        "person": person.to_dict(),
    }
    event = GenieEvent(topic=Topic.APOLLO_UP_TO_DATE_ENRICHED_DATA, data=data_to_send)
    event.send()


test_new_updated_pdl_data("org_A0Qv1tDwcI20mwHe", "9e048fed-46f6-410a-8459-2b1594e809d5")
