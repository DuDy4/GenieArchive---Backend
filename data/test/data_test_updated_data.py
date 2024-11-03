import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.repositories.personal_data_repository import (
    PersonalDataRepository,
)
from data.data_common.utils.postgres_connector import get_db_connection

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic


def test_new_contact():

    uuid = "98f0a054-36f7-4862-bfb2-23dd694940e0"
    persons_repository = PersonsRepository(get_db_connection())
    # personal_data_repository = PersonalDataRepository(get_db_connection())
    person = persons_repository.get_person(uuid)
    # personal_data = personal_data_repository.get_pdl_personal_data(uuid)
    data_to_send = {"person": person.to_dict()}
    event = GenieEvent(topic=Topic.NEW_PERSONAL_DATA, data=data_to_send)
    event.send()


test_new_contact()
