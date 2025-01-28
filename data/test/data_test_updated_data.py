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
from common.genie_logger import GenieLogger

logger = GenieLogger()


def test_new_contact():
    uuid = "00a64c11-da7d-45dc-bde5-dd6e30e5f0d2"
    persons_repository = PersonsRepository()
    personal_data_repository = PersonalDataRepository()
    person = persons_repository.get_person(uuid)
    personal_data_repository.update_news_last_updated_for_testing(person.email)
    personal_data = personal_data_repository.get_pdl_personal_data(uuid)
    data_to_send = {"person": person.to_dict(), "personal_data": personal_data, "force": True}
    event = GenieEvent(topic=Topic.NEW_PERSONAL_DATA, data=data_to_send)
    logger.set_tenant_id('org_RPLWQRTI8t7EWU1L')
    logger.set_user_id('google-oauth2|102736324632194671211')
    event.send()


def test_new_news():
    uuid = "00a64c11-da7d-45dc-bde5-dd6e30e5f0d2"
    event = GenieEvent(Topic.NEW_PERSONAL_NEWS, {"person_uuid": uuid, "force": True})
    logger.set_tenant_id('org_RPLWQRTI8t7EWU1L')
    logger.set_user_id('google-oauth2|102736324632194671211')
    event.send()


# test_new_contact()
test_new_news()
