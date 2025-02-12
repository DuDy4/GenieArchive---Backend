import sys
import os

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from common.genie_logger import GenieLogger

from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.pdl_consumer import PDLConsumer
from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.repositories.personal_data_repository import PersonalDataRepository
from data.data_common.dependencies.dependencies import (
    get_db_connection,
    persons_repository,
    personal_data_repository,
    companies_repository,
)

from data.data_common.services.person_builder_service import (
    create_person_from_pdl_personal_data,
    create_person_from_apollo_personal_data,
)

logger = GenieLogger()

persons_repository = persons_repository()
personal_data_repository = personal_data_repository()
companies_repository = companies_repository()


def get_all_uuids_that_did_not_try_apollo():
    all_personal_data_uuid = personal_data_repository.get_all_uuids_without_apollo()
    return all_personal_data_uuid


def fetch_apollo_data(uuids: list):
    for uuid in uuids:
        try:
            person = persons_repository.get_person(uuid)
            if not person:
                logger.error(f"Person with uuid {uuid} not found")
                continue
            if not person.email:
                logger.error(f"Person with uuid {uuid} has no email")
                continue
            event = GenieEvent(topic=Topic.APOLLO_NEW_PERSON_TO_ENRICH, data={"person": person.to_dict()})
            event.send()
            logger.info(f"Sent event for {person.email}")
        except Exception as e:
            logger.error(f"Error sending event for {uuid}: {e}")
            break


# all_personal_data_uuid = get_all_uuids_that_did_not_try_apollo()
# logger.info(f"Persons without apollo_data: {len(all_personal_data_uuid)}")
# fetch_apollo_data(all_personal_data_uuid)
