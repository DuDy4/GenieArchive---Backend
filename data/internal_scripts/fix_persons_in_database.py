import sys
import os

from common.utils import email_utils
from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.topics import Topic

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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

from data.data_common.utils.persons_utils import (
    create_person_from_pdl_personal_data,
    create_person_from_apollo_personal_data,
)

logger = GenieLogger()

persons_repository = persons_repository()
personal_data_repository = personal_data_repository()
companies_repository = companies_repository()


def get_all_persons_without_linkedin_url():
    all_persons = persons_repository.get_all_persons_with_missing_attribute()
    return all_persons


def update_persons_with_linkedin_url(persons: list[PersonDTO]):
    new_persons = []
    for person in persons:
        new_person = create_person_from_pdl_personal_data(person)
        logger.info(f"Person from pdl: {new_person}")
        if not new_person:
            new_person = create_person_from_apollo_personal_data(person)
            logger.info(f"Person from apollo: {new_person}")
        if new_person:
            new_persons.append(new_person)
            persons_repository.save_person(new_person)
            personal_data_repository.update_name_in_personal_data(new_person.uuid, new_person.name)
            personal_data_repository.update_linkedin_url(new_person.uuid, new_person.linkedin)
            logger.info(f"Updated LinkedIn URL for {new_person.email} to {new_person.linkedin}")
    return new_persons


persons = get_all_persons_without_linkedin_url()
logger.info(f"Persons without LinkedIn URL: {persons}")
persons = update_persons_with_linkedin_url(persons)
logger.info(f"Persons without LinkedIn URL: {persons}")
