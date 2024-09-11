import sys
import os

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

from data.data_common.utils.persons_utils import (
    create_person_from_pdl_personal_data,
    create_person_from_apollo_personal_data,
)

logger = GenieLogger()

persons_repository = persons_repository()
personal_data_repository = personal_data_repository()
companies_repository = companies_repository()


def get_all_personal_data_with_missing_attributes():
    all_personal_data_uuid = personal_data_repository.get_all_personal_data_with_missing_attributes()
    return all_personal_data_uuid


def update_personal_data_attributes(uuids: list):
    for uuid in uuids:
        try:
            new_person = persons_repository.get_person(uuid)
            if not new_person:
                logger.error(f"Person with uuid {uuid} not found")
                new_person = persons_repository.find_person_by_email(personal_data_repository.get_email(uuid))
                if not new_person:
                    logger.error(f"Person with email {personal_data_repository.get_email(uuid)} not found")
                    continue
                personal_data_repository.update_uuid(uuid, new_person.uuid)
            if new_person.name:
                personal_data_repository.update_name_in_personal_data(new_person.uuid, new_person.name)
                logger.info(f"Updated name for {new_person.email} to {new_person.name}")
            if new_person.linkedin:
                personal_data_repository.update_linkedin_url(new_person.uuid, new_person.linkedin)
                logger.info(f"Updated LinkedIn URL for {new_person.email} to {new_person.linkedin}")
            logger.info(f"Updated LinkedIn URL for {new_person.email} to {new_person.linkedin}")
        except Exception as e:
            logger.error(f"Error updating personal data for {uuid}: {e}")
            break
    return


def clean_database_duplicates():
    # Fetch all duplicates by email
    duplicates = personal_data_repository.get_duplicates_by_email()
    logger.info(f"Found {len(duplicates)} duplicates: {duplicates}")

    # Group duplicates by email
    duplicates_by_email = {}
    for duplicate in duplicates:
        uuid, name, email, linkedin_url = duplicate
        if email not in duplicates_by_email:
            duplicates_by_email[email] = []
        duplicates_by_email[email].append(duplicate)

    # Process each email group, keeping the most complete record
    for email, records in duplicates_by_email.items():
        # Sort records by completeness (prefer non-NULL name and linkedin_url)
        records.sort(key=lambda r: (r[1] is None, r[3] is None))

        # Keep the first record (most complete) and delete the others
        most_complete_record = records[0]  # This is the record we will keep
        records_to_delete = records[1:]  # These are the records to delete

        logger.info(f"Keeping record {most_complete_record} for email {email}")
        for record in records_to_delete:
            uuid_to_delete = record[0]
            logger.info(f"Deleting duplicate record: {record}")
            personal_data_repository.delete(uuid_to_delete)


clean_database_duplicates()

all_personal_data_uuid = get_all_personal_data_with_missing_attributes()
logger.info(f"Persons without LinkedIn URL: {len(all_personal_data_uuid)}")
update_personal_data_attributes(all_personal_data_uuid)
