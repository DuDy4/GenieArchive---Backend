import json
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.repositories.persons_repository import PersonsRepository
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.dependencies.dependencies import (
    get_db_connection,
)


def test_person_data():
    conn = get_db_connection()
    persons_repository = PersonsRepository(conn=conn)
    uuid = "ThisIsATest"
    name = "Asaf Savich"
    company = "GenieAI"
    email = "asaf@genieai.ai"
    linkedin = "linkedin.com/in/asaf-savich"
    position = "CTO"
    timezone = ""
    person = PersonDTO(uuid, name, company, email, linkedin, position, timezone)
    person2 = PersonDTO.from_dict(
        {
            "uuid": "de19a684-7ded-4de5-b88b-6bc712e3497d",
            "name": "Adi Baltter",
            "company": "GenieAI",
            "email": "adi@genieai.ai",
            "linkedin": "https://www.linkedin.com/in/adibaltter/",
            "position": "CEO",
            "timezone": "",
        }
    )
    person3 = PersonDTO.from_dict(
        {
            "uuid": "e5d5726a-4293-49c5-ae5b-4b146a539e8b",
            "name": "Dan Gross",
            "company": "GenieAI",
            "email": "dan@genieai.ai",
            "linkedin": "https://www.linkedin.com/in/sansgross/",
            "position": "COO",
            "timezone": "",
        }
    )
    if not persons_repository.exists(uuid):
        persons_repository.insert(person)
    if not persons_repository.exists(person2.uuid):
        persons_repository.insert(person2)
    if not persons_repository.exists(person3.uuid):
        persons_repository.insert(person3)
    assert (
        persons_repository.exists(uuid)
        and persons_repository.exists(person2.uuid)
        and persons_repository.exists(person3.uuid)
    )
    print("Ownership test passed")


test_person_data()
