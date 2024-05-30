from ..repositories.persons_repository import PersonsRepository
from ..postgres_connector import conn


def persons_repository() -> PersonsRepository:
    return PersonsRepository(conn=conn)
