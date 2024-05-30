from loguru import logger

from ..repositories.persons_repository import PersonsRepository
from ..postgres_connector import get_db_connection


def persons_repository() -> PersonsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return PersonsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
