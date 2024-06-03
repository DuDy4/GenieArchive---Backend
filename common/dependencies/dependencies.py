from loguru import logger

from common.utils.postgres_connector import get_db_connection
from common.repositories.personal_data_repository import PersonalDataRepository
from common.repositories.profiles_repository import ProfilesRepository
from common.repositories.persons_repository import PersonsRepository


def personal_data_repository() -> PersonalDataRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return PersonalDataRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None


def profiles_repository() -> ProfilesRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return ProfilesRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None


def persons_repository() -> PersonsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return PersonsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
