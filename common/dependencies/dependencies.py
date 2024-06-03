from loguru import logger

from common.utils.postgres_connector import get_db_connection
from common.repositories.personal_data_repository import PersonalDataRepository


def profiles_repository() -> PersonalDataRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return PersonalDataRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
