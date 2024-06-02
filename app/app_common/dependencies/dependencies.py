from loguru import logger

from ..repositories.persons_repository import PersonsRepository
from ..repositories.interactions_repository import InteractionsRepository
from ..repositories.salesforce_users_repository import SalesforceUsersRepository
from ..postgres_connector import get_db_connection


def persons_repository() -> PersonsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return PersonsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None


def interactions_repository() -> InteractionsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return InteractionsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None


def salesforce_users_repository() -> SalesforceUsersRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return SalesforceUsersRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
