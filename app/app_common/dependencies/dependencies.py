from loguru import logger

from ..repositories.contacts_repository import ContactsRepository
from ..repositories.interactions_repository import InteractionsRepository
from ..repositories.salesforce_users_repository import SalesforceUsersRepository
from ..postgres_connector import get_db_connection


def contacts_repository() -> ContactsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return ContactsRepository(conn=conn)
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
