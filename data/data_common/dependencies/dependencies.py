from loguru import logger

from data.data_common.repositories.contacts_repository import ContactsRepository
from data.data_common.repositories.interactions_repository import InteractionsRepository
from data.data_common.repositories.salesforce_users_repository import (
    SalesforceUsersRepository,
)
from ..postgres_connector import get_db_connection
from ..repositories.personal_data_repository import PersonalDataRepository
from ..repositories.persons_repository import PersonsRepository
from ..repositories.profiles_repository import ProfilesRepository
from ..salesforce.salesforce_event_handler import SalesforceEventHandler


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


def salesforce_users_repository() -> SalesforceUsersRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return SalesforceUsersRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None


def salesforce_event_handler() -> SalesforceEventHandler:
    return SalesforceEventHandler(
        contacts_repository=contacts_repository(),
    )
