from data.data_common.repositories.interactions_repository import InteractionsRepository

from ..utils.postgres_connector import get_db_connection
from ..repositories.personal_data_repository import PersonalDataRepository
from ..repositories.persons_repository import PersonsRepository
from ..repositories.profiles_repository import ProfilesRepository
from ..repositories.tenants_repository import TenantsRepository
from ..repositories.meetings_repository import MeetingsRepository
from ..repositories.google_creds_repository import GoogleCredsRepository
from ..repositories.ownerships_repository import OwnershipsRepository
from ..repositories.hobbies_repository import HobbiesRepository
from ..repositories.companies_repository import CompaniesRepository
from common.genie_logger import GenieLogger

logger = GenieLogger()


def tenants_repository() -> TenantsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return TenantsRepository(conn=conn)
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


def companies_repository() -> CompaniesRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return CompaniesRepository(conn=conn)
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


def meetings_repository() -> MeetingsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return MeetingsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None


def google_creds_repository() -> GoogleCredsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return GoogleCredsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None


def ownerships_repository() -> OwnershipsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return OwnershipsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None


def hobbies_repository() -> HobbiesRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return HobbiesRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
