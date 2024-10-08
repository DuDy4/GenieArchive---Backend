from data.data_common.repositories.interactions_repository import InteractionsRepository

from ..utils.postgres_connector import get_db_connection, connection_pool
from ..repositories.personal_data_repository import PersonalDataRepository
from ..repositories.persons_repository import PersonsRepository
from ..repositories.profiles_repository import ProfilesRepository
from ..repositories.tenants_repository import TenantsRepository
from ..repositories.meetings_repository import MeetingsRepository
from ..repositories.google_creds_repository import GoogleCredsRepository
from ..repositories.ownerships_repository import OwnershipsRepository
from ..repositories.hobbies_repository import HobbiesRepository
from ..repositories.companies_repository import CompaniesRepository
from ..repositories.stats_repository import StatsRepository
from ..repositories.badges_repository import BadgesRepository
from common.genie_logger import GenieLogger

logger = GenieLogger()


def tenants_repository() -> TenantsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        return TenantsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
    finally:
        if conn:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool in tenants_repository")


def stats_repository() -> StatsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        return StatsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
    finally:
        if conn:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool in stats_repository")


def badges_repository() -> BadgesRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        return BadgesRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
    finally:
        if conn:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool in badges_repository")


def interactions_repository() -> InteractionsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        return InteractionsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
    finally:
        if conn:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool in interactions_repository")


def companies_repository() -> CompaniesRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        return CompaniesRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
    finally:
        if conn:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool in companies_repository")


def personal_data_repository() -> PersonalDataRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        return PersonalDataRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
    finally:
        if conn:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool in personal_data_repository")


def profiles_repository() -> ProfilesRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        return ProfilesRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
    finally:
        if conn:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool in profiles_repository")


def persons_repository() -> PersonsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        return PersonsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
    finally:
        if conn:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool in persons_repository")


def meetings_repository() -> MeetingsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        return MeetingsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
    finally:
        if conn:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool in meetings_repository")


def google_creds_repository() -> GoogleCredsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        return GoogleCredsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
    finally:
        if conn:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool in google_creds_repository")


def ownerships_repository() -> OwnershipsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        return OwnershipsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
    finally:
        if conn:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool in ownerships_repository")


def hobbies_repository() -> HobbiesRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        return HobbiesRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
    finally:
        if conn:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool in hobbies_repository")
