from loguru import logger

from app_common.repositories.tenants_repository import TenantsRepository

from ..postgres_connector import get_db_connection


def tenants_repository() -> TenantsRepository:
    conn = get_db_connection()  # Establish the database connection
    try:
        with conn:
            return TenantsRepository(conn=conn)
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        return None
