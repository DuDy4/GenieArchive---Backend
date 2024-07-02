import traceback
import uuid
from typing import Optional, Union, List

from app_common.utils.str_utils import get_uuid4

from loguru import logger


class TenantsRepository:
    def __init__(self, conn):
        self.conn = conn
        # self.cursor = conn.cursor()
        self.create_table_if_not_exists()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS tenants (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            tenant_id VARCHAR,
            user_name VARCHAR,
            email VARCHAR,
            salesforce_client_url VARCHAR,
            salesforce_refresh_token VARCHAR,
            salesforce_access_token VARCHAR
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
        except Exception as error:
            logger.error("Error creating table:", error)
            # self.conn.rollback()

    def insert(self, tenant: dict):
        insert_query = """
        INSERT INTO tenants (uuid, tenant_id, user_name, email, salesforce_client_url, salesforce_refresh_token, salesforce_access_token)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        if self.exists(tenant.get("tenantId"), tenant.get("name")):
            logger.info("User already exists in database")
            return

        uuid = get_uuid4()

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    insert_query,
                    (
                        tenant.get("uuid", uuid),
                        tenant.get("tenantId"),
                        tenant.get("name"),
                        tenant.get("email"),
                        tenant.get("salesforce_client_url"),
                        tenant.get("salesforce_refresh_token"),
                        tenant.get("salesforce_access_token"),
                    ),
                )
                self.conn.commit()
                return uuid
        except Exception as error:
            logger.error("Error inserting tenant:", error)
            logger.error(traceback.format_exc())

    def exists(self, tenant_id: str, user_name: str):
        exists_query = """
        SELECT uuid FROM tenants WHERE tenant_id = %s and user_name = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, (tenant_id, user_name))
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as error:
            logger.error("Error checking if tenant exists:", error)
            logger.error(traceback.format_exc())

    def update_salesforce_credentials(
        self, tenant_id: str, salesforce_credentials: dict
    ):
        update_query = """
        UPDATE tenants SET salesforce_client_url = %s, salesforce_refresh_token = %s, salesforce_access_token = %s
        WHERE tenant_id = %s
        """
        try:
            with self.conn.cursor() as cursor:
                logger.debug(f"Updating tenant credentials: {salesforce_credentials}")
                cursor.execute(
                    update_query,
                    (
                        salesforce_credentials.get("client_url"),
                        salesforce_credentials.get("refresh_token"),
                        salesforce_credentials.get("access_token"),
                        tenant_id,
                    ),
                )
                logger.debug(f"about to commit the update")
                self.conn.commit()
        except Exception as error:
            logger.error("Error updating tenant credentials:", error)
            logger.error(traceback.format_exc())

    def get_salesforce_credentials(self, tenant_id: str) -> Optional[dict]:
        select_query = """
        SELECT salesforce_client_url, salesforce_refresh_token, salesforce_access_token
        FROM tenants
        WHERE tenant_id = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (tenant_id,))
                result = cursor.fetchone()
                if result[2] is not None:
                    return {"salesforce_access_token": result[2]}
        except Exception as error:
            logger.error("Error getting tenant credentials:", error)
            logger.error(traceback.format_exc())
        return None

    def has_salesforce_credentials(self, tenant_id: str) -> bool:
        select_query = """
        SELECT salesforce_client_url, salesforce_refresh_token, salesforce_access_token
        FROM tenants
        WHERE tenant_id = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (tenant_id,))
                result = cursor.fetchone()
                logger.debug(f"Result of existence check: {result}")
                return result[0] is not None
        except Exception as error:
            logger.error("Error getting tenant credentials:", error)
            logger.error(traceback.format_exc())
        return False

    def delete_salesforce_credentials(self, tenant_id):
        delete_query = """
        UPDATE tenants SET salesforce_client_url = NULL, salesforce_refresh_token = NULL, salesforce_access_token = NULL
        WHERE tenant_id = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_query, (tenant_id,))
                self.conn.commit()
        except Exception as error:
            logger.error("Error deleting tenant credentials:", error)
            logger.error(traceback.format_exc())
