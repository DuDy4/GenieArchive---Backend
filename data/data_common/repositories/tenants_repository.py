import traceback
import uuid
from typing import Optional, Union, List
from data.data_common.data_transfer_objects.tenant_dto import TenantDTO

import psycopg2
from common.utils.str_utils import get_uuid4

from common.genie_logger import GenieLogger

logger = GenieLogger()


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
            user_id VARCHAR
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
        INSERT INTO tenants (uuid, tenant_id, user_name, email, user_id)

        VALUES (%s, %s, %s, %s, %s)
        """
        if self.exists(tenant.get("tenantId")):
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
                        tenant.get("user_id"),
                    ),
                )
                self.conn.commit()
                return uuid
        except Exception as error:
            logger.error("Error inserting tenant:", error)
            logger.error(traceback.format_exc())

    def exists(self, tenant_id: str):
        exists_query = """
        SELECT uuid FROM tenants WHERE tenant_id = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, (tenant_id,))
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as error:
            logger.error("Error checking if tenant exists:", error)
            logger.error(traceback.format_exc())

    def email_exists(self, email: str):
        exists_query = """
        SELECT uuid FROM tenants WHERE email = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, (email,))
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as error:
            logger.error("Error checking if tenant exists:", error)
            logger.error(traceback.format_exc())

    def get_tenant_id_by_email(self, email):
        select_query = """SELECT tenant_id FROM tenants WHERE email = %s"""
        try:
            logger.debug(f"Getting tenant_id for email: {email}")
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email,))
                result = cursor.fetchone()
                logger.info(f"Result of tenant id query: {result}")
                if len(result) > 1:
                    logger.error("More than one tenant found for email")
                    return result[0]
                if result is not None:
                    return result[0]
                else:
                    return None
        except psycopg2.Error as error:
            logger.error("Error getting tenant id:", error)
            logger.error(f"Specific error message: {error.pgerror}")


    def get_all_tenants(self) -> list[TenantDTO]:
        select_query = """
        SELECT uuid, tenant_id, user_name, email, user_id
        FROM tenants
        WHERE tenant_id <> '';
        """
        try:
            self.create_table_if_not_exists()
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, ())
                tenants = cursor.fetchall()
                if tenants:
                    logger.info(f"Got {len(tenants)} meetings from database")
                    return [TenantDTO.from_tuple(tenant) for tenant in tenants]
                else:
                    logger.error(f"No tenants found")
                    return []
        except Exception as error:
            logger.error("Error fetching tenants data:", error)
            traceback.print_exception(error)
            return []

    def get_tenant_email(self, tenant_id):
        select_query = """
        SELECT email FROM tenants WHERE tenant_id = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (tenant_id,))
                result = cursor.fetchone()
                logger.debug(f"Result of email query: {result}")
                return result[0] if result else None
        except Exception as error:
            logger.error("Error getting tenant email:", error)
            logger.error(traceback.format_exc())
            return None

    def update_tenant_id(
        self, old_tenant_id, new_tenant_id, user_id: Optional[str] = None, user_name: Optional[str] = None
    ):
        logger.debug(
            f"About to update tenant id from {old_tenant_id} to {new_tenant_id} and user_id: {user_id}"
        )
        update_query = "UPDATE tenants SET tenant_id = %s"
        if user_id:
            update_query += ", user_id = %s"
        if user_name:
            update_query += ", user_name = %s"
        update_query += " WHERE tenant_id = %s"
        arguments = (new_tenant_id,)
        if user_id:
            arguments += (user_id,)
        if user_name:
            arguments += (user_name,)
        arguments += (old_tenant_id,)
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, arguments)
                self.conn.commit()
                logger.info(f"Updated tenant id from {old_tenant_id} to {new_tenant_id}")
        except Exception as error:
            logger.error("Error updating tenant id:", error)
            logger.error(traceback.format_exc())

    def get_all_tenants_ids(self):
        select_query = """
        SELECT tenant_id FROM tenants;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query)
                result = cursor.fetchall()
                return [row[0] for row in result]
        except Exception as error:
            logger.error("Error getting all tenants ids:", error)
            logger.error(traceback.format_exc())
            return []
