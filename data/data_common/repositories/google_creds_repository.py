import traceback
from typing import Optional, Union, List

import psycopg2
from common.utils.str_utils import get_uuid4

from loguru import logger


class GoogleCredsRepository:
    def __init__(self, conn):
        self.conn = conn
        # self.cursor = conn.cursor()
        self.create_table_if_not_exists()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS google_creds (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            tenant_id VARCHAR,
            refresh_token TEXT,
            access_token TEXT
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
        except Exception as error:
            logger.error("Error creating table:", error)
            self.conn.rollback()

    def insert(self, creds: dict):
        insert_query = """
        INSERT INTO google_creds (uuid, tenant_id, refresh_token, access_token)
        VALUES (%s, %s, %s, %s)
        """
        if self.exists(creds.get("tenantId")):
            logger.info("User already exists in database")
            self.update_creds(creds)
            return

        uuid = get_uuid4()

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    insert_query,
                    (
                        uuid,
                        creds.get("tenantId"),
                        creds.get("refreshToken"),
                        creds.get("accessToken"),
                    ),
                )
                self.conn.commit()
        except Exception as error:
            logger.error("Error inserting tenant:", error)
            self.conn.rollback()

    def exists(self, tenant_id: str) -> bool:
        query = """
        SELECT * FROM tenants WHERE tenant_id = %s;
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (tenant_id,))
            return cursor.fetchone() is not None

    def get_creds(self, tenant_id: str) -> Union[dict, None]:
        query = """
        SELECT * FROM google_creds WHERE tenant_id = %s;
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (tenant_id,))
            creds = cursor.fetchone()
            creds_dict = {
                "uuid": creds[1],
                "tenant_id": creds[2],
                "refresh_token": creds[3],
                "access_token": creds[4],
            }
            return creds_dict if creds else None

    def has_google_creds(self, tenant_id: str) -> bool:
        google_creds = self.get_creds(tenant_id)
        return google_creds.get("refresh_token") is not None

    def update_creds(self, creds):
        update_query = """
        UPDATE google_creds SET refresh_token = %s, access_token = %s
        WHERE tenant_id = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    update_query,
                    (
                        creds.get("refreshToken") or creds.get("refresh_token"),
                        creds.get("accessToken") or creds.get("access_token"),
                        creds.get("tenantId") or creds.get("tenant_id"),
                    ),
                )
                self.conn.commit()
                return
        except psycopg2.Error as error:
            self.conn.rollback()
            logger.error(f"Error updating creds, because: {error.pgerror}")
            traceback.print_exc()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
