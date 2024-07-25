import traceback
import uuid
from typing import Optional, Union, List

import psycopg2
from common.utils.str_utils import get_uuid4

from loguru import logger


class GoogleCredsRepository:
    def __init__(self, conn):
        self.conn = conn
        self.create_table_if_not_exists()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS google_creds (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            email VARCHAR,
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
        INSERT INTO google_creds (uuid, email, refresh_token, access_token)
        VALUES (%s, %s, %s, %s)
        """
        if self.exists(creds.get("email")):
            logger.info(
                "Google creds already exist in database for email:",
                creds.get("email"),
            )
            self.update_creds(creds)
            return

        uuid = get_uuid4()

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    insert_query,
                    (
                        uuid,
                        creds.get("email"),
                        creds.get("refreshToken"),
                        creds.get("accessToken"),
                    ),
                )
                self.conn.commit()
        except Exception as error:
            logger.error("Error inserting credentials:", error)
            self.conn.rollback()

    def exists(self, email: str) -> bool:
        query = """
        SELECT * FROM google_creds WHERE email = %s;
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (email,))
            result = cursor.fetchone()
            logger.debug(f"Result: {result}")
            return result is not None

    def get_creds(self, email: str) -> Union[dict, None]:
        query = """
        SELECT * FROM google_creds WHERE email = %s;
        """
        logger.debug(f"About to get creds for email: {email}")
        with self.conn.cursor() as cursor:
            cursor.execute(query, (email,))
            creds = cursor.fetchone()
            if not creds:
                return None
            creds_dict = {
                "uuid": creds[1],
                "email": creds[2],
                "refresh_token": creds[3],
                "access_token": creds[4],
            }
            return creds_dict if creds else None

    def has_google_creds(self, email: str) -> bool:
        google_creds = self.get_creds(email)
        return google_creds.get("refresh_token") is not None

    def update_creds(self, creds):
        update_query = """
        UPDATE google_creds SET refresh_token = %s, access_token = %s
        WHERE email = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    update_query,
                    (
                        creds.get("refreshToken") or creds.get("refresh_token"),
                        creds.get("accessToken") or creds.get("access_token"),
                        creds.get("email"),
                    ),
                )
                self.conn.commit()
                logger.info("Updated credentials in database")
                return
        except psycopg2.Error as error:
            self.conn.rollback()
            logger.error(f"Error updating credentials, because: {error.pgerror}")
            traceback.print_exc()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
