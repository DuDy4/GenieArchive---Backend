import traceback
import uuid
from typing import Optional, Union, List

import psycopg2
from common.utils.str_utils import get_uuid4

from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection

logger = GenieLogger()


class GoogleCredsRepository:
    def __init__(self):
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS google_creds (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            email VARCHAR,
            refresh_token TEXT,
            access_token TEXT,
            last_update TIMESTAMP,
            last_fetch_meetings TIMESTAMP
        );
        """
        with db_connection() as conn:

            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
            except Exception as error:
                logger.error("Error creating table:", error)
                # conn.rollback()

    def insert(self, creds: dict):
        insert_query = """
        INSERT INTO google_creds (uuid, email, refresh_token, access_token, last_update)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        if self.exists(creds.get("email")):
            logger.info(
                "Overriding existing google creds in database for email:",
                creds.get("email"),
            )
            self.update_creds(creds)
            return

        uuid = get_uuid4()
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        insert_query,
                        (
                            uuid,
                            creds.get("email"),
                            creds.get("refreshToken"),
                            creds.get("accessToken"),
                        ),
                    )
                    conn.commit()
            except Exception as error:
                logger.error("Error inserting credentials:", error)
                # conn.rollback()

    def exists(self, email: str) -> bool:
        query = """
        SELECT * FROM google_creds WHERE email = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (email,))
                    result = cursor.fetchone()
                    logger.debug(f"Result: {result}")
                    return result is not None
            except psycopg2.Error as error:
                logger.error(f"Error checking if email exists: {error}")
                return False

    def get_creds(self, email: str) -> Union[dict, None]:
        query = """
        SELECT uuid, email, refresh_token, access_token, last_fetch_meetings
        FROM google_creds WHERE email = %s;
        """
        logger.debug(f"About to get creds for email: {email}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (email,))
                    creds = cursor.fetchone()
                    if not creds:
                        return None
                    creds_dict = {
                        "uuid": creds[0],
                        "email": creds[1],
                        "refresh_token": creds[2],
                        "access_token": creds[3],
                        "last_fetch_meetings": creds[4],
                    }
                    return creds_dict if creds else None
            except psycopg2.Error as error:
                logger.error(f"Error getting credentials: {error}")
                return None

    def has_google_creds(self, email: str) -> bool:
        google_creds = self.get_creds(email)
        return google_creds.get("refresh_token") is not None

    def update_creds(self, creds):
        update_query = """
        UPDATE google_creds SET refresh_token = %s, access_token = %s, last_update = CURRENT_TIMESTAMP
        WHERE email = %s
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        update_query,
                        (
                            creds.get("refreshToken") or creds.get("refresh_token"),
                            creds.get("accessToken") or creds.get("access_token"),
                            creds.get("email"),
                        ),
                    )
                    conn.commit()
                    logger.info("Updated credentials in database")
                    return
            except psycopg2.Error as error:
                # conn.rollback()
                logger.error(f"Error updating credentials, because: {error.pgerror}")
                traceback.print_exc()
            except Exception as e:
                # conn.rollback()
                logger.error(f"Unexpected error: {e}")
                # traceback.print_exc()

    def update_google_creds(self, user_email, user_access_token, user_refresh_token):
        update_query = """
        UPDATE google_creds SET access_token = %s, refresh_token = %s, last_update = CURRENT_TIMESTAMP
        WHERE email = %s
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        update_query,
                        (user_access_token, user_refresh_token, user_email),
                    )
                    conn.commit()
                    logger.info("Updated credentials in database")
                    return
            except psycopg2.Error as error:
                logger.error(f"Error updating credentials, because: {error.pgerror}")
                traceback.print_exc()
            except Exception as e:
                logger.error(f"Unexpected error: {e}")

    def save_creds(self, user_email, user_access_token, user_refresh_token):
        if not self.exists(user_email):
            self.insert(
                {
                    "email": user_email,
                    "accessToken": user_access_token,
                    "refreshToken": user_refresh_token,
                }
            )
        else:
            self.update_google_creds(user_email, user_access_token, user_refresh_token)

    def update_last_fetch_meetings(self, email: str):
        update_query = """
        UPDATE google_creds SET last_fetch_meetings = CURRENT_TIMESTAMP
        WHERE email = %s
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (email,))
                    conn.commit()
                    logger.info("Updated last fetch meetings in database")
                    return
            except psycopg2.Error as error:
                logger.error(f"Error updating last fetch meetings, because: {error.pgerror}")
                traceback.print_exc()
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
