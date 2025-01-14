import traceback
import uuid
from typing import Optional, Union, List
from data.data_common.data_transfer_objects.user_dto import UserDTO

import psycopg2
from common.utils.str_utils import get_uuid4

from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection

logger = GenieLogger()


class UsersRepository:
    def __init__(self):
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                uuid VARCHAR UNIQUE NOT NULL,
                user_id VARCHAR UNIQUE NOT NULL,
                user_name VARCHAR,
                email VARCHAR,
                tenant_id VARCHAR,
                reminder_subscription BOOLEAN DEFAULT TRUE
            );
            """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
            except Exception as error:
                logger.error("Error creating table:", error)

    def insert(self, user: UserDTO):
        insert_query = """
            INSERT INTO users (uuid, user_id, user_name, email, tenant_id)
            VALUES (%s, %s, %s, %s, %s)
            """
        if self.exists(user):
            logger.info("User already exists in database")
            return

        with db_connection() as conn:
            if not self.exists(user):
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            insert_query,
                            (
                                user.uuid or get_uuid4(),
                                user.user_id,
                                user.name,
                                user.email,
                                user.tenant_id,
                            ),
                        )
                        conn.commit()
                except psycopg2.Error as error:
                    logger.error("Error getting tenant id:", error)
                    logger.error(f"Specific error message: {error.pgerror}")
                except Exception as error:
                    logger.error("Error inserting user:", error)


    def exists(self, user: UserDTO) -> bool:
        select_query = """
            SELECT uuid, user_id, user_name, email, tenant_id
            FROM users WHERE user_id = %s or email = %s
            """
        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query, (user.user_id, user.email))
                return cursor.fetchone() is not None

    def email_exists(self, email: str) -> bool:
        select_query = """
            SELECT uuid, user_id, user_name, email, tenant_id
            FROM users WHERE email = %s
            """
        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query, (email,))
                return cursor.fetchone() is not None

    def get_user_by_id(self, user_id: str) -> Optional[UserDTO]:
        select_query = """
            SELECT uuid, user_id, user_name, email, tenant_id FROM users WHERE user_id = %s
            """
        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query, (user_id,))
                result = cursor.fetchone()
                return UserDTO.from_tuple(result) if result else None

    def get_user_by_email(self, email: str) -> Optional[UserDTO]:
        select_query = """
            SELECT uuid, user_id, user_name, email, tenant_id FROM users WHERE email = %s
            """
        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query, (email,))
                result = cursor.fetchone()
                return UserDTO.from_tuple(result) if result else None

    def get_email_by_user_id(self, user_id: str) -> Optional[str]:
        select_query = """
            SELECT email FROM users WHERE user_id = %s
            """
        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query, (user_id,))
                result = cursor.fetchone()
                return result[0] if result else None

    def get_user_email_and_name(self, user_id: str) -> Optional[tuple]:
        select_query = """
            SELECT email, user_name FROM users WHERE user_id = %s
            """
        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query, (user_id,))
                result = cursor.fetchone()
                return result[0], result[1] if result else None

    def get_all_users(self) -> List[UserDTO]:
        select_query = """
            SELECT uuid, user_id, user_name, email, tenant_id FROM users
            """
        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query)
                return [UserDTO.from_tuple(row) for row in cursor.fetchall()]

    def get_email_by_tenant_id(self, tenant_id: str) -> Optional[str]:
        select_query = """
            SELECT email FROM users WHERE tenant_id = %s
            """
        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query, (tenant_id,))
                result = cursor.fetchone()
                return result[0] if result else None

    def get_tenant_id_by_user_id(self, user_id: str) -> Optional[str]:
        select_query = """
            SELECT tenant_id FROM users WHERE user_id = %s
            """
        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_query, (user_id,))
                result = cursor.fetchone()
                return result[0] if result else None

    def update_reminder_subscription(self, user_id: str, subscription: bool):
        update_query = """
            UPDATE users SET reminder_subscription = %s WHERE user_id = %s
            """
        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(update_query, (subscription, user_id))
                conn.commit()


