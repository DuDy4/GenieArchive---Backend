import traceback
import psycopg2
from common.genie_logger import GenieLogger
from common.utils.str_utils import get_uuid4
from data.data_common.utils.postgres_connector import db_connection

logger = GenieLogger()



class SalesforceUsersRepository:
    def __init__(self, ):
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sf_users (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR,
            tenant_id VARCHAR,
            salesforce_user_id VARCHAR NOT NULL,
            salesforce_tenant_id VARCHAR NOT NULL,
            salesforce_instance_url VARCHAR,
            salesforce_refresh_token VARCHAR,
            salesforce_access_token VARCHAR
        );
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
            except Exception as error:
                logger.error("Error creating table:", error)

    def save_user_creds(self, salesforce_user_id, salesforce_tenant_id, salesforce_instance_url, salesforce_refresh_token, salesforce_access_token, user_id=None, tenant_id=None):
        if self.exists_salesforce_id(salesforce_user_id):
            logger.info("Updating user credentials")
            self._update(salesforce_user_id, salesforce_instance_url, salesforce_refresh_token, salesforce_access_token)
        else:
            logger.info("Inserting user credentials")
            self._insert(salesforce_user_id, salesforce_tenant_id, salesforce_instance_url, salesforce_refresh_token, salesforce_access_token, user_id, tenant_id)


    def _insert(self, salesforce_user_id, salesforce_tenant_id, instance_url, refresh_token, access_token, user_id=None, tenant_id=None):
        self.create_table_if_not_exists()
        insert_query = """
        INSERT INTO sf_users (user_id, tenant_id, salesforce_user_id, salesforce_tenant_id, salesforce_instance_url, salesforce_refresh_token, salesforce_access_token)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        if not user_id:
            user_id = get_uuid4()
        if not tenant_id:
            tenant_id = self.get_tenant_id_by_sf_tenant_id(salesforce_tenant_id) or get_uuid4()
        with db_connection() as conn:
            try:

                with conn.cursor() as cursor:
                    cursor.execute(insert_query, (user_id, tenant_id, salesforce_user_id, salesforce_tenant_id, instance_url, refresh_token, access_token))
                    conn.commit()
                    logger.info("Inserted salesforce user into database")
            except psycopg2.Error as error:
                logger.error("Error inserting user:", error.pgerror)
                logger.error(traceback.format_exc())
                logger.error(
                    f"Specific error message: {error.pgerror}"
                )  # Log specific error message

    def _update(self, salesforce_user_id, instance_url, refresh_token, access_token):
        update_query = """
        UPDATE sf_users
        SET salesforce_instance_url = %s, salesforce_refresh_token = %s, salesforce_access_token = %s
        WHERE salesforce_user_id = %s
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (instance_url, refresh_token, access_token, salesforce_user_id))
                    conn.commit()
                    logger.info("Updated salesforce user in database")
            except psycopg2.Error as error:
                logger.error("Error updating user:", error)
                logger.error(f"Specific error message: {error.pgerror}")

    def exists_user_id(self, user_id):
        select_query = """SELECT user_id FROM sf_users WHERE user_id = %s"""
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (user_id,))
                    result = cursor.fetchone()
                    if result is not None:
                        return True
                    else:
                        return False
            except psycopg2.Error as error:
                logger.error("Error checking if user exists:", error)
                logger.error(f"Specific error message: {error.pgerror}")

    def exists_salesforce_id(self, salesforce_id):
        select_query = """SELECT salesforce_user_id FROM sf_users WHERE salesforce_user_id = %s"""
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (salesforce_id,))
                    result = cursor.fetchone()
                    if result is not None:
                        return True
                    else:
                        return False
            except psycopg2.Error as error:
                logger.error("Error checking if salesforce id exists:", error)
                logger.error(f"Specific error message: {error.pgerror}")

    def exists_user_id_or_salesforce_id(self, user_id, salesforce_id):
        select_query = """SELECT user_id, salesforce_user_id FROM sf_users WHERE user_id = %s OR salesforce_user_id = %s"""
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (user_id, salesforce_id))
                    result = cursor.fetchone()
                    if result is not None:
                        return True
                    else:
                        return False
            except psycopg2.Error as error:
                logger.error("Error checking if user id or salesforce id exists:", error)
                logger.error(f"Specific error message: {error.pgerror}")

    def get_tenant_id_by_sf_tenant_id(self, salesforce_tenant_id):
        select_query = """SELECT tenant_id FROM sf_users WHERE salesforce_tenant_id = %s"""
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (salesforce_tenant_id,))
                    result = cursor.fetchone()
                    if result is not None:
                        return result[0]
                    else:
                        return None
            except psycopg2.Error as error:
                logger.error("Error getting tenant id:", error)
                logger.error(f"Specific error message: {error.pgerror}")


    def get_user_id_by_sf_id(self, salesforce_id):
        select_query = """SELECT user_id FROM sf_users WHERE salesforce_id = %s"""
        try:
            logger.debug(f"Getting user id for salesforce id: {salesforce_id}")
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (salesforce_id,))
                result = cursor.fetchone()
                logger.info(f"Result of user id query: {result}")
                if result is not None:
                    return result[0]
                else:
                    return None
        except psycopg2.Error as error:
            logger.error("Error getting user id:", error)
            logger.error(f"Specific error message: {error.pgerror}")


    def get_access_token(self, user_id):
        select_query = """SELECT salesforce_access_token FROM sf_users WHERE user_id = %s"""
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (user_id,))
                    result = cursor.fetchone()
                    if result is not None:
                        return result[0]
                    else:
                        return None
            except psycopg2.Error as error:
                logger.error("Error getting access token:", error)
                logger.error(f"Specific error message: {error.pgerror}")

    def get_refresh_token(self, user_id):
        select_query = """SELECT salesforce_refresh_token FROM sf_users WHERE user_id = %s"""
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (user_id,))
                    result = cursor.fetchone()
                    if result is not None:
                        return result[0]
                    else:
                        return None
            except psycopg2.Error as error:
                logger.error("Error getting refresh token:", error)
                logger.error(f"Specific error message: {error.pgerror}")
