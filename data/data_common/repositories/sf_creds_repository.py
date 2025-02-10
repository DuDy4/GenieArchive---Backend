import traceback
import psycopg2
from common.genie_logger import GenieLogger
from common.utils.str_utils import get_uuid4
from data.data_common.utils.postgres_connector import db_connection
from data.data_common.data_transfer_objects.sf_creds_dto import SalesforceCredsDTO

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

    def save_user_creds(self, salesforce_creds_dto: SalesforceCredsDTO):
        if self.exists_salesforce_id(salesforce_creds_dto.salesforce_user_id):
            logger.info("Updating user credentials")
            self._update(salesforce_creds_dto)
        else:
            logger.info("Inserting user credentials")
            self._insert(salesforce_creds_dto)


    def _insert(self, salesforce_creds_dto: SalesforceCredsDTO):
        self.create_table_if_not_exists()
        insert_query = """
        INSERT INTO sf_users (user_id, tenant_id, salesforce_user_id, salesforce_tenant_id, salesforce_instance_url, salesforce_refresh_token, salesforce_access_token)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        if not salesforce_creds_dto.user_id:
            salesforce_creds_dto.user_id = get_uuid4()
        if not salesforce_creds_dto.tenant_id:
            salesforce_creds_dto.tenant_id = self.get_tenant_id_by_sf_tenant_id(salesforce_creds_dto.salesforce_tenant_id) or get_uuid4()
        with db_connection() as conn:
            try:

                with conn.cursor() as cursor:
                    cursor.execute(insert_query, (
                        salesforce_creds_dto.user_id, salesforce_creds_dto.tenant_id,
                        salesforce_creds_dto.salesforce_user_id, salesforce_creds_dto.salesforce_tenant_id,
                        salesforce_creds_dto.instance_url, salesforce_creds_dto.refresh_token, salesforce_creds_dto.access_token))
                    conn.commit()
                    logger.info("Inserted salesforce user into database")
            except psycopg2.Error as error:
                logger.error("Error inserting user:", error.pgerror)
                logger.error(traceback.format_exc())
                logger.error(
                    f"Specific error message: {error.pgerror}"
                )  # Log specific error message

    def _update(self, salesforce_cred_dto: SalesforceCredsDTO):
        update_query = """
        UPDATE sf_users
        SET salesforce_instance_url = %s, salesforce_refresh_token = %s, salesforce_access_token = %s
        WHERE salesforce_user_id = %s
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (salesforce_cred_dto.instance_url, salesforce_cred_dto.refresh_token, salesforce_cred_dto.access_token, salesforce_cred_dto.salesforce_user_id))
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

    def get_instance_url(self, user_id):
        select_query = """SELECT salesforce_instance_url FROM sf_users WHERE user_id = %s"""
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
                logger.error("Error getting instance url:", error)
                logger.error(f"Specific error message: {error.pgerror}")

    def get_sf_creds_by_salesforce_user_id(self, salesforce_user_id):
        select_query = """SELECT salesforce_user_id, salesforce_tenant_id, salesforce_instance_url, salesforce_access_token, salesforce_refresh_token, user_id, tenant_id
         FROM sf_users WHERE salesforce_user_id = %s"""
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (salesforce_user_id,))
                    result = cursor.fetchone()
                    if result:
                        return SalesforceCredsDTO.from_tuple(result)
                    else:
                        return None
            except psycopg2.Error as error:
                logger.error("Error getting sf creds by salesforce user id:", error)
                logger.error(f"Specific error message: {error.pgerror}")

    def update_access_token(self, refresh_token, access_token):
        update_query = """UPDATE sf_users SET salesforce_access_token = %s WHERE salesforce_refresh_token = %s"""
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (access_token, refresh_token))
                    conn.commit()
            except psycopg2.Error as error:
                logger.error("Error updating access token:", error)
                logger.error(f"Specific error message: {error.pgerror}")
