import traceback
import psycopg2
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection

logger = GenieLogger()



class SalesforceUsersRepository:
    def __init__(self, ):
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS "sf_users" (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR NOT NULL,
            salesforce_id VARCHAR NOT NULL,
            salesforce_client_url VARCHAR,
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

    def insert(self, user_id, salesforce_id, client_url, refresh_token, access_token):
        self.create_table_if_not_exists()
        insert_query = """
        INSERT INTO sf_users (user_id, salesforce_id, salesforce_client_url, salesforce_refresh_token, salesforce_access_token)
        VALUES (%s, %s, %s, %s, %s)
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, (user_id, salesforce_id, client_url, refresh_token, access_token))
                    conn.commit()
                    logger.info("Inserted salesforce user into database")
            except psycopg2.Error as error:
                logger.error("Error inserting user:", error.pgerror)
                logger.error(traceback.format_exc())

                logger.error(
                    f"Specific error message: {error.pgerror}"
                )  # Log specific error message

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

    # def get_refresh_token(self, company):
    #     select_query = (
    #         """SELECT salesforce_refresh_token FROM sf_users WHERE company = %s"""
    #     )
    #     try:
    #         logger.debug(f"Getting refresh token for company: {company}")
    #         with self.conn.cursor() as cursor:
    #             cursor.execute(select_query, (company,))
    #             result = cursor.fetchone()
    #             logger.info(f"Result of refresh token query: {result}")
    #             if result is not None:
    #                 return result[0]
    #             else:
    #                 return None
    #     except psycopg2.Error as error:
    #         logger.error("Error getting refresh token:", error)
    #         logger.error(f"Specific error message: {error.pgerror}")
    #
    # def get_refresh_token_by_access_token(self, access_token):
    #     select_query = """SELECT salesforce_refresh_token FROM sf_users WHERE salesforce_access_token = %s"""
    #     try:
    #         logger.debug(f"Getting refresh token for company: {access_token}")
    #         with self.conn.cursor() as cursor:
    #             cursor.execute(select_query, (access_token,))
    #             result = cursor.fetchone()
    #             logger.info(f"Result of refresh token query: {result}")
    #             if result is not None:
    #                 return result[0]
    #             else:
    #                 return None
    #     except psycopg2.Error as error:
    #         logger.error("Error getting refresh token:", error)
    #         logger.error(f"Specific error message: {error.pgerror}")
    #
    # def update_token(self, uuid, refresh_token, access_token):
    #     update_query = """
    #     UPDATE sf_users
    #     SET salesforce_refresh_token = %s, salesforce_access_token = %s
    #     WHERE uuid = %s
    #     """
    #     try:
    #         with self.conn.cursor() as cursor:
    #             cursor.execute(update_query, (refresh_token, access_token, uuid))
    #             self.conn.commit()
    #             logger.info("Updated salesforce user in database")
    #     except psycopg2.Error as error:
    #         logger.error("Error updating user:", error)
    #         logger.error(f"Specific error message: {error.pgerror}")
