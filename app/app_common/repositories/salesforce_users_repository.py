from typing import Optional, Union, List

import psycopg2

from loguru import logger


class SalesforceUsersRepository:
    def __init__(self, conn):
        self.conn = conn
        # self.cursor = conn.cursor()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS "sf_users" (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            name VARCHAR,
            company VARCHAR,
            salesforce_client_url VARCHAR,
            salesforce_refresh_token VARCHAR,
            salesforce_access_token VARCHAR
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
                logger.info(f"Created SF users table in database")
        except psycopg2.Error as error:
            logger.error("Error creating table:", error)

    def insert(self, uuid, name, company, client_url, refresh_token, access_token):
        self.create_table_if_not_exists()
        insert_query = """
        INSERT INTO "sf_users" (uuid, name, company, salesforce_client_url, salesforce_refresh_token, salesforce_access_token)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        try:
            if not self.exists(uuid):
                with self.conn.cursor() as cursor:
                    logger.debug(f"About to insert Salesforce user: {uuid}")
                    cursor.execute(
                        insert_query,
                        (uuid, name, company, client_url, refresh_token, access_token),
                    )
                    logger.debug("About to commit the sql command")
                    self.conn.commit()
                    logger.info("Inserted Salesforce user into database")
        except psycopg2.Error as error:
            logger.error("Error inserting user:", error.pgerror)
            logger.error(
                f"Specific error message: {error.pgerror}"
            )  # Log specific error message

    def exists(self, uuid):
        select_query = """SELECT uuid FROM "sf_users" WHERE uuid = %s"""
        try:
            with self.conn.cursor() as cursor:
                logger.debug(f"Executing SQL query: {select_query}, UUID: {uuid}")
                cursor.execute(select_query, (uuid,))
                result = cursor.fetchone()
                logger.info(f"Result of existence check: {result}")
                if result is not None:
                    return True
                else:
                    return False
        except psycopg2.Error as error:
            logger.error("Error checking existence:", error)
            logger.error(
                f"Specific error message: {error.pgerror}"
            )  # Log specific error message
            return False
