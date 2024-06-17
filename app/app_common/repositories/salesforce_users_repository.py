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

    def insert(self, uuid, company, client_url, refresh_token, access_token):
        self.create_table_if_not_exists()
        insert_query = """
        INSERT INTO sf_users (uuid, company, salesforce_client_url, salesforce_refresh_token, salesforce_access_token)
        VALUES (%s, %s, %s, %s, %s)
        """
        try:
            if not self.exists(company, client_url):
                with self.conn.cursor() as cursor:
                    cursor.execute(
                        insert_query,
                        (uuid, company, client_url, refresh_token, access_token),
                    )
                    self.conn.commit()
                    logger.info("Inserted Salesforce user into database")
        except psycopg2.Error as error:
            logger.error("Error inserting user:", error.pgerror)
            logger.error(
                f"Specific error message: {error.pgerror}"
            )  # Log specific error message

    def exists(self, company: str, client_url: str):
        self.create_table_if_not_exists()
        select_query = """SELECT uuid FROM sf_users WHERE company = %s AND salesforce_client_url = %s"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (company, client_url))
                result = cursor.fetchone()
                logger.info(f"Result of existence check: {result}")
                if result is not None:
                    return result[0]
                else:
                    return False
        except psycopg2.Error as error:
            logger.error("Error checking existence:", error)
            logger.error(
                f"Specific error message: {error.pgerror}"
            )  # Log specific error message
            return False

    def get_refresh_token(self, company):
        select_query = (
            """SELECT salesforce_refresh_token FROM sf_users WHERE company = %s"""
        )
        try:
            logger.debug(f"Getting refresh token for company: {company}")
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (company,))
                result = cursor.fetchone()
                logger.info(f"Result of refresh token query: {result}")
                if result is not None:
                    return result[0]
                else:
                    return None
        except psycopg2.Error as error:
            logger.error("Error getting refresh token:", error)
            logger.error(f"Specific error message: {error.pgerror}")

    def update_token(self, uuid, refresh_token, access_token):
        update_query = """
        UPDATE sf_users
        SET salesforce_refresh_token = %s, salesforce_access_token = %s
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (refresh_token, access_token, uuid))
                self.conn.commit()
                logger.info("Updated Salesforce user in database")
        except psycopg2.Error as error:
            logger.error("Error updating user:", error)
            logger.error(f"Specific error message: {error.pgerror}")
