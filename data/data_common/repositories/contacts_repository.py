import json
import traceback
from datetime import date, datetime
from typing import Optional, Union, List
import psycopg2
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection
logger = GenieLogger()


class ContactsRepository:
    def __init__(self):
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS contacts (
            id SERIAL PRIMARY KEY,
            salesforce_id VARCHAR NOT NULL,
            name VARCHAR,
            email VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP            
        );
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
            except Exception as error:
                logger.error(f"Error creating table: {error}")

    def save_contact(self, salesforce_id: str, name: str, email: str, user_id: str):
        if self.exists(salesforce_id, user_id):
            logger.info(f"Contact already exists: {salesforce_id}")
            return
        insert_query = """
        INSERT INTO contacts (salesforce_id, name, email, user_id)
        VALUES (%s, %s, %s, %s)
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, (salesforce_id, name, email, user_id))
                    conn.commit()
            except Exception as error:
                logger.error(f"Error saving contact: {error}")

    def exists(self, salesforce_id: str, user_id: str) -> bool:
        select_query = """
        SELECT id FROM contacts
        WHERE salesforce_id = %s AND user_id = %s
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (salesforce_id, user_id))
                    return cursor.fetchone() is not None
            except Exception as error:
                logger.error(f"Error checking contact exists: {error}")
                return False



