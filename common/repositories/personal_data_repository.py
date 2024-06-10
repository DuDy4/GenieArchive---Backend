from typing import Optional, Union, List
import json
import psycopg2
import traceback

from loguru import logger

from common.data_transfer_objects.person_dto import PersonDTO


class PersonalDataRepository:
    FETCHED = "FETCHED"
    TRIED_BUT_FAILED = "TRIED_BUT_FAILED"

    def __init__(self, conn):
        self.conn = conn

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS personalData (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            name VARCHAR,
            linkedin_url VARCHAR,
            personal_data JSONB,
            status TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
                # logger.info("Created personalData table")
        except psycopg2.Error as e:
            logger.error(f"Error creating table: {e.pgcode}: {e.pgerror}")
        except Exception as e:
            logger.error(f"Error creating table: {repr(e)}")

    def insert(
        self,
        uuid: str,
        name: Optional[str] = None,
        linkedin_url: str = None,
        personal_data: json = None,
        status: str = "FETCHED",
    ):
        """
        Insert a new personalData into the database.

        :param uuid: Unique identifier for the personalData.
        :param name: Name of the person (optional).
        :param personal_data: Personal data of the person (optional).
        """
        insert_query = """
        INSERT INTO personalData (uuid, name, linkedin_url, personal_data, status)
        VALUES (%s, %s, %s, %s, %s)
        """
        if self.exists_uuid(uuid):
            logger.error("Personal data with this UUID already exists")
            return
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    insert_query, (uuid, name, linkedin_url, personal_data, status)
                )
                self.conn.commit()
                logger.info("Inserted personalData into database")
        except psycopg2.IntegrityError as e:
            logger.error("personalData with this UUID already exists")
            self.conn.rollback()
        except Exception as e:
            logger.error("Error inserting personalData:", e)
            # logger.error(traceback.format_exc())
            # self.conn.rollback()

    def exists_uuid(self, uuid: str) -> bool:
        """
        Check if a personalData with the given UUID exists in the database.

        :param uuid: Unique identifier for the personalData.
        :return: True if personalData exists, False otherwise.
        """
        self.create_table_if_not_exists()
        select_query = """
        SELECT EXISTS (
            SELECT 1
            FROM personalData
            WHERE uuid = %s
        )
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                exists = cursor.fetchone()[0]
                return exists
        except psycopg2.Error as e:
            logger.error("Error checking for existing personalData:", e)
            return False
        except Exception as e:
            logger.error("Error checking personalData existence:", e)
            return False

    def exists_linkedin_url(self, linkedin_url: str) -> bool:
        """
        Check if a personalData with the given linkedin_url exists in the database.

        :param linkedin_url: Unique identifier for the personalData.
        :return: True if personalData exists, False otherwise.
        """
        self.create_table_if_not_exists()
        select_query = """
        SELECT EXISTS (
            SELECT 1
            FROM personalData
            WHERE linkedin_url = %s
        )
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (linkedin_url,))
                exists = cursor.fetchone()[0]
                return exists
        except psycopg2.Error as e:
            logger.error("Error checking for existing personalData:", e)
            return False
        except Exception as e:
            logger.error("Error checking personalData existence:", e)
            return False

    def get_personal_data(self, uuid: str) -> Optional[dict]:
        """
        Retrieve personal data associated with an uuid.

        :param uuid: Unique identifier for the personalData.
        :return: Personal data as a json if personalData exists, None otherwise.
        """
        logger.info(f"Got get request for {uuid}")
        self.create_table_if_not_exists()
        select_query = """
        SELECT personal_data
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                personal_data = cursor.fetchone()
                if personal_data:
                    return personal_data[0]
                else:
                    logger.warning("personalData was not found")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving personal data: {e}", e)
            traceback.format_exc()
            return None

    def update(self, uuid, personal_data):
        """
        Save personal data to the database.

        :param uuid: Unique identifier for the personalData.
        :param personal_data: Personal data to save.
        """
        update_query = """
        UPDATE personalData
        SET personal_data = %s, last_updated = CURRENT_TIMESTAMP, status = 'FETCHED'
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (personal_data, uuid))
                self.conn.commit()
                logger.info("Updated personal data")
        except psycopg2.Error as e:
            logger.error("Error updating personal data:", e)
            self.conn.rollback()
        except Exception as e:
            logger.error("Error updating personal data:", e)
            self.conn.rollback()
        return

    def save_personal_data(self, uuid, personal_data: dict | str):
        """
        Save personal data to the database.

        :param uuid: Unique identifier for the personal_data.
        :param personal_data: Personal data to save.
        """
        self.create_table_if_not_exists()
        if not self.exists_uuid(uuid):
            logger.error("Person with this UUID does not exist in Personal_data table")
            return
        self.update(uuid, personal_data)
        return

    def get_last_updated(self, uuid):
        """
        Retrieve the last updated timestamp for a profile.

        :param uuid: Unique identifier for the profile.
        :return: Timestamp if profile exists, None otherwise.
        """
        select_query = """
        SELECT last_updated
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                last_updated = cursor.fetchone()
                if last_updated:
                    return last_updated[0]
                else:
                    logger.warning("Profile was not found")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving last updated timestamp: {e}", e)
            traceback.format_exc()
            return None
