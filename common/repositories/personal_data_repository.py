from typing import Optional, Union, List
import json
import psycopg2
import traceback

from loguru import logger


class PersonalDataRepository:
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
            personal_data JSONB
        );
        """
        with self.conn.cursor() as cursor:
            cursor.execute(create_table_query)
            self.conn.commit()

        # self.conn.rollback()

    def insert(self, uuid: str, name: Optional[str] = None, personal_data: json = None):
        """
        Insert a new personalData into the database.

        :param uuid: Unique identifier for the profile.
        :param name: Name of the person (optional).
        :param personal_data: Personal data of the person (optional).
        """
        insert_query = """
        INSERT INTO profiles (uuid, name, personal_data)
        VALUES (%s, %s, %s)
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, (uuid, name, personal_data))
                self.conn.commit()
                logger.info("Inserted profile into database")
        except psycopg2.IntegrityError as e:
            logger.error("Profile with this UUID already exists")
            self.conn.rollback()
        except Exception as e:
            logger.error("Error inserting profile:", e)
            # logger.error(traceback.format_exc())
            # self.conn.rollback()

    def exists(self, uuid: str) -> bool:
        """
        Check if a personalData with the given UUID exists in the database.

        :param uuid: Unique identifier for the profile.
        :return: True if profile exists, False otherwise.
        """
        select_query = """
        SELECT EXISTS (
            SELECT 1
            FROM profiles
            WHERE uuid = %s
        )
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                exists = cursor.fetchone()[0]
                return exists
        except psycopg2.Error as e:
            logger.error("Error checking for existing profile:", e)
            return False
        except Exception as e:
            logger.error("Error checking profile existence:", e)
            return False

    def get_personal_data(self, uuid: str) -> Optional[dict]:
        """
        Retrieve personal data associated with an uuid.

        :param uuid: Unique identifier for the profile.
        :return: Personal data as a json if profile exists, None otherwise.
        """
        logger.info(f"Got get request for {uuid}")
        self.create_table_if_not_exists()
        select_query = """
        SELECT personal_data
        FROM profiles
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                personal_data = cursor.fetchone()
                if personal_data:
                    return personal_data[0]
                else:
                    logger.warning("Profile was not found")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving personal data: {e}", e)
            traceback.format_exc()
            return None

    def update(self, uuid, personal_data):
        """
        Save personal data to the database.

        :param uuid: Unique identifier for the profile.
        :param personal_data: Personal data to save.
        """
        update_query = """
        UPDATE profiles
        SET personal_data = %s
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

    def save_personal_data(self, uuid, personal_data):
        """
        Save personal data to the database.

        :param uuid: Unique identifier for the profile.
        :param personal_data: Personal data to save.
        """
        self.create_table_if_not_exists()
        if not self.exists(uuid):
            logger.error("Profile with this UUID does not exist")
            return
        self.update(uuid, personal_data)
        return
