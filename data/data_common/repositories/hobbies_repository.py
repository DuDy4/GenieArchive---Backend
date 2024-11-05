import traceback
from typing import Optional
import uuid
import psycopg2
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection

logger = GenieLogger()


class HobbiesRepository:
    def __init__(self):
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS hobbies (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            hobby_name VARCHAR,
            icon_url VARCHAR
        );
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
            except Exception as error:
                logger.error("Error creating table:", error)

    def insert(self, hobby: {}) -> Optional[int]:
        self.create_table_if_not_exists()
        insert_query = """
        INSERT INTO hobbies (uuid, hobby_name, icon_url)
        VALUES (%s, %s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert hobby: {hobby}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    logger.debug("About to execute sql command")
                    cursor.execute(
                        insert_query,
                        (hobby["uuid"], hobby["hobby_name"], hobby["icon_url"]),
                    )
                    logger.debug("About to commit the sql command")
                    conn.commit()
                    logger.info("Inserted new hobby")
                    hobby_id = cursor.fetchone()[0]
                    logger.info(f"Inserted hobby to database. Hobby id: {hobby_id}")
                    return hobby_id
            except psycopg2.Error as error:
                logger.error(f"Error inserting hobby: {error}")
                return None

    def exists(self, uuid: str) -> bool:
        logger.info(f"About to check if uuid exists: {uuid}")
        select_query = """
        SELECT * FROM hobbies WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid,))
                    hobby = cursor.fetchone()
                    if hobby:
                        logger.info(f"Hobby with uuid {uuid} exists")
                        return True
                    logger.info(f"Hobby with uuid {uuid} does not exist")
                    return False
            except psycopg2.Error as error:
                logger.error(f"Error checking if hobby exists: {error}")
                return False

    def get_hobby(self, uuid: str) -> {}:
        logger.info(f"About to get hobby with uuid: {uuid}")
        select_query = """
        SELECT hobby_name, icon_url FROM hobbies WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid,))
                    hobby = cursor.fetchone()
                    if hobby:
                        logger.info(f"Got hobby with uuid {uuid}")
                        return {
                            "hobby_name": hobby[0],
                            "icon_url": hobby[1],
                        }
                    logger.info(f"Hobby with uuid {uuid} does not exist")
                    return None
            except psycopg2.Error as error:
                logger.error(f"Error getting hobby: {error}")
                traceback.print_exc()
                return None

    def update_icon_url(self, uuid: str, icon_url: str) -> bool:
        logger.info(f"About to update hobby with uuid: {uuid}")
        update_query = """
        UPDATE hobbies
        SET icon_url = %s
        WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (icon_url, uuid))
                    conn.commit()
                    logger.info(f"Updated hobby with uuid {uuid}")
                    return True
            except psycopg2.Error as error:
                logger.error(f"Error updating hobby: {error}")
                return False

    def find_or_create_hobby(self, hobby_name: str, icon_url: str) -> str:
        """
        Find hobby by name or create a new one
        """
        logger.info(f"About to find or create hobby with name: {hobby_name}")
        hobby_uuid = self.find_hobby(hobby_name)
        if not hobby_uuid:
            hobby_uuid = self.create_hobby(hobby_name, icon_url)
        return hobby_uuid

    def create_hobby(self, hobby_name: str, icon_url: str) -> str:
        """
        Create new hobby
        """
        logger.info(f"About to create hobby with name: {hobby_name}")
        hobby_uuid = str(uuid.uuid4())
        hobby = {
            "uuid": hobby_uuid,
            "hobby_name": hobby_name,
            "icon_url": icon_url,
        }
        self.insert(hobby)
        return hobby_uuid

    def find_hobby(self, hobby_name: str) -> str:
        """
        Find hobby by name and return uuid
        """
        logger.info(f"About to find hobby with name: {hobby_name}")
        select_query = """
        SELECT uuid FROM hobbies WHERE hobby_name = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (hobby_name,))
                    hobby = cursor.fetchone()
                    if hobby:
                        logger.info(f"Found hobby with name {hobby_name}")
                        return hobby[0]
                    logger.info(f"Hobby with name {hobby_name} does not exist")
                    return None
            except psycopg2.Error as error:
                logger.error(f"Error finding hobby: {error}")
                return None

    def get_hobby_by_name(self, hobby_name):
        """
        Get hobby by name
        """
        logger.info(f"About to get hobby with name: {hobby_name}")
        select_query = """
        SELECT uuid, hobby_name, icon_url FROM hobbies WHERE hobby_name = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (hobby_name.lower(),))
                    hobby = cursor.fetchone()
                    if hobby:
                        logger.info(f"Got hobby with name {hobby_name}")
                        return {
                            "uuid": hobby[0],
                            "hobby_name": hobby[1],
                            "icon_url": hobby[2],
                        }
                    logger.info(f"Hobby with name {hobby_name} does not exist")
                    return None
            except psycopg2.Error as error:
                logger.error(f"Error getting hobby: {error}")
                return None
