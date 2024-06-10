from typing import Optional, Union, List

import psycopg2

from ..data_transfer_objects.person_dto import PersonDTO
from ..data_transfer_objects.profile_dto import ProfileDTO
from loguru import logger


class ProfilesRepository:
    def __init__(self, conn):
        self.conn = conn
        # self.cursor = conn.cursor()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS profiles (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            name VARCHAR,
            challenges JSONB,
            strengths JSONB,
            summary TEXT;
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
                logger.info(f"Created profiles table in database")
        except Exception as error:
            logger.error("Error creating table:", error)
            # self.conn.rollback()

    def insert_profile(self, person: PersonDTO) -> str | None:
        """
        :param person: PersonDTO object with person data to insert into database
        :return the id of the newly created person in database:
        """
        insert_query = """
        INSERT INTO persons (uuid, name, challenges, strengths, summary)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert profile: {person}")
        person_data = person.to_tuple()

        logger.info(f"About to insert person data: {person_data}")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, person_data)
                self.conn.commit()
                person_id = cursor.fetchone()[0]
                logger.info(f"Inserted person to database. Person id: {person_id}")
                return person_id
        except psycopg2.Error as error:
            # self.conn.rollback()
            raise Exception(f"Error inserting person, because: {error.pgerror}")

    def exists(self, uuid: str) -> bool:
        logger.info(f"About to check if uuid exists: {uuid}")
        exists_query = "SELECT 1 FROM persons WHERE uuid = %s;"
        try:
            with self.conn.cursor() as cursor:
                logger.info(f"about to execute check if uuid exists: {uuid}")

                cursor.execute(exists_query, (uuid,))
                result = cursor.fetchone() is not None
                logger.info(f"{uuid} existence in database: {result}")
                return result
        except psycopg2.Error as error:
            logger.error(f"Error checking existence of uuid {uuid}: {error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def get_person_id(self, uuid):
        select_query = "SELECT id FROM persons WHERE uuid = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got {row[2]} from database")
                    return
                else:
                    logger.error(f"Error with getting person id for {uuid}")

        except Exception as error:
            logger.error("Error fetching id by uuid:", error)
        return None

    def get_profile_data(self, uuid: str) -> Union[ProfileDTO, None]:
        select_query = """
        SELECT name, challenges, strengths, summary
        FROM persons
        WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got {row[0]} from database")
                    return ProfileDTO.from_tuple(row)
                else:
                    logger.error(f"Error with getting person data for {uuid}")

        except Exception as error:
            logger.error("Error fetching profile data by uuid:", error)
        return None

    def update(self, person):
        update_query = """
        UPDATE persons
        SET name = %s, challenges = %s, strengths = %s, summary = %s
        WHERE uuid = %s;
        """
        person_data = person.to_tuple()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, person_data)
                self.conn.commit()
                logger.info(f"Updated person with uuid: {person.uuid}")
        except psycopg2.Error as error:
            # self.conn.rollback()
            raise Exception(f"Error updating person, because: {error.pgerror}")

    def save_profile(self, person: PersonDTO):
        self.create_table_if_not_exists()
        if self.exists(person.uuid):
            self.update(person)
        else:
            self.insert_profile(person)
