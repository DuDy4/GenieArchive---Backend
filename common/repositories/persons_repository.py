from typing import Optional, Union, List

import psycopg2

from ..data_transfer_objects.person import PersonDTO
from loguru import logger


class PersonsRepository:
    def __init__(self, conn):
        self.conn = conn
        # self.cursor = conn.cursor()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS persons (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            name VARCHAR,
            company VARCHAR,
            email VARCHAR,
            position VARCHAR,
            timezone VARCHAR,
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
                logger.info(f"Created persons table in database")
        except Exception as error:
            logger.error("Error creating table:", error)
            # self.conn.rollback()

    def insert(self, person: PersonDTO) -> str | None:
        """
        :param person: PersonDTO object with person data to insert into database
        :return the id of the newly created person in database:
        """
        insert_query = """
        INSERT INTO persons (uuid, name, company, email, position, timezone)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert person: {person}")
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

    def update(self, person: PersonDTO):
        update_query = """
        UPDATE persons
        SET name = %s, company = %s, email = %s, position = %s, timezone = %s
        WHERE uuid = %s
        """
        person_data = person.to_tuple()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, person_data)
                self.conn.commit()
                logger.info(f"Updated person in database")
        except psycopg2.Error as error:
            raise Exception(f"Error updating person, because: {error.pgerror}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def save_person(self, person: PersonDTO):
        self.create_table_if_not_exists()
        if self.exists(person.uuid):
            return self.update(person)
        else:
            return self.insert(person)
