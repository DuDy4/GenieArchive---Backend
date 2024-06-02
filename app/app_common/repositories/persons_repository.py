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
            challenges TEXT[],
            strengths TEXT[]
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

    def insert_person(self, person: PersonDTO) -> str | None:
        """
        :param person:
        :return the id of the newly created person in database:
        """
        self.create_table_if_not_exists()
        if self.exists(person.uuid):
            logger.warning(f"Person already exists in database. Skipping insert")
            raise Exception("Person already exists in database")
        insert_query = """
        INSERT INTO persons (uuid, name, company, email, position, timezone, challenges, strengths)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert person: {person}")
        person_data = person.to_tuple()

        logger.info(f"About to insert person data: {person_data}")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, person_data)
                logger.info(f"Cursor was executed")
                self.conn.commit()
                logger.info("Inserted new person")
                person_id = cursor.fetchone()[0]
                logger.info(f"Inserted person to database. Person id: {person_id}")
                return person_id
        except psycopg2.Error as error:
            logger.error("Error inserting person:", error.pgerror)
            # self.conn.rollback()
            raise Exception(f"Error inserting person, because: {error.pgerror}")

    def exists(self, uuid: str) -> bool:
        logger.info(f"About to check if uuid exists: {uuid}")
        exists_query = "SELECT 1 FROM persons WHERE uuid = %s;"

        try:
            with self.conn.cursor() as cursor:
                logger.info(f"about to execute check if uuid exists: {uuid}")

                cursor.execute(exists_query, (uuid,))
                logger.info(f"Executed sql query")
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

    def get_person_by_id(self, id: str) -> Optional[PersonDTO]:
        select_query = "SELECT * FROM persons WHERE id = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (id,))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got {row[2]} from database")
                    return PersonDTO(*row[1:])

        except Exception as error:
            logger.error("Error fetching person by id:", error)
        return None

    def get_name(self, id_or_uuid: str | int) -> Optional[str]:
        return self._get_attribute(id_or_uuid, "name")

    def get_company(self, id_or_uuid: str | int) -> Optional[str]:
        return self._get_attribute(id_or_uuid, "company")

    def get_email(self, id_or_uuid: str | int) -> Optional[str]:
        return self._get_attribute(id_or_uuid, "email")

    def get_position(self, id_or_uuid: str | int) -> Optional[str]:
        return self._get_attribute(id_or_uuid, "position")

    def get_timezone(self, id_or_uuid: str | int) -> Optional[str]:
        return self._get_attribute(id_or_uuid, "timezone")

    def get_challenges(self, id_or_uuid: str | int) -> Optional[list[str]]:
        return self._get_attribute(id_or_uuid, "challenges")

    def get_strengths(self, id_or_uuid: str | int) -> Optional[list[str]]:
        return self._get_attribute(id_or_uuid, "strengths")

    def update_person(self, person: PersonDTO):
        update_query = """
        UPDATE persons
        SET name = %s, company = %s, email = %s, position = %s, timezone = %s, challenges = %s, strengths = %s
        WHERE uuid = %s;
        """
        person_data = person.to_tuple
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, person_data)
                self.conn.commit()
                logger.info(f"Updated {person.name} in database")
        except Exception as error:
            logger.error("Error updating person:", error)

    # def _update_attribute_by_uuid(self, uuid, attribute, value):
    #     select_query = f"UPDATE persons SET {attribute} = %s WHERE uuid = %s;"
    #     try:
    #         with self.conn.cursor() as cursor:
    #             cursor.execute(select_query, (value, uuid))
    #             self.conn.commit()
    #             logger.info(f"Updated {attribute} for {uuid}")
    #     except Exception as error:
    #         logger.error(f"Error fetching {attribute} by uuid:", error)
    #     return None

    def delete_person(self, id: str):
        delete_query = "DELETE FROM persons WHERE id = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_query, (id,))
                self.conn.commit()
                logger.info(f"Deleted {id} from database")
        except Exception as error:
            logger.error("Error deleting person:", error)
            # self.conn.rollback()

    def handle_sf_contacts_list(self, persons_list: list[dict]):
        for contact in persons_list:
            person = PersonDTO.from_sf_contact(contact)
            try:
                self.insert_person(person)
                logger.info(f"Inserted person: {person.name}")
            except Exception as e:
                logger.error(f"Failed to insert person: {e}")

    def _get_attribute(
        self, id_or_uuid: str | int, attribute: str
    ) -> Optional[Union[str, List[str]]]:
        select_query = (
            f"SELECT {attribute} FROM persons WHERE "
            f"{'id' if isinstance(id_or_uuid, int) else 'uuid'} = %s;"
        )

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (id_or_uuid,))
                row = cursor.fetchone()
                if row:
                    return row[0]
        except Exception as error:
            logger.error(f"Error fetching {attribute} by uuid:", error)
        return None
