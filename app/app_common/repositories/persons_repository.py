from typing import Optional

import psycopg2

from ..data_transfer_objects.person import PersonDTO
from loguru import logger


class PersonsRepository:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor()

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
            self.cursor.execute(create_table_query)
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
        insert_query = """
        INSERT INTO persons (uuid, name, company, email, position, timezone, challenges, strengths)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        logger.info(f"About to insert person: {person}")
        person_data = person.to_tuple()

        logger.info(f"About to insert person data: {person_data}")

        try:
            if not self.exists(person.uuid):
                self.cursor.execute(insert_query, person_data)
                logger.info(f"Cursor was executed")
                self.conn.commit()
                # logger.info("Inserted new person")
                # self.cursor.execute("SELECT LAST_INSERT_ID();")
                # logger.info("Selected last inserted")
                # person_id = self.cursor.fetchone()[0]
                # logger.info(f"Inserted person to database. Person id: {person_id}")
                # return person_id
                return "12"
            else:
                logger.warning(f"Person already exists in database. Skipping insert")
        except psycopg2.Error as error:
            logger.error("Error inserting person:", error.pgerror)
            # self.conn.rollback()
            return None

    def exists(self, uuid: str) -> bool:
        logger.info(f"about to check if uuid exists: {uuid}")
        exists_query = "SELECT 1 FROM persons WHERE uuid = %s;"
        try:
            self.cursor.execute(exists_query, (uuid,))
            logger.info(f"Executed sql query")
            result = self.cursor.fetchone() is not None
            logger.info(f"{uuid} existence in database: {result}")
            return result
        except psycopg2.Error as error:
            logger.error("Error checking if person exists:", error)
            return False

    def get_person_by_id(self, id: str) -> Optional[PersonDTO]:
        select_query = "SELECT * FROM persons WHERE id = %s;"
        try:
            self.cursor.execute(select_query, (id,))
            row = self.cursor.fetchone()
            if row:
                logger.info(f"Got {row[1]} from database")
                return PersonDTO(
                    uuid=row[1],
                    name=row[2],
                    company=row[3],
                    email=row[4],
                    position=row[5],
                    timezone=row[6],
                    challenges=row[7],
                    strengths=row[8],
                )
        except Exception as error:
            logger.error("Error fetching person by uuid:", error)
        return None

    def get_person_by_uuid(self, uuid: str) -> Optional[PersonDTO]:
        select_query = "SELECT * FROM persons WHERE uuid = %s;"
        try:
            self.cursor.execute(select_query, (uuid,))
            row = self.cursor.fetchone()
            if row:
                logger.info(f"Got {row[1]} from database")
                return PersonDTO(
                    uuid=row[1],
                    name=row[2],
                    company=row[3],
                    email=row[4],
                    position=row[5],
                    timezone=row[6],
                    challenges=row[7],
                    strengths=row[8],
                )
        except Exception as error:
            logger.error("Error fetching person by uuid:", error)
        return None

    def get_person_id_by_uuid(self, uuid: str) -> Optional[PersonDTO]:
        select_query = "SELECT id FROM persons WHERE uuid = %s;"
        try:
            self.cursor.execute(select_query, (uuid,))
            row = self.cursor.fetchone()
            if row:
                logger.info(f"Got id:{row[0]} from database")
                return row[0]
        except Exception as error:
            logger.error("Error fetching person by uuid:", error)
        return None

    def update_person(self, person: PersonDTO):
        update_query = """
        UPDATE persons
        SET name = %s, company = %s, email = %s, position = %s, timezone = %s, challenges = %s, strengths = %s
        WHERE uuid = %s;
        """
        person_data = (
            person.name,
            person.company,
            person.email,
            person.position,
            person.timezone,
            person.challenges,
            person.strengths,
            person.uuid,
        )
        try:
            self.cursor.execute(update_query, person_data)
            self.conn.commit()
            logger.info(f"Updated {person.name} in database")
        except Exception as error:
            logger.error("Error updating person:", error)
            # self.conn.rollback()

    def delete_person(self, id: str):
        delete_query = "DELETE FROM persons WHERE id = %s;"
        try:
            self.cursor.execute(delete_query, (id,))
            self.conn.commit()
            logger.info(f"Deleted {id} from database")
        except Exception as error:
            print("Error deleting person:", error)
            # self.conn.rollback()

    # Individual get methods for each attribute
    def get_name(self, uuid: str) -> Optional[str]:
        select_query = "SELECT name FROM persons WHERE uuid = %s;"
        try:
            self.cursor.execute(select_query, (uuid,))
            row = self.cursor.fetchone()
            if row:
                return row[0]
        except Exception as error:
            logger.error("Error fetching name by uuid:", error)
        return None

    def get_company(self, uuid: str) -> Optional[str]:
        select_query = "SELECT company FROM persons WHERE uuid = %s;"
        try:
            self.cursor.execute(select_query, (uuid,))
            row = self.cursor.fetchone()
            if row:
                return row[0]
        except Exception as error:
            logger.error("Error fetching company by uuid:", error)
        return None

    def get_email(self, uuid: str) -> Optional[str]:
        select_query = "SELECT email FROM persons WHERE uuid = %s;"
        try:
            self.cursor.execute(select_query, (uuid,))
            row = self.cursor.fetchone()
            if row:
                return row[0]
        except Exception as error:
            logger.error("Error fetching email by uuid:", error)
        return None

    def get_position(self, uuid: str) -> Optional[str]:
        select_query = "SELECT position FROM persons WHERE uuid = %s;"
        try:
            self.cursor.execute(select_query, (uuid,))
            row = self.cursor.fetchone()
            if row:
                return row[0]
        except Exception as error:
            logger.error("Error fetching position by uuid:", error)
        return None

    def get_timezone(self, uuid: str) -> Optional[str]:
        select_query = "SELECT timezone FROM persons WHERE uuid = %s;"
        try:
            self.cursor.execute(select_query, (uuid,))
            row = self.cursor.fetchone()
            if row:
                return row[0]
        except Exception as error:
            logger.error("Error fetching timezone by uuid:", error)
        return None

    def get_challenges(self, uuid: str) -> Optional[list[str]]:
        select_query = "SELECT challenges FROM persons WHERE uuid = %s;"
        try:
            self.cursor.execute(select_query, (uuid,))
            row = self.cursor.fetchone()
            if row:
                return row[0]
        except Exception as error:
            logger.error("Error fetching challenges by uuid:", error)
        return None

    def get_strengths(self, uuid: str) -> Optional[list[str]]:
        select_query = "SELECT strengths FROM persons WHERE uuid = %s;"
        try:
            self.cursor.execute(select_query, (uuid,))
            row = self.cursor.fetchone()
            if row:
                return row[0]
        except Exception as error:
            logger.error("Error fetching strengths by uuid:", error)
        return None

    # Individual update methods for each attribute
    def update_name(self, uuid: str, name: str):
        update_query = "UPDATE persons SET name = %s WHERE uuid = %s;"
        try:
            self.cursor.execute(update_query, (name, uuid))
            self.conn.commit()
            logger.info(f"Updated name for {uuid} to {name}")
        except Exception as error:
            logger.error("Error updating name:", error)
            # self.conn.rollback()

    def update_company(self, uuid: str, company: str):
        update_query = "UPDATE persons SET company = %s WHERE uuid = %s;"
        try:
            self.cursor.execute(update_query, (company, uuid))
            self.conn.commit()
            logger.info(f"Updated company for {uuid} to {company}")
        except Exception as error:
            logger.error("Error updating company:", error)
            # self.conn.rollback()

    def update_email(self, uuid: str, email: str):
        update_query = "UPDATE persons SET email = %s WHERE uuid = %s;"
        try:
            self.cursor.execute(update_query, (email, uuid))
            self.conn.commit()
            logger.info(f"Updated email for {uuid} to {email}")
        except Exception as error:
            logger.error("Error updating email:", error)
            # self.conn.rollback()

    def update_position(self, uuid: str, position: str):
        update_query = "UPDATE persons SET position = %s WHERE uuid = %s;"
        try:
            self.cursor.execute(update_query, (position, uuid))
            self.conn.commit()
            logger.info(f"Updated position for {uuid} to {position}")
        except Exception as error:
            logger.error("Error updating position:", error)
            # self.conn.rollback()

    def update_timezone(self, uuid: str, timezone: str):
        update_query = "UPDATE persons SET timezone = %s WHERE uuid = %s;"
        try:
            self.cursor.execute(update_query, (timezone, uuid))
            self.conn.commit()
            logger.info(f"Updated timezone for {uuid} to {timezone}")
        except Exception as error:
            logger.error("Error updating timezone:", error)
            # self.conn.rollback()

    def update_challenges(self, uuid: str, challenges: list[str]):
        update_query = "UPDATE persons SET challenges = %s WHERE uuid = %s;"
        try:
            self.cursor.execute(update_query, (challenges, uuid))
            self.conn.commit()
            logger.info(f"Updated challenges for {uuid}")
        except Exception as error:
            logger.error("Error updating challenges:", error)
            # self.conn.rollback()

    def update_strengths(self, uuid: str, strengths: list[str]):
        update_query = "UPDATE persons SET strengths = %s WHERE uuid = %s;"
        try:
            self.cursor.execute(update_query, (strengths, uuid))
            self.conn.commit()
            logger.info(f"Updated strengths for {uuid}")
        except Exception as error:
            logger.error("Error updating strengths:", error)
            # self.conn.rollback()
