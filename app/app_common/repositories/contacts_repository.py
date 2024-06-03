from typing import Optional, Union, List

import psycopg2

from ..data_transfer_objects.person import PersonDTO
from loguru import logger


class ContactsRepository:
    def __init__(self, conn):
        self.conn = conn
        # self.cursor = conn.cursor()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS contacts (
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
                logger.info(f"Created contacts table in database")
        except Exception as error:
            logger.error("Error creating table:", error)
            # self.conn.rollback()

    def insert_contact(self, contact: PersonDTO) -> str | None:
        """
        :param contact: PersonDTO object with contact data to insert into database
        :return the id of the newly created contact in database:
        """
        self.create_table_if_not_exists()
        insert_query = """
        INSERT INTO contacts (uuid, name, company, email, position, timezone, challenges, strengths)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert contact: {contact}")
        contact_data = contact.to_tuple()

        logger.info(f"About to insert contact data: {contact_data}")

        try:
            if not self.exists(contact.uuid):
                with self.conn.cursor() as cursor:
                    cursor.execute(insert_query, contact_data)
                    self.conn.commit()
                    contact_id = cursor.fetchone()[0]
                    logger.info(
                        f"Inserted contact to database. contact id: {contact_id}"
                    )
                    return contact_id
            else:
                raise Exception("contact already exists in database")
        except psycopg2.Error as error:
            # self.conn.rollback()
            raise Exception(f"Error inserting contact, because: {error.pgerror}")

    def exists(self, uuid: str) -> bool:
        logger.info(f"About to check if uuid exists: {uuid}")
        exists_query = "SELECT 1 FROM contacts WHERE uuid = %s;"

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

    def get_contact_id(self, uuid):
        select_query = "SELECT id FROM contacts WHERE uuid = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got {row[2]} from database")
                    return
                else:
                    logger.error(f"Error with getting contact id for {uuid}")

        except Exception as error:
            logger.error("Error fetching id by uuid:", error)
        return None

    def get_contact_by_id(self, id: str) -> Optional[PersonDTO]:
        select_query = "SELECT * FROM contacts WHERE id = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (id,))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got {row[2]} from database")
                    return PersonDTO(*row[1:])

        except Exception as error:
            logger.error("Error fetching contact by id:", error)
        return None

    def get_contact_by_uuid(self, uuid: str) -> Optional[PersonDTO]:
        select_query = "SELECT * FROM contacts WHERE uuid = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got {row[2]} from database")
                    return PersonDTO(*row[1:])

        except Exception as error:
            logger.error("Error fetching contact by id:", error)
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

    def update_contact(self, contact: PersonDTO):
        update_query = """
        UPDATE contacts
        SET name = %s, company = %s, email = %s, position = %s, timezone = %s, challenges = %s, strengths = %s
        WHERE uuid = %s;
        """
        contact_data = contact.to_tuple
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, contact_data)
                self.conn.commit()
                logger.info(f"Updated {contact.name} in database")
        except Exception as error:
            logger.error("Error updating contact:", error)

    # def _update_attribute_by_uuid(self, uuid, attribute, value):
    #     select_query = f"UPDATE contacts SET {attribute} = %s WHERE uuid = %s;"
    #     try:
    #         with self.conn.cursor() as cursor:
    #             cursor.execute(select_query, (value, uuid))
    #             self.conn.commit()
    #             logger.info(f"Updated {attribute} for {uuid}")
    #     except Exception as error:
    #         logger.error(f"Error fetching {attribute} by uuid:", error)
    #     return None

    def delete_contact(self, id: str):
        delete_query = "DELETE FROM contacts WHERE id = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_query, (id,))
                self.conn.commit()
                logger.info(f"Deleted {id} from database")
        except Exception as error:
            logger.error("Error deleting contact:", error)
            # self.conn.rollback()

    def handle_sf_contacts_list(self, contacts_list: list[dict]):
        for contact in contacts_list:
            contact = PersonDTO.from_sf_contact(contact)
            try:
                self.insert_contact(contact)
                logger.info(f"Inserted person: {contact.name}")
            except Exception as e:
                logger.warning(f"Failed to insert contact: {e}")

    def _get_attribute(
        self, id_or_uuid: str | int, attribute: str
    ) -> Optional[Union[str, List[str]]]:
        select_query = (
            f"SELECT {attribute} FROM contacts WHERE "
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
