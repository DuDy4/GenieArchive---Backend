import traceback

from typing import Optional, Union, List

import psycopg2

from data.data_common.data_transfer_objects.person_dto import PersonDTO
from loguru import logger


class ContactsRepository:
    def __init__(self, conn):
        self.conn = conn
        # self.cursor = conn.cursor()
        self.create_table_if_not_exists()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS contacts (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            owner_id VARCHAR,
            salesforce_id VARCHAR,
            name VARCHAR,
            company VARCHAR,
            email VARCHAR,
            linkedin VARCHAR,
            position VARCHAR,
            timezone VARCHAR
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
        except Exception as error:
            logger.error("Error creating table:", error)
            # self.conn.rollback()

    def insert_contact(self, contact: PersonDTO, salesforce_id) -> str | None:
        """
        :param owner_id: id of the tenant who own of the contact
        :param contact: PersonDTO object with contact data to insert into database
        :param salesforce_id: salesforce id of the contact
        :return the id of the newly created contact in database:
        """
        insert_query = """
        INSERT INTO contacts ( salesforce_id, uuid, owner_id, name, company, email, linkedin, position, timezone)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        contact_data = contact.to_tuple()
        try:
            if not self.exists(contact.uuid):
                with self.conn.cursor() as cursor:
                    cursor.execute(insert_query, (salesforce_id,) + contact_data)
                    self.conn.commit()
                    contact_id = cursor.fetchone()[0]
                    logger.info(
                        f"Inserted contact to database. contact id: {contact_id}"
                    )
                    return contact_id
            else:
                logger.error(f"Contact with uuid {contact.uuid} already exists")
        except psycopg2.Error as error:
            # self.conn.rollback()
            raise Exception(f"Error inserting contact, because: {error.pgerror}")

    def exists(self, uuid: str) -> bool:
        exists_query = "SELECT 1 FROM contacts WHERE uuid = %s;"

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, (uuid,))
                result = cursor.fetchone() is not None
                return result
        except psycopg2.Error as error:
            logger.error(f"Error checking existence of uuid {uuid}: {error}")
            traceback.print_exc()
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
            return False

    def exists_salesforce_id(self, salesforce_id) -> bool:
        """
        Check if a contact with the same salesforce_id already exists in the database
        and return the uuid of the contact if it exists

        :param salesforce_id: salesforce id of the contact
        :return: uuid of the contact if it exists, otherwise False
        """
        exists_query = "SELECT uuid FROM contacts WHERE salesforce_id = %s;"

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, (salesforce_id,))
                result = cursor.fetchone()
                return result[0] if result else False
        except psycopg2.Error as error:
            logger.error(
                f"Error checking existence of salesforce_id {salesforce_id}: {error}"
            )
            traceback.print_exc()
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
            return False

    def exists_identity(self, name, email):
        """
        Check if an identity with the same name and email already exists in the database
        (Assuming that name and email are unique identifiers for a person)
        """

        if email is None:
            exists_query = (
                "SELECT uuid FROM contacts WHERE name = %s AND email IS NULL;"
            )
            query_params = (name,)
        else:
            exists_query = "SELECT uuid FROM contacts WHERE name = %s AND email = %s;"
            query_params = (name, email)
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, query_params)
                result = cursor.fetchone()
                logger.info(
                    f"Identity existence in database for {name} and {email}: {result}"
                )
                return result[0] if result else None
        except Exception as error:
            logger.error("Error checking existence of identity:", error)
        return False

    def exists_all(self, person: PersonDTO):
        """
        Check if a person with all the same attributes already exists in the database
        """
        exists_query = """
        SELECT 1 FROM contacts
        WHERE uuid = %s AND name = %s AND company = %s AND email = %s
        AND linkedin = %s AND position = %s AND timezone = %s;
        """
        if person.email is None:
            exists_query.replace("AND email = %s", "AND email IS NULL")
        person_data = person.to_tuple()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, person_data[0:1] + person_data[2:])
                logger.debug(cursor.query)
                person = cursor.fetchone()
                logger.debug(f"Person existence in database: {person}")
                result = person is not None
                logger.info(f"Person existence in database: {result}")
                return bool(result)
        except Exception as error:
            logger.error("Error checking existence of person:", error)
        return False

    def get_contact_id_by_uuid(self, uuid):
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

    def get_contact_id_by_salesforce_id(self, salesforce_id):
        select_query = "SELECT id FROM contacts WHERE salesforce_id = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (salesforce_id,))
                row = cursor.fetchone()
                if row:
                    return row[0]
                else:
                    logger.error(f"Error with getting contact id for {salesforce_id}")
                    traceback.print_exc()

        except Exception as error:
            logger.error("Error fetching id by salesforce_id:", error)
        return None

    def get_contact_by_salesforce_id(
        self, tenant_id, salesforce_id: str
    ) -> Optional[PersonDTO]:
        select_query = (
            "SELECT * FROM contacts WHERE salesforce_id = %s AND owner_id = %s;"
        )
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (salesforce_id, tenant_id))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got {row[2]} from database")
                    return PersonDTO(*row[2:])

        except Exception as error:
            logger.error("Error fetching contact by salesforce_id:", error)
            traceback.print_exc()
        return None

    def get_contact_by_id(self, id: str) -> Optional[PersonDTO]:
        select_query = "SELECT * FROM contacts WHERE id = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (id,))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got {row[4]} from database")
                    return PersonDTO(*row[2:])

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
                    logger.info(f"Got {row[4]} from database")
                    return PersonDTO(*row[2:])

        except Exception as error:
            logger.error("Error fetching contact by id:", error)
        return None

    def get_name(self, id_or_uuid: str | int) -> Optional[str]:
        return self._get_attribute(id_or_uuid, "name")

    def get_salesforce_id(self, id_or_uuid: str | int) -> Optional[str]:
        return self._get_attribute(id_or_uuid, "salesforce_id")

    def get_company(self, id_or_uuid: str | int) -> Optional[str]:
        return self._get_attribute(id_or_uuid, "company")

    def get_email(self, id_or_uuid: str | int) -> Optional[str]:
        return self._get_attribute(id_or_uuid, "email")

    def get_linkedin_url(self, id_or_uuid: str | int) -> Optional[str]:
        return self._get_attribute(id_or_uuid, "linkedin")

    def get_position(self, id_or_uuid: str | int) -> Optional[str]:
        return self._get_attribute(id_or_uuid, "position")

    def get_timezone(self, id_or_uuid: str | int) -> Optional[str]:
        return self._get_attribute(id_or_uuid, "timezone")

    def update_contact(self, contact: PersonDTO):
        update_query = """
        UPDATE contacts
        SET name = %s, company = %s, email = %s, linkedin = %s, position = %s, timezone = %s
        WHERE uuid = %s;
        """
        if contact.email is None:
            update_query = update_query.replace("email = %s,", "email IS NULL,")
        contact_data = contact.to_tuple()
        logger.debug(f"Contact data: {contact_data}")
        contact_data = contact_data[2:] + (contact_data[0],)
        logger.debug(f"Contact data: {contact_data}")
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, contact_data)
                logger.debug(cursor.query)
                self.conn.commit()
                logger.info(f"Updated {contact.name} in database")
                return True
        except Exception as error:
            logger.error("Error updating contact:", error)
            traceback.print_exc()

    def delete_contact(self, id: str):
        delete_query = "DELETE FROM contacts WHERE id = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_query, (id,))
                self.conn.commit()
                logger.info(f"Deleted from database")
        except Exception as error:
            logger.error("Error deleting contact:", error)
            # self.conn.rollback()

    def search_contact(
        self, name: str, company: str, email: str
    ) -> Optional[PersonDTO]:
        self.create_table_if_not_exists()
        search_query = (
            "SELECT * FROM contacts WHERE name = %s OR company = %s OR email = %s;"
        )
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(search_query, (name, company, email))
                rows = cursor.fetchall()
                if rows:
                    logger.info(f"Found {len(rows)} contacts")
                    persons_list = [PersonDTO(*row[1:]) for row in rows]
                    if len(persons_list) > 1:
                        logger.warning(
                            "Multiple contacts found by the same credentials"
                        )
                    return True
                else:
                    logger.info("No contact found")
                    return None
        except Exception as error:
            logger.error("Error searching contact:", error)

    def handle_sf_contacts_list(self, tenant_id: str, contacts_list: list[dict]):
        """
        Insert or update contacts from salesforce to the database,
        and return the list of changed contacts (from last time we fetched contact from salesforce) that we need to
        check if the personal data has changed

        :param contacts_list: list of contacts from salesforce
        :return: list of changed contacts
        """
        self.create_table_if_not_exists()
        changed_contacts = []
        for contact in contacts_list:
            logger.info(f"Handling contact: {contact}")
            contact_id = contact.get("Id")
            contact = PersonDTO.from_sf_contact(contact, owner_id=tenant_id)
            logger.info(f"Contact: {contact}")
            try:
                uuid = self.exists_salesforce_id(contact_id)
                if uuid:
                    contact.uuid = uuid

                    # check if the contact was changed since the last time we fetched it
                    if not self.exists_all(contact):
                        self.update_contact(contact)
                        changed_contacts.append(contact.to_dict())
                else:
                    self.insert_contact(contact, contact_id)
                    logger.info(f"Inserted person: {contact.name}")
                    changed_contacts.append(contact)
            except Exception as e:
                logger.warning(f"Failed to insert contact: {e}")
        return changed_contacts

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
