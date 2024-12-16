import traceback

import psycopg2

from uuid import UUID

from data.data_common.data_transfer_objects.person_dto import PersonDTO, PersonStatus
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection

logger = GenieLogger()



class PersonsRepository:
    def __init__(self):
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS persons (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            name VARCHAR,
            company VARCHAR,
            email VARCHAR,
            linkedin VARCHAR,
            position VARCHAR,
            timezone VARCHAR,
            last_message_sent_at TIMESTAMP,
            status TEXT
        );
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
            except Exception as error:
                logger.error("Error creating table:", error)

    def insert(self, person: PersonDTO) -> str | None:
        """
        :param person: PersonDTO object with person data to insert into database
        :return the id of the newly created person in database:
        """
        insert_query = """
        INSERT INTO persons (uuid, name, company, email, linkedin, position, timezone, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert person: {person}")
        person_data = person.to_tuple()

        logger.info(f"About to insert person data: {person_data}")
        with db_connection() as conn:

            try:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, person_data + (PersonStatus.IN_PROGRESS.value,))
                    conn.commit()
                    person_id = cursor.fetchone()[0]
                    logger.info(f"Inserted person to database. Person id: {person_id}")
                    return person.uuid
            except psycopg2.IntegrityError:
                logger.warning(f"Duplicate entry for UUID: {person.uuid}, skipping insert.")
                conn.rollback()  # Rollback the transaction in case of duplicate key
                return None  # Return None or some other indication that insert was skipped
            except psycopg2.Error as error:
                logger.error(f"Error inserting person: {error.pgerror}")
                conn.rollback()  # Rollback the transaction for any other database error
                traceback.print_exc()
                raise Exception(f"Error inserting person, because: {error.pgerror}")

    def exists(self, uuid: str) -> bool:
        logger.info(f"About to check if uuid exists: {uuid}")
        exists_query = "SELECT 1 FROM persons WHERE uuid = %s;"
        with db_connection() as conn:

            try:
                with conn.cursor() as cursor:
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

    def exists_properties(self, person: PersonDTO) -> bool:
        logger.info(f"About to check if person exists: {person}")
        exists_query = "SELECT uuid FROM persons WHERE email = %s;"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(exists_query, (person.email,))
                    result = cursor.fetchone()
                    return result[0] if result else None
            except psycopg2.Error as error:
                logger.error(f"Error checking existence of person ({person.name}): {error}")
                return False
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return False

    def exist_email(self, email):
        query = """
        SELECT 1 FROM persons WHERE email = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (email,))
                    return cursor.fetchone() is not None
            except psycopg2.Error as error:
                logger.error(f"Error checking existence of email {email}: {error}")
                return False
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return False

    def get_person_by_email(self, email: str) -> PersonDTO | None:
        select_query = """
        SELECT uuid, name, company, email, linkedin, position, timezone FROM persons WHERE email = %s;
        """
        with db_connection() as conn:

            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (email,))
                    person = cursor.fetchone()
                    if person:
                        logger.info(f"Got person with email {email}")
                        return PersonDTO.from_tuple(person)
                    logger.info(f"Person with email {email} does not exist")
                    return None
            except psycopg2.Error as error:
                logger.error(f"Error getting person by email: {error}")
                traceback.print_exc()
                return None

    def get_person(self, uuid: str) -> PersonDTO | None:
        select_query = """
        SELECT * FROM persons WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (str(uuid),))
                    person = cursor.fetchone()
                    if person:
                        logger.info(f"Got person with uuid {uuid}")
                        return PersonDTO.from_tuple(person[1:])
                    logger.info(f"Person with uuid {uuid} does not exist")
                    return None
            except psycopg2.Error as error:
                logger.error(f"Error getting person: {error}")
                traceback.print_exc()
                return None

    def get_emails_list(self, uuids: list[str], name: str) -> list[str]:
        """
        Get a list of email addresses by a list of UUIDs.

        :param uuids: List of UUIDs to fetch emails for.
        :param name: Name of the person to fetch emails for.
        :return: List of email addresses correlated to the provided UUIDs.
        """
        if not uuids:
            return []

        # Generate a SQL query with a list of placeholders for UUIDs
        query = f"SELECT email FROM persons WHERE uuid IN ({', '.join(['%s'] * len(uuids))})"
        parameters = uuids

        if name:
            query += " AND name ILIKE %s"
            parameters.append(f"%{name}%")
        with db_connection() as conn:

            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, tuple(parameters))
                    rows = cursor.fetchall()
                    logger.info(f"Retrieved emails for UUIDs: {rows}")
                    emails = [row[0] for row in rows if rows]
                    logger.info(f"Retrieved emails for UUIDs: {emails}")
                    return emails
            except psycopg2.Error as error:
                logger.error(f"Error fetching emails by UUIDs: {error.pgerror}")
                traceback.print_exc()
                return []
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                traceback.print_exc()
                return []

    def update_person_linkedin(self, email: str, linkedin: str) -> bool:
        """
        Update a person's LinkedIn URL by their email address.

        :param email: Email address of the person to update.
        :param linkedin: New LinkedIn URL to set.
        :return: True if the update was successful, False otherwise.
        """
        if not email:
            return False

        update_query = """
        UPDATE persons
        SET linkedin = %s
        WHERE email = %s;
        """
        with db_connection() as conn:

            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (linkedin, email))
                    conn.commit()
                    logger.info(f"Updated LinkedIn URL for {email}")
                    return True
            except psycopg2.Error as error:
                logger.error(f"Error updating LinkedIn URL by email: {error.pgerror}")
                traceback.print_exc()
                return False
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                traceback.print_exc()
                return

    def get_person_id(self, uuid):
        select_query = "SELECT id FROM persons WHERE uuid = %s;"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid,))
                    row = cursor.fetchone()
                    if row:
                        logger.info(f"Got {row[0]} from database")
                        return row[0]
                    else:
                        logger.error(f"Error with getting person id for {uuid}")
            except Exception as error:
                logger.error("Error fetching id by uuid:", error)
            return None

    def update(self, person: PersonDTO):
        base_query = """
        UPDATE persons
        SET name = %s, company = %s, email = %s, position = %s, timezone = %s
        """
        query_values = [
            person.name,
            person.company,
            person.email,
            person.position,
            person.timezone,
        ]

        if person.linkedin is not None:
            base_query += ", linkedin = %s"
            query_values.append(person.linkedin)

        base_query += " WHERE uuid = %s"
        query_values.append(person.uuid)
        with db_connection() as conn:

            try:
                with conn.cursor() as cursor:
                    cursor.execute(base_query, query_values)
                    conn.commit()
                    logger.info("Updated person in database")
                    return True
            except psycopg2.Error as error:
                raise Exception(
                    f"Error updating person, because: {error.pgerror if hasattr(error, 'pgerror') else error}"
                )
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return False

    def save_person(self, person: PersonDTO):
        self.create_table_if_not_exists()
        uuid = self.exists_properties(person)
        logger.info(f"Result of exists_properties: {uuid}")
        if uuid:
            self.update(person)
            return uuid
        else:
            return self.insert(person)

    def find_person_by_email(self, email):
        query = """
        SELECT uuid, name, company, email, linkedin, position, timezone FROM persons WHERE email = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                        cursor.execute(query, (email,))
                        person = cursor.fetchone()
                        logger.info(f"Person by email {email}: {person}")
                        if person:
                            return PersonDTO.from_tuple(person)
                        return None
            except psycopg2.Error as error:
                logger.error(f"Error finding person by email: {error}")
                return None

    def get_person_email(self, uuid):
        query = """
        SELECT email FROM persons WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (uuid,))
                    return cursor.fetchone()[0]
            except psycopg2.Error as error:
                logger.error(f"Error getting person email: {error}")
                return None

    def get_all_persons_with_missing_attribute(self):
        query = """
        SELECT p.uuid, p.name, p.company, p.email, p.linkedin, p.position, p.timezone
        FROM persons p
        INNER JOIN personaldata pd ON p.uuid = pd.uuid
        WHERE ((TRIM(p.linkedin) IS NULL OR TRIM(p.linkedin) = '')
               OR (TRIM(p.name) IS NULL OR TRIM(p.name) = '')
               OR (TRIM(p.company) IS NULL OR TRIM(p.company) = '')
               OR (TRIM(p.position) IS NULL OR TRIM(p.position) = ''))
          AND (pd.pdl_status = 'FETCHED' OR pd.apollo_status = 'FETCHED')
        order by p.id desc
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return [PersonDTO.from_tuple(row) for row in cursor.fetchall()]
            except psycopg2.Error as error:
                logger.error(f"Error getting persons with missing attributes: {error}")
                return []

    def get_last_message_sent_at_by_email(self, email):
        query = """
        SELECT last_message_sent_at FROM persons WHERE email = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                        cursor.execute(query, (email,))
                        return cursor.fetchone()[0]
            except psycopg2.Error as error:
                logger.error(f"Error getting last message sent at by email: {error}")
                return None

    def update_last_message_sent_at_by_email(self, email):
        query = """
        UPDATE persons
        SET last_message_sent_at = NOW()
        WHERE email = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (email,))
                    conn.commit()
                    logger.info(f"Updated last message sent at for {email}")
            except psycopg2.Error as error:
                logger.error(f"Error updating last message sent at by email: {error}")
                return None

    def update_status(self, uuid: str | UUID, status: PersonStatus):
        query = """
        UPDATE persons
        SET status = %s
        WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (status.value, uuid))
                    conn.commit()
                    logger.info(f"Updated status for {uuid}")
            except psycopg2.Error as error:
                logger.error(f"Error updating status: {error}")
                return None

    def update_status_by_email(self, email: str, status: PersonStatus):
        query = """
        UPDATE persons
        SET status = %s
        WHERE email = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (status.value, email))
                    conn.commit()
                    logger.info(f"Updated status for {email}")
            except psycopg2.Error as error:
                logger.error(f"Error updating status by email: {error}")
                return None


    def remove_last_sent_message(self, uuid):
        query = """
        UPDATE persons
        SET last_message_sent_at = NULL
        WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (uuid,))
                    conn.commit()
                    logger.info(f"Removed last sent message for {uuid}")
            except psycopg2.Error as error:
                logger.error(f"Error removing last sent message: {error}")
                return None
