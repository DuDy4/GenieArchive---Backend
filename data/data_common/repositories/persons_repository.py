import traceback

import psycopg2

from data.data_common.data_transfer_objects.person_dto import PersonDTO
from loguru import logger


class PersonsRepository:
    def __init__(self, conn):
        self.conn = conn
        self.create_table_if_not_exists()

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

    def insert(self, person: PersonDTO) -> str | None:
        """
        :param person: PersonDTO object with person data to insert into database
        :return the id of the newly created person in database:
        """
        insert_query = """
        INSERT INTO persons (uuid, name, company, email, linkedin, position, timezone)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
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
                return person.uuid
        except psycopg2.Error as error:
            logger.error(f"Error inserting person: {error.pgerror}")
            traceback.print_exc()
            raise Exception(f"Error inserting person, because: {error.pgerror}")

    def exists(self, uuid: str) -> bool:
        logger.info(f"About to check if uuid exists: {uuid}")
        exists_query = "SELECT 1 FROM persons WHERE uuid = %s;"
        try:
            with self.conn.cursor() as cursor:
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
        exists_query = "SELECT uuid FROM persons WHERE  linkedin = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, (person.linkedin,))
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
        with self.conn.cursor() as cursor:
            cursor.execute(query, (email,))
            return cursor.fetchone() is not None
        
    def get_person_by_email(self, email: str) -> PersonDTO | None:
        select_query = """
        SELECT * FROM persons WHERE email = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email,))
                person = cursor.fetchone()
                if person:
                    logger.info(f"Got person with email {email}")
                    return PersonDTO.from_tuple(person[1:])
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
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
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

        try:
            with self.conn.cursor() as cursor:
                logger.debug(f"Executing query: {query}")
                logger.debug(f"UUIDs: {uuids}")
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

    def get_person_complete_data(self, email: str) -> PersonDTO:
        if not email:
            return None

        # Generate a SQL query with a list of placeholders for UUIDs
        query = f"""SELECT
                    p.uuid,
                    p.name,
                    p2.company,
                    p.email ,
                    p2.position,
                    p.linkedin,
                    h.hobby_name,
                    h.icon_url,
                    connections_data->'name' as connection_name,
                    connections_data->'image_url' as connection_image
                FROM
                    persons p
                LEFT JOIN
                    profiles p2 ON p.uuid = p2.uuid
                LEFT JOIN
                    LATERAL jsonb_array_elements(p2.connections) AS connections_data ON TRUE
                LEFT JOIN
                    LATERAL jsonb_array_elements_text(p2.hobbies) AS hobby_uuid ON TRUE
                LEFT JOIN
                    hobbies h ON hobby_uuid.value = h.uuid
                WHERE p.email = %s;

                """

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (email,))
                row = cursor.fetchone()
                logger.info(f"Retrieved person for email: {email}")
                if row:
                    # return PersonDTO(row[0], row[1], row[2], row[3], row[4], row[5], "")
                    return {
                        "uuid": row[0],
                        "name": row[1],
                        "company": row[2],
                        "email": row[3],
                        "position": row[4],
                        "linkedin": row[5],
                        "hobbies": [{"hobby": row[6], "icon_url": row[7]}],
                        "relevant_connections": [
                            {
                                "name": row[8],
                                "picture_url": row[9],
                                "linkedin_url": "",
                            }
                        ],
                    }
                else:
                    logger.error(f"Error with getting person for {email}")
                    traceback.print_exc()
                return None
        except psycopg2.Error as error:
            logger.error(f"Error fetching profile by email: {error.pgerror}")
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

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (linkedin, email))
                self.conn.commit()
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
        try:
            with self.conn.cursor() as cursor:
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

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(base_query, query_values)
                self.conn.commit()
                logger.info("Updated person in database")
                return True
        except psycopg2.Error as error:
            raise Exception(f"Error updating person, because: {error.pgerror}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def save_person(self, person: PersonDTO):
        self.create_table_if_not_exists()
        uuid = self.exists_properties(person)
        logger.info(f"Person exists: {uuid}")
        if uuid:
            self.update(person)
            return uuid
        else:
            return self.insert(person)

    def find_person_by_email(self, email):
        query = """
        SELECT uuid, name, company, email, linkedin, position, timezone FROM persons WHERE email = %s;
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (email,))
            person = cursor.fetchone()
            logger.info(f"Person by email {email}: {person}")
            if person:
                return PersonDTO.from_tuple(person)

    def get_person_email(self, uuid):
        query = """
        SELECT email FROM persons WHERE uuid = %s;
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query, (uuid,))
            return cursor.fetchone()[0]
