from typing import Optional
import json
import psycopg2
import traceback

from common.genie_logger import GenieLogger
logger = GenieLogger()


class PersonalDataRepository:
    FETCHED = "FETCHED"
    TRIED_BUT_FAILED = "TRIED_BUT_FAILED"

    def __init__(self, conn):
        self.conn = conn
        self.create_table_if_not_exists()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS personalData (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            name VARCHAR,
            email VARCHAR,
            linkedin_url VARCHAR,
            personal_data JSONB,
            status TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
                # logger.info("Created personalData table")
        except psycopg2.Error as e:
            logger.error(f"Error creating table: {e.pgcode}: {e.pgerror}")
        except Exception as e:
            logger.error(f"Error creating table: {repr(e)}")

    def insert(
        self,
        uuid: str,
        name: Optional[str] = None,
        email: Optional[str] = None,
        linkedin_url: Optional[str] = None,
        personal_data: Optional[json] = None,
        status: str = "FETCHED",
    ):
        """
        Insert a new personalData into the database.

        :param uuid: Unique identifier for the personalData.
        :param name: Name of the person (optional).
        :param email: Email of the person (optional).
        :param linkedin_url: LinkedIn URL of the person (optional).
        :param personal_data: Personal data of the person (optional).
        :param status: Status of the personalData (default is "FETCHED").
        """
        insert_query = """
        INSERT INTO personalData (uuid, name, email, linkedin_url, personal_data, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        if self.exists_uuid(uuid):
            logger.error("Personal data with this UUID already exists")
            return
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    insert_query,
                    (uuid, name, email, linkedin_url, personal_data, status),
                )
                self.conn.commit()
                logger.info("Inserted personalData into database")
        except psycopg2.IntegrityError as e:
            logger.error("personalData with this UUID already exists")
            self.conn.rollback()
            traceback.print_exc()
        except Exception as e:
            logger.error("Error inserting personalData:", e)
            logger.error(traceback.format_exc())
            self.conn.rollback()

    def exists_uuid(self, uuid: str) -> bool:
        """
        Check if a personalData with the given UUID exists in the database.

        :param uuid: Unique identifier for the personalData.
        :return: True if personalData exists, False otherwise.
        """
        self.create_table_if_not_exists()
        select_query = """
        SELECT EXISTS (
            SELECT 1
            FROM personalData
            WHERE uuid = %s
        )
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                exists = cursor.fetchone()[0]
                return exists
        except psycopg2.Error as e:
            logger.error("Error checking for existing personalData:", e)
            return False
        except Exception as e:
            logger.error("Error checking personalData existence:", e)
            return False

    def exists_linkedin_url(self, linkedin_url: str) -> bool:
        """
        Check if a personalData with the given linkedin_url exists in the database.

        :param linkedin_url: LinkedIn URL of the person.
        :return: True if personalData exists, False otherwise.
        """
        self.create_table_if_not_exists()
        select_query = """
        SELECT EXISTS (
            SELECT 1
            FROM personalData
            WHERE linkedin_url = %s
        )
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (linkedin_url,))
                exists = cursor.fetchone()[0]
                return exists
        except psycopg2.Error as e:
            logger.error("Error checking for existing personalData:", e)
            return False
        except Exception as e:
            logger.error("Error checking personalData existence:", e)
            return False

    def get_personal_data(self, uuid: str) -> Optional[dict]:
        """
        Retrieve personal data associated with an uuid.

        :param uuid: Unique identifier for the personalData.
        :return: Personal data as a json if personalData exists, None otherwise.
        """
        self.create_table_if_not_exists()
        select_query = """
        SELECT personal_data
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                personal_data = cursor.fetchone()
                if personal_data:
                    return personal_data[0]
                else:
                    logger.warning("personalData was not found in db by uuid")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving personal data: {e}", e)
            traceback.format_exc()
            return None

    def get_personal_data_by_linkedin(
        self, linkedin_profile_url: str
    ) -> Optional[dict]:
        """
        Retrieve personal data associated with an uuid.

        :param linkedin_profile_url: LinkedIn profile URL of the person.
        :return: Personal data as a json if personalData exists, None otherwise.
        """
        logger.info(f"Got get request for {linkedin_profile_url}")
        self.create_table_if_not_exists()
        select_query = """
        SELECT
        FROM personalData
        WHERE linkedin_url = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (linkedin_profile_url,))
                personal_data = cursor.fetchone()
                if personal_data:
                    return personal_data[1:]
                else:
                    logger.warning("personalData was not found in db by linkedin url")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving personal data: {e}", e)
            traceback.format_exc()
            return None

    def get_personal_data_by_email(self, email_address: str):
        """
        Retrieve personal data associated with an email address.

        :param email_address: Email address of the person.
        :return: Personal data as a json if personalData exists, None otherwise.
        """
        self.create_table_if_not_exists()
        select_query = """
        SELECT personal_data
        FROM personalData
        WHERE email = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email_address,))
                personal_data = cursor.fetchone()
                if personal_data:
                    return personal_data[0]
                else:
                    logger.warning("personalData was not found in db by email address")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving personal data: {e}", e)
            traceback.format_exc()
            return None

    def get_personal_uuid_by_email(self, email_address: str):
        """
        Retrieve personal data uuid associated with an email address.

        :param email_address: Email address of the person.
        :return: Personal data uuid if personalData exists, None otherwise.
        """
        self.create_table_if_not_exists()
        select_query = """
        SELECT uuid
        FROM personalData
        WHERE email = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email_address,))
                uuid = cursor.fetchone()
                if uuid:
                    return uuid[0]
                else:
                    logger.warning("personalData uuid was not found in db by email address")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving personal data: {e}", e)
            traceback.format_exc()
            return None

    def update(self, uuid, personal_data):
        """
        Save personal data to the database.

        :param uuid: Unique identifier for the personalData.
        :param personal_data: Personal data to save.
        """
        update_query = """
        UPDATE personalData
        SET personal_data = %s, last_updated = CURRENT_TIMESTAMP, status = 'FETCHED'
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (personal_data, uuid))
                self.conn.commit()
                logger.info("Updated personal data")
        except psycopg2.Error as e:
            logger.error(f"Failed to executre personal data query: {update_query}")
            logger.error("psycopg2 Error updating personal data:", str(e))
            self.conn.rollback()
        except Exception as e:
            logger.error("Exception Error updating personal data:", str(e))
            self.conn.rollback()
        return

    def update_uuid(self, uuid, uuid1):
        """
        Update the UUID for a profile.

        :param uuid: Old UUID for the profile.
        :param uuid1: New UUID for the profile.
        """
        update_query = """
        UPDATE personalData
        SET uuid = %s
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (uuid1, uuid))
                self.conn.commit()
                logger.info("Updated UUID")
        except psycopg2.Error as e:
            logger.error("Error updating UUID:", e)
            traceback.print_exc()
            self.conn.rollback()
        except Exception as e:
            logger.error("Error updating UUID:", e)
            traceback.print_exc()
            self.conn.rollback()
        return

    def save_personal_data(self, person, personal_data: dict | str, status: str):
        """
        Save personal data to the database.

        :param person: Person object.
        :param personal_data: Personal data to save.
        """
        self.create_table_if_not_exists()
        if isinstance(personal_data, dict):
            personal_data = json.dumps(personal_data)
    
        if not self.exists_uuid(person.uuid):
            self.insert(
                uuid=person.uuid,
                name=person.name,
                email=person.email,
                linkedin_url=person.linkedin,
                personal_data=personal_data,
                status=status,
            )
            return
        self.update(person.uuid, personal_data)
        # This use case is for when we try to fetch personal data by email and fail and then someone updates 
        # linkekdin url and we are able to fetch personal data but linkedin url is still missing from table
        if person and person.linkedin and not self.exists_linkedin_url(person.linkedIn):
            self.update_linkedin_url(person.uuid, person.linkedIn)
        return
    
    def update_linkedin_url(self, uuid, linkedin_url):
        """
        Update the LinkedIn URL for a profile.

        :param uuid: Unique identifier for the profile.
        :param linkedin_url: LinkedIn URL to update.
        """
        update_query = """
        UPDATE personalData
        SET linkedin_url = %s
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (linkedin_url, uuid))
                self.conn.commit()
                logger.info("Updated LinkedIn URL")
        except psycopg2.Error as e:
            logger.error("Error updating LinkedIn URL:", e)
            traceback.print_exc()
            self.conn.rollback()
        except Exception as e:
            logger.error("Error updating LinkedIn URL:", e)
            traceback.print_exc()
            self.conn.rollback()
        return

    def get_last_updated(self, uuid):
        """
        Retrieve the last updated timestamp for a profile.

        :param uuid: Unique identifier for the profile.
        :return: Timestamp if profile exists, None otherwise.
        """
        select_query = """
        SELECT last_updated
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                last_updated = cursor.fetchone()
                if last_updated:
                    return last_updated[0]
                else:
                    logger.warning("Profile was not found")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving last updated timestamp: {e}", e)
            traceback.format_exc()
            return None

    def get_email(self, uuid):
        """
        Retrieve the email address for a profile.

        :param uuid: Unique identifier for the profile.
        :return: Email address if profile exists, None otherwise.
        """
        select_query = """
        SELECT email
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                email = cursor.fetchone()
                if email:
                    return email[0]
                else:
                    logger.warning("Profile was not found")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving email address: {e}", e)
            traceback.format_exc()
            return None

    def get_personal_data_row(self, uuid):
        """
        Retrieve the personal data row as a dict with column names as keys.
        """
        select_query = """
        SELECT *
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                personal_data = cursor.fetchone()
                if personal_data:
                    personal_data_dict = {
                        "uuid": personal_data[1],
                        "name": personal_data[2],
                        "email": personal_data[3],
                        "linkedin_url": personal_data[4],
                        "personal_data": personal_data[5],
                        "status": personal_data[6],
                        "last_updated": personal_data[7],
                    }
                    return personal_data_dict
                else:
                    logger.warning("personalData object was not found in db by uuid")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving personal data: {e}", e)
            traceback.format_exc()
            return None

    def get_social_media_links(self, uuid: str):
        """
        Retrieve social media links for a profile.

        :param uuid: Unique identifier for the profile.
        :return: Social media links if profile exists, None otherwise.
        """
        select_query = """
        SELECT personal_data -> 'profiles'
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                result = cursor.fetchone()
                logger.info(f"Got result: {result}")
                if result:
                    profiles_data = result[0]
                    if profiles_data:
                        # Assuming profiles_data is a JSONB object and you want to extract specific data.
                        return profiles_data
                    else:
                        logger.warning("No personal data found")
                        return None
                else:
                    logger.warning("Personal data was not found")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving social media links: {e}", e)
            traceback.format_exc()
            return None

    def get_status(self, existing_uuid):
        """
        Retrieve the status of a profile.

        :param existing_uuid: Unique identifier for the profile.
        :return: Status if profile exists, None otherwise.
        """
        select_query = """
        SELECT status
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (existing_uuid,))
                status = cursor.fetchone()
                if status:
                    return status[0]
                else:
                    logger.warning("Profile was not found")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving status: {e}", e)
            traceback.format_exc()
            return None
