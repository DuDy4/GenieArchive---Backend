from typing import Optional, List
import json
import psycopg2
import traceback
from datetime import datetime, timedelta

from data.data_common.utils.postgres_connector import get_db_connection

from common.utils import env_utils
from data.data_common.data_transfer_objects.company_dto import SocialMediaLinks
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.data_transfer_objects.news_data_dto import NewsData, SocialMediaPost
from common.genie_logger import GenieLogger
from data.data_common.utils.str_utils import to_custom_title_case

logger = GenieLogger()

LAST_UPDATED_NEWS_INTERVAL = env_utils.get("LAST_UPDATED_NEWS_INTERVAL", "14")


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
            pdl_personal_data JSONB,
            pdl_status TEXT,
            pdl_last_updated TIMESTAMP,
            apollo_personal_data JSONB,
            apollo_status TEXT,
            apollo_last_updated TIMESTAMP,
            news JSONB,
            news_status TEXT,
            news_last_updated TIMESTAMP
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
        except psycopg2.Error as e:
            logger.error(f"Error creating table: {e.pgcode}: {e.pgerror}")
            # self.conn.rollback()
        except Exception as e:
            logger.error(f"Error creating table: {repr(e)}")
            # self.conn.rollback()

    def insert(
        self,
        uuid: str,
        name: Optional[str] = None,
        email: Optional[str] = None,
        linkedin_url: Optional[str] = None,
        pdl_personal_data: Optional[dict] = None,
        apollo_personal_data: Optional[dict] = None,
        pdl_status: str = None,
        apollo_status: str = None,
    ):
        # Start with the mandatory columns and values
        columns = ["uuid", "name", "email", "linkedin_url"]
        values = [uuid, name, email, linkedin_url]

        # Append optional columns and values
        if pdl_personal_data:
            columns.append("pdl_personal_data")
            columns.append("pdl_status")
            values.append(
                json.dumps(pdl_personal_data) if isinstance(pdl_personal_data, dict) else pdl_personal_data
            )
            values.append(pdl_status)
        elif pdl_status:
            columns.append("pdl_status")
            values.append(pdl_status)

        if apollo_personal_data:
            columns.append("apollo_personal_data")
            columns.append("apollo_status")
            values.append(
                json.dumps(apollo_personal_data)
                if isinstance(apollo_personal_data, dict)
                else apollo_personal_data
            )
            values.append(apollo_status)
        elif apollo_status:
            columns.append("apollo_status")
            values.append(apollo_status)

        # Build the insert query dynamically
        insert_query = f"""
        INSERT INTO personalData ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(values))})
        """

        if self.exists_uuid(uuid):
            logger.error("Personal data with this UUID already exists")
            return

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, tuple(values))
                self.conn.commit()

                # Conditionally update the timestamps
                if pdl_personal_data or pdl_status:
                    cursor.execute(
                        "UPDATE personalData SET pdl_last_updated = CURRENT_TIMESTAMP WHERE uuid = %s",
                        (uuid,),
                    )
                if apollo_personal_data or apollo_status:
                    cursor.execute(
                        "UPDATE personalData SET apollo_last_updated = CURRENT_TIMESTAMP WHERE uuid = %s",
                        (uuid,),
                    )

                self.conn.commit()
                logger.debug(
                    f"Inserted personalData into database: {[(columns[i], values[i]) for i in range(len(columns))]}"
                )
                logger.info("Inserted personalData into database")
        except psycopg2.IntegrityError as e:
            logger.error("PersonalData with this UUID already exists")
            traceback.print_exc()
        except Exception as e:
            logger.error(f"Error inserting personalData: {e}")
            logger.error(traceback.format_exc())

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

    def get_pdl_personal_data(self, uuid: str) -> Optional[dict]:
        """
        Retrieve personal data associated with an uuid.

        :param uuid: Unique identifier for the personalData.
        :return: Personal data as a json if personalData exists, None otherwise.
        """
        self.create_table_if_not_exists()
        select_query = """
        SELECT pdl_personal_data
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                personal_data = cursor.fetchone()
                if personal_data and personal_data[0]:
                    logger.info(f"Got PDL data from DB: {str(personal_data)[:300]}")
                    return (
                        json.loads(personal_data[0])
                        if isinstance(personal_data[0], str)
                        else personal_data[0]
                    )
                else:
                    logger.warning(f"pdl personalData was not found in db by uuid {uuid}")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving personal data: {e}", e)
            traceback.print_exc()
            return None

    def should_do_personal_data_lookup(self, uuid: str) -> bool:
        """
        Check if a there's a potential for personal data update.
        The reasons for additional data lookup are:
        1. The last update was more than 30 days ago.
        2. PDL data exists
        3. Apollo data exists
        4. LinkedIn URL exists

        :param uuid: Unique identifier for the personalData.
        :return: True if personalData exists, False otherwise.
        """
        self.create_table_if_not_exists()
        select_query = """
            SELECT
                CASE
                    WHEN pdl_status = 'TRIED_BUT_FAILED'
                    AND pdl_last_updated > NOW() - INTERVAL '30 days'
                    AND apollo_status = 'TRIED_BUT_FAILED'
                    AND apollo_last_updated > NOW() - INTERVAL '30 days'
                    AND (linkedin_url IS NULL OR linkedin_url = '')
                    THEN true
                    ELSE false
                END AS update_failed_recently
            FROM
                personaldata
            WHERE
                uuid = %s;

        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                update_failed_recently = cursor.fetchone()
                return not update_failed_recently[0]
        except psycopg2.Error as e:
            logger.error("Error checking for existing personalData:", e)
            return False
        except Exception as e:
            logger.error("Error checking personalData existence:", e)
            return False

    def get_apollo_personal_data(self, uuid: str) -> Optional[dict]:
        self.create_table_if_not_exists()
        select_query = """
        SELECT apollo_personal_data
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                personal_data = cursor.fetchone()
                if personal_data and personal_data[0]:
                    logger.info(f"Got personal data from apollo: {str(personal_data)[:300]}")
                    return (
                        json.loads(personal_data[0])
                        if isinstance(personal_data[0], str)
                        else personal_data[0]
                    )
                else:
                    logger.warning("apollo personalData was not found in db by uuid")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving personal data: {e}", e)
            traceback.print_exc()
            return None

    def get_pdl_personal_data_by_linkedin(self, linkedin_profile_url: str) -> Optional[dict]:
        """
        Retrieve personal data associated with an uuid.

        :param linkedin_profile_url: LinkedIn profile URL of the person.
        :return: Personal data as a json if personalData exists, None otherwise.
        """
        logger.info(f"Got get request for {linkedin_profile_url}")
        self.create_table_if_not_exists()
        select_query = """
        SELECT pdl_personal_data
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

    def get_apollo_personal_data_by_linkedin(self, linkedin_profile_url: str) -> Optional[dict]:
        """
        Retrieve personal data associated with an uuid.

        :param linkedin_profile_url: LinkedIn profile URL of the person.
        :return: Personal data as a json if personalData exists, None otherwise.
        """
        logger.info(f"Got get request for {linkedin_profile_url}")
        self.create_table_if_not_exists()
        select_query = """
        SELECT apollo_personal_data
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

    def get_pdl_personal_data_by_email(self, email_address: str):
        """
        Retrieve personal data associated with an email address.

        :param email_address: Email address of the person.
        :return: Personal data as a json if personalData exists, None otherwise.
        """
        self.create_table_if_not_exists()
        select_query = """
        SELECT pdl_personal_data
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

    def get_apollo_personal_data_by_email(self, email_address: str):
        """
        Retrieve personal data associated with an email address.

        :param email_address: Email address of the person.
        :return: Personal data as a json if personalData exists, None otherwise.
        """
        self.create_table_if_not_exists()
        select_query = """
        SELECT apollo_personal_data
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

    def should_do_linkedin_posts_lookup(self, uuid: str) -> bool:
        """
        Check if there's a potential for a LinkedIn posts update.
        The reasons for additional data lookup are:
        1. The last update was more than 7 days ago and the update previously failed.
        2. LinkedIn URL exists.
        3. No 'FETCHED' status in the last 14 days.

        :param uuid: Unique identifier for the personalData.
        :return: True if LinkedIn posts update should be done, False otherwise.
        """
        select_query = f"""
        SELECT
            news_status, news_last_updated
        FROM
            personalData
        WHERE
            uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                result = cursor.fetchone()

                if result is None:
                    logger.error(f"No data found for uuid: {uuid}")
                    return False  # If no row is returned, the check fails.

                # If the news status is 'FETCHED' or 'TRIED_BUT_FAILED' and the last update was within the last 14 days, no update is needed.
                if (result[0] == self.FETCHED or result[0] == self.TRIED_BUT_FAILED) and result[
                    1
                ] < datetime.now() - timedelta(days=14):
                    return True
                else:
                    return False

        except psycopg2.Error as e:
            logger.error("Error checking for existing personalData:", exc_info=True)
            return False
        except Exception as e:
            logger.error("Error checking personalData existence:", exc_info=True)
            return False

    def get_all_uuids_that_should_try_fetch_posts(self) -> List[str]:
        select_query = f"""
        SELECT uuid
        FROM personalData
        WHERE (pdl_status = '{self.FETCHED}' OR apollo_status = '{self.FETCHED}') AND
        (news_status IS NULL OR news_status = 'null' OR
        news_status = 'TRIED_BUT_FAILED'
        AND news_last_updated > NOW() - INTERVAL '{LAST_UPDATED_NEWS_INTERVAL} days'
        AND (linkedin_url IS NULL OR linkedin_url = '')
        OR news_status = 'FETCHED'
        AND news_last_updated > NOW() - INTERVAL '{LAST_UPDATED_NEWS_INTERVAL} days')
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query)
                uuids = cursor.fetchall()
                return [uuid[0] for uuid in uuids]
        except Exception as e:
            logger.error(f"Error retrieving UUIDs that should try posts: {e}", e)
            traceback.format_exc()
            return []

    def _get_news(self, query: str, arg: str):
        try:
            with (self.conn.cursor() as cursor):
                cursor.execute(query, (arg,))
                news = cursor.fetchone()
                if news is None:
                    logger.error(f"No news data for {arg}, and news is null instead of empty list")
                    return []
                else:
                    news = news[0]
                if not news:
                    logger.warning(f"No news data for {arg}")
                    return []
                if len(news) > 2:
                    news = news[:2]
                res_news = [NewsData.from_dict(item) for item in news]
                if not res_news:
                    logger.warning(f"No news data for {arg}")
                    return []
                return res_news
        except psycopg2.Error as error:
            logger.error(f"Error getting news data: {error}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None

    def get_news_data_by_uuid(self, uuid: str):
        query = """SELECT news FROM personalData WHERE uuid = %s;"""
        return self._get_news(query, uuid)

    def get_news_data_by_email(self, email):
        query = """SELECT news FROM personalData WHERE email = %s;"""
        return self._get_news(query, email)

    def update_news_to_db(self, uuid: str, news_data: dict | None, status: str = "FETCHED"):
        """
        Update news data in the personaldata table in the database.

        :param uuid: Unique identifier for the personalData (required).
        :param news_data: A dictionary containing the news data.
        :param status: A string indicating the status of the news ("Fetched" or "Failed to fetch").
        """
        if not uuid:
            logger.error("UUID is None or empty. Cannot update the database.")
            return

        update_query = """
        UPDATE personalData
        SET news = COALESCE(personalData.news, '[]'::jsonb) || %s::jsonb,
            news_status = %s,
            news_last_updated = %s
        WHERE uuid = %s
        """
        try:
            news_last_updated = datetime.now()
            json_news_data = json.dumps(news_data) if news_data else None

            logger.debug(
                f"Updating news in DB, UUID: {uuid}, status: {status}, news_data: {str(json_news_data)[:100]}"
            )

            with self.conn.cursor() as cursor:
                cursor.execute(
                    update_query,
                    (
                        json_news_data,  # Update the news column by appending new data to the existing data
                        status,  # Update the news status
                        news_last_updated,  # Update the last updated timestamp
                        uuid,  # Use the UUID to find the correct row
                    ),
                )
                self.conn.commit()

            logger.info(f"Successfully updated news with UUID: {uuid} and status: {status}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error updating news in the database: {e}")
            raise

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

    def get_any_personal_data_by_email(self, email_address: str):
        """
        Retrieve personal data associated with an email address.

        :param email_address: Email address of the person.
        :return: Personal data as a json if personalData exists, None otherwise.
        """
        self.create_table_if_not_exists()
        select_query = """
        SELECT pdl_personal_data, apollo_personal_data
        FROM personalData
        WHERE email = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email_address,))
                personal_data = cursor.fetchone()
                pdl_personal_data = personal_data[0] if personal_data else None
                apollo_personal_data = personal_data[1] if personal_data else None
                if pdl_personal_data:
                    return pdl_personal_data
                elif apollo_personal_data:
                    return apollo_personal_data
                else:
                    logger.warning("personalData was not found in db by email address")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving personal data: {e}", e)
            traceback.format_exc()
            return None

    def get_profile_picture_url(self, uuid: str):
        """
        Retrieve the profile picture URL for a profile.

        :param uuid: Unique identifier for the profile.
        :return: Profile picture URL if profile exists, None otherwise.
        """
        select_query = """
        SELECT apollo_personal_data -> 'photo_url'
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                profile_picture_url = cursor.fetchone()
                if profile_picture_url:
                    return profile_picture_url[0]
                else:
                    logger.warning("Profile was not found")
                    return ""
        except Exception as e:
            logger.error(f"Error retrieving profile picture URL: {e}", e)
            traceback.format_exc()
            return None

    def get_all_uuids_without_apollo(self) -> List[str]:
        """
        Retrieve all UUIDs without Apollo data.
        """
        select_query = """
        SELECT uuid
        FROM personalData
        WHERE apollo_status IS NULL OR apollo_status = 'null'
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query)
                uuids = cursor.fetchall()
                return [uuid[0] for uuid in uuids]
        except Exception as e:
            logger.error(f"Error retrieving UUIDs without Apollo data: {e}", e)
            traceback.format_exc()
            return []

    def update_pdl_personal_data(self, uuid, personal_data, status="FETCHED", name=None):
        """
        Save personal data to the database.

        :param uuid: Unique identifier for the personalData.
        :param personal_data: Personal data to save.
        """
        update_query = f"""
        UPDATE personalData
        SET pdl_personal_data = %s, pdl_last_updated = CURRENT_TIMESTAMP, pdl_status = %s {f", name = '{name}'" if name else ''}
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (json.dumps(personal_data), status, uuid))
                self.conn.commit()
                logger.info("Updated personal data")
        except psycopg2.Error as e:
            logger.error(f"Failed to executre personal data query: {update_query}")
            logger.error("psycopg2 Error updating personal data:", str(e))
            # self.conn.rollback()
        except Exception as e:
            logger.error("Exception Error updating personal data:", str(e))
            # self.conn.rollback()
        return

    def update_apollo_personal_data(self, uuid, personal_data, status="FETCHED"):
        """
        Save personal data to the database.

        :param uuid: Unique identifier for the personalData.
        :param personal_data: Personal data to save.
        """

        logger.debug(f"Updating personal data for {uuid}, apollo_personal_data: {str(personal_data)[:300]}")

        update_query = """
        UPDATE personalData
        SET apollo_personal_data = %s, apollo_last_updated = CURRENT_TIMESTAMP, apollo_status = %s
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (json.dumps(personal_data), status, uuid))
                self.conn.commit()
                logger.info("Updated personal data")
        except psycopg2.Error as e:
            logger.error(f"Failed to executre personal data query: {update_query}")
            logger.error("psycopg2 Error updating personal data:", str(e))
            # self.conn.rollback()
        except Exception as e:
            logger.error("Exception Error updating personal data:", str(e))
            # self.conn.rollback()
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
            # self.conn.rollback()
        except Exception as e:
            logger.error("Error updating UUID:", e)
            traceback.print_exc()
            # self.conn.rollback()
        return

    def update_pdl_status(self, uuid, status):
        """
        Update the status for a profile.

        :param uuid: Unique identifier for the profile.
        :param status: New status for the profile.
        """
        update_query = """
        UPDATE personalData
        SET pdl_status = %s, pdl_last_updated = CURRENT_TIMESTAMP
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (status, uuid))
                self.conn.commit()
                logger.info("Updated status")
        except psycopg2.Error as e:
            logger.error("Error updating status:", e)
            traceback.print_exc()
            # self.conn.rollback()
        except Exception as e:
            logger.error("Error updating status:", e)
            traceback.print_exc()
            # self.conn.rollback()
        return

    def update_name_in_personal_data(self, uuid, name):
        """
        Update the name for a profile.

        :param uuid: Unique identifier for the profile.
        :param name: New name for the profile.
        """
        update_query = """
        UPDATE personalData
        SET name = %s
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (name, uuid))
                self.conn.commit()
                logger.info("Updated name")
        except psycopg2.Error as e:
            logger.error("Error updating name:", e)
            traceback.print_exc()
            # self.conn.rollback()
        except Exception as e:
            logger.error("Error updating name:", e)
            traceback.print_exc()
            # self.conn.rollback()
        return

    def save_pdl_personal_data(self, person: PersonDTO, personal_data: dict | str, status: str = "FETCHED"):
        """
        Save personal data to the database.

        :param person: Person object.
        :param personal_data: Personal data to save.
        """
        logger.debug(f"Saving personal data for {person.email}")
        if not self.exists_uuid(person.uuid):
            self.insert(
                uuid=person.uuid,
                name=person.name,
                email=person.email,
                linkedin_url=person.linkedin,
                pdl_personal_data=personal_data,
                pdl_status=status,
            )
            return
        if person.name:
            self.update_pdl_personal_data(
                uuid=person.uuid, personal_data=personal_data, status=status, name=person.name
            )
        else:
            self.update_pdl_personal_data(uuid=person.uuid, personal_data=personal_data, status=status)
        # This use case is for when we try to fetch personal data by email and fail and then someone updates
        # linkekdin url and we are able to fetch personal data but linkedin url is still missing from table
        if person and person.linkedin and not self.exists_linkedin_url(person.linkedin):
            self.update_linkedin_url(person.uuid, person.linkedin)
        return

    def save_apollo_personal_data(
        self, person: PersonDTO, personal_data: dict | str | None, status: str = "FETCHED"
    ):
        """
        Save personal data to the database.

        :param person: Person object.
        :param personal_data: Personal data to save.
        """
        self.create_table_if_not_exists()
        logger.debug(
            f"Saving personal data for {person.uuid}, apollo_personal_data: {str(personal_data)[:300]}"
        )
        if not self.exists_uuid(person.uuid):
            self.insert(
                uuid=person.uuid,
                name=person.name,
                email=person.email,
                linkedin_url=person.linkedin,
                apollo_personal_data=personal_data,
                apollo_status=status,
            )
            return
        self.update_apollo_personal_data(person.uuid, personal_data, status)
        # This use case is for when we try to fetch personal data by email and fail and then someone updates
        # linkekdin url and we are able to fetch personal data but linkedin url is still missing from table
        if person and person.linkedin and not self.exists_linkedin_url(person.linkedin):
            self.update_linkedin_url(person.uuid, person.linkedin)
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
            # self.conn.rollback()
        except Exception as e:
            logger.error("Error updating LinkedIn URL:", e)
            traceback.print_exc()
            # self.conn.rollback()
        return

    def get_pdl_last_updated(self, uuid):
        """
        Retrieve the last updated timestamp for a profile.

        :param uuid: Unique identifier for the profile.
        :return: Timestamp if profile exists, None otherwise.
        """
        select_query = """
        SELECT pdl_last_updated
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

    def get_apollo_last_updated(self, uuid):
        """
        Retrieve the last updated timestamp for a profile.

        :param uuid: Unique identifier for the profile.
        :return: Timestamp if profile exists, None otherwise.
        """
        select_query = """
        SELECT apollo_last_updated
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

    def get_linkedin_url(self, uuid):
        """
        Retrieve the LinkedIn URL for a profile.

        :param uuid: Unique identifier for the profile.
        :return: LinkedIn URL if profile exists, None otherwise.
        """
        select_query = """
        SELECT linkedin_url
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                linkedin_url = cursor.fetchone()
                if linkedin_url:
                    return linkedin_url[0]
                else:
                    logger.warning("Profile was not found")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving LinkedIn URL: {e}", e)
            traceback.format_exc()
            return None

    def get_personal_data_row(self, uuid):
        """
        Retrieve the personal data row as a dict with column names as keys.
        """
        select_query = """
        SELECT uuid, name, email, linkedin_url, pdl_personal_data, pdl_status, pdl_last_updated, apollo_personal_data,
        apollo_status, apollo_last_updated
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                personal_data = cursor.fetchone()
                if personal_data:
                    personal_data_dict = {
                        "uuid": personal_data[0],
                        "name": personal_data[1],
                        "email": personal_data[2],
                        "linkedin_url": personal_data[3],
                        "pdl_personal_data": personal_data[4],
                        "pdl_status": personal_data[5],
                        "pdl_last_updated": personal_data[6],
                        "apollo_personal_data": personal_data[7],
                        "apollo_status": personal_data[8],
                        "apollo_last_updated": personal_data[9],
                    }
                    return personal_data_dict
                else:
                    logger.warning("personalData object was not found in db by uuid")
                    return None
        except Exception as e:
            logger.error(f"Error retrieving personal data: {e}", e)
            traceback.format_exc()
            return None

    def get_social_media_links(self, uuid: str) -> List[SocialMediaLinks]:
        """
        Retrieve social media links for a profile and return them as a list of SocialMediaLinks models.

        :param uuid: Unique identifier for the profile.
        :return: List of SocialMediaLinks if profile exists, empty list otherwise.
        """
        select_query = """
        SELECT pdl_personal_data -> 'profiles', apollo_personal_data
        FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (str(uuid),))
                result = cursor.fetchone()
                logger.info(f"Got result: {result}")
                if result:
                    pdl_social_link = result[0]
                    apollo_data = result[1]

                    social_media_list = []

                    if pdl_social_link:
                        for profile in pdl_social_link:
                            url = profile.get("url")
                            network = profile.get("network")
                            if url and network:
                                social_media_list.append(
                                    SocialMediaLinks.from_dict({"url": url, "platform": network})
                                )

                    if apollo_data:
                        apollo_links = {
                            "linkedin_url": apollo_data.get("linkedin_url"),
                            "twitter_url": apollo_data.get("twitter_url"),
                            "facebook_url": apollo_data.get("facebook_url"),
                            "github_url": apollo_data.get("github_url"),
                        }

                        platform_mapping = {
                            "linkedin_url": "LinkedIn",
                            "twitter_url": "Twitter",
                            "facebook_url": "Facebook",
                            "github_url": "GitHub",
                        }
                        for key, url in apollo_links.items():
                            if url and not any(s.url == url for s in social_media_list):
                                social_media_list.append(
                                    SocialMediaLinks.from_dict(
                                        {"url": url, "platform": platform_mapping[key]}
                                    )
                                )

                    return social_media_list
                else:
                    logger.warning("Personal data was not found")
                    return []
        except Exception as e:
            logger.error(f"Error retrieving social media links: {e}", e)
            traceback.format_exc()
            return []

    def get_pdl_status(self, existing_uuid):
        """
        Retrieve the status of a profile.

        :param existing_uuid: Unique identifier for the profile.
        :return: Status if profile exists, None otherwise.
        """
        select_query = """
        SELECT pdl_status
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

    def get_apollo_status(self, existing_uuid):
        """
        Retrieve the status of a profile.

        :param existing_uuid: Unique identifier for the profile.
        :return: Status if profile exists, None otherwise.
        """
        select_query = """
        SELECT apollo_status
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

    def get_all_personal_data_with_missing_attributes(self):
        """
        Retrieve personal data with missing attributes that have valid corresponding data in the persons table.

        :return: Personal data with missing attributes.
        """
        select_query = """
        SELECT pd.uuid
        FROM personalData pd
        INNER JOIN persons p ON pd.uuid = p.uuid
        WHERE (pd.name IS NULL OR pd.name = '') AND p.name IS NOT NULL AND p.name != ''
          OR (pd.linkedin_url IS NULL OR pd.linkedin_url = '') AND p.linkedin IS NOT NULL AND p.linkedin != ''
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query)
                personal_data = cursor.fetchall()
                logger.info(f"Got personal data: {len(personal_data)}")
                if personal_data:
                    return personal_data
                else:
                    logger.warning("No personal data found")
                    return []
        except Exception as e:
            logger.error(f"Error retrieving personal data: {e}", e)
            traceback.format_exc()
            return None

    def get_duplicates_by_email(self):
        """
        Retrieve personal data with missing attributes that have valid corresponding data in the persons table.

        :return: Personal data with missing attributes.
        """
        select_query = """
        SELECT a.uuid, a.name, a.email, a.linkedin_url
        FROM personalData AS a INNER JOIN personalData AS b
        ON a.email = b.email
        where a.id != b.id
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query)
                personal_data = cursor.fetchall()
                logger.info(f"Got personal data: {len(personal_data)}")
                if personal_data:
                    return personal_data
                else:
                    logger.warning("No personal data found")
                    return []
        except Exception as e:
            logger.error(f"Error retrieving personal data: {e}", e)
            traceback.format_exc()
            return None

    def delete(self, uuid):
        """
        Delete a personalData from the database.

        :param uuid: Unique identifier for the personalData.
        """
        delete_query = """
        DELETE FROM personalData
        WHERE uuid = %s
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_query, (uuid,))
                self.conn.commit()
                logger.info("Deleted personalData")
        except psycopg2.Error as e:
            logger.error("Error deleting personalData:", e)
            traceback.print_exc()
            # self.conn.rollback()
        except Exception as e:
            logger.error("Error deleting personalData:", e)
            traceback.print_exc()
            # self.conn.rollback()
        return
