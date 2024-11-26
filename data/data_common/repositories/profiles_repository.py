import json
import traceback
from datetime import date, datetime
from typing import Union, Optional

import psycopg2
from pydantic import AnyUrl

from common.utils import env_utils
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.data_transfer_objects.profile_dto import (
    ProfileDTO,
    Strength,
    Connection,
    Phrase,
    Hobby,
    UUID,
)
from data.data_common.data_transfer_objects.profile_category_dto import SalesCriteria
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection

logger = GenieLogger()
DEFAULT_PROFILE_PICTURE = "https://monomousumi.com/wp-content/uploads/anonymous-user-8.png"
BLOB_CONTAINER_PICTURES_NAME = env_utils.get("BLOB_CONTAINER_PICTURES_NAME", "profile-pictures")


class ProfilesRepository:
    def __init__(self):
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS profiles (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            name VARCHAR,
            company VARCHAR,
            position VARCHAR,
            strengths JSONB,
            hobbies JSONB,
            connections JSONB,
            get_to_know JSONB,
            summary TEXT,
            picture_url VARCHAR,
            work_history_summary VARCHAR,
            sales_criteria JSONB
        );
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
            except Exception as error:
                logger.error(f"Error creating table: {error}")
                traceback.print_exc()

    def save_new_profile_from_person(self, person: PersonDTO):
        self.create_table_if_not_exists()
        logger.debug(f"About to save profile from person: {person}")
        profile = ProfileDTO(
            uuid=person.uuid, name=person.name, company=person.company, position=person.position
        )
        self.save_profile(profile)

    def save_profile(self, profile: ProfileDTO):
        self.create_table_if_not_exists()
        logger.debug(f"About to save profile: {profile}")
        if self.exists(str(profile.uuid)):
            self._update(profile)
        else:
            self._insert(profile)

    def exists(self, uuid: str) -> bool:
        logger.info(f"About to check if uuid exists: {uuid}")
        exists_query = "SELECT 1 FROM profiles WHERE uuid = %s;"
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

    def get_profile_id(self, uuid):
        select_query = "SELECT id FROM profiles WHERE uuid = %s;"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid,))
                    row = cursor.fetchone()
                    if row:
                        logger.info(f"Got {row[0]} from database")
                        return row[0]
                    else:
                        logger.error(f"Error with getting profile id for {uuid}")
            except Exception as error:
                logger.error(f"Error fetching id by uuid: {error}")
            return None

    def get_latest_profile_ids(self, limit: int, search_term: Optional[str] = None):
        select_query = """ SELECT pr.uuid FROM profiles pr """
        where_query = """ JOIN persons pe ON pr.uuid = pe.uuid
                          WHERE pe.email LIKE %s OR pr.name LIKE %s """
        order_query = "ORDER BY pr.id DESC LIMIT %s;"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    if search_term:
                        select_query = select_query + where_query + order_query
                        cursor.execute(select_query, (f"%{search_term}%", f"%{search_term}%", limit))
                    else:
                        cursor.execute(select_query + order_query, (limit,))
                    rows = cursor.fetchall()
                    if rows:
                        profile_ids = [row[0] for row in rows]
                        return profile_ids
                    else:
                        logger.error(f"Error with getting latest profile ids")
            except Exception as error:
                logger.error(f"Error fetching latest id: {error}")
            return None

    def get_profile_data(self, uuid: str) -> Union[ProfileDTO, None]:
        select_query = """
        SELECT uuid, name, company, position, strengths, hobbies, connections, get_to_know, summary, picture_url, work_history_summary, sales_criteria
        FROM profiles
        WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid,))
                    row = cursor.fetchone()
                    if row:
                        logger.info(f"Got {row[0]} from database")
                        uuid = UUID(row[0])
                        name = row[1]
                        company = row[2]
                        position = row[3]
                        summary = row[8] if row[8] else None
                        picture_url = AnyUrl(row[9]) if AnyUrl(row[9]) else DEFAULT_PROFILE_PICTURE
                        strengths = [Strength.from_dict(item) for item in row[4]]
                        hobbies = json.loads(row[5]) if isinstance(row[5], str) else row[5]
                        connections = [Connection.from_dict(item) for item in row[6]]
                        get_to_know = {k: [Phrase.from_dict(p) for p in v] for k, v in row[7].items()}
                        work_history_summary = row[10] if row[10] else None
                        sales_criteria = (SalesCriteria.from_tuple(criteria) for criteria in row[11]) if row[11] else None
                        profile_data = (
                            uuid,
                            name,
                            company,
                            position,
                            summary,
                            picture_url,
                            get_to_know,
                            connections,
                            strengths,
                            hobbies,
                            work_history_summary,
                            sales_criteria
                        )
                        return ProfileDTO.from_tuple(profile_data)
                    else:
                        logger.error(f"Error with getting profile data for {uuid}")
                        traceback.print_exc()
            except Exception as error:
                logger.error(f"Error fetching profile data by uuid: {error}")
                traceback.print_exception(error)
            return None

    def delete_by_email(self, email: str):
        delete_query = """
        DELETE FROM profiles
        WHERE uuid = (SELECT uuid FROM persons WHERE email = %s);
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(delete_query, (email,))
                    conn.commit()
                    logger.info(f"Deleted profile for {email}")
            except psycopg2.Error as error:
                raise Exception(f"Error deleting profile, because: {error.pgerror}")

    def get_profiles_from_list(self, uuids: list, search: Optional[str] = None) -> list:
        """
        Retrieve profiles from a list of UUIDs with optional search on profile names.

        :param uuids: List of profile UUIDs.
        :param search: Optional partial text to search profile names.
        :return: List of ProfileDTO objects.
        """
        with db_connection() as conn:

            try:
                logger.debug(f"About to get profiles from list: {uuids} with search: {search}")
                with conn.cursor() as cursor:
                    if search:
                        select_query = """
                        SELECT uuid, name, company, position, strengths, hobbies, connections, get_to_know, summary, picture_url, work_history_summary, sales_criteria
                        FROM profiles
                        WHERE uuid = ANY(%s) AND name ILIKE %s;
                        """
                        search_pattern = f"%{search}%"
                        cursor.execute(select_query, (uuids, search_pattern))
                    else:
                        select_query = """
                        SELECT uuid, name, company, position, strengths, hobbies, connections, get_to_know, summary, picture_url, work_history_summary, sales_criteria
                        FROM profiles
                        WHERE uuid = ANY(%s);
                        """
                        cursor.execute(select_query, (uuids,))

                    rows = cursor.fetchall()
                    logger.info(f"Got {len(rows)} profiles from database")
                    profiles = []

                    for row in rows:
                        uuid = UUID(row[0])
                        logger.debug(f"UUID: {uuid}")

                        name = row[1] if row[1] else ""
                        if name == "":
                            logger.error(f"Name is empty for {uuid}")
                        logger.debug(f"Name: {name}")

                        company = row[2] if row[2] else ""
                        if company == "":
                            logger.error(f"Company is empty for {uuid}")
                        logger.debug(f"Company: {company}")

                        position = row[3] if row[3] else ""
                        if position == "":
                            logger.error(f"Position is empty for {uuid}")
                        logger.debug(f"Position: {position}")

                        summary = row[8] if row[8] else None
                        logger.debug(f"Summary: {summary}")

                        # Ensure strengths is a list of Strength objects
                        strengths = (
                            [Strength.from_dict(item) for item in json.loads(row[4])]
                            if isinstance(row[4], str)
                            else row[4]
                        )
                        logger.debug(f"Strengths length: {len(strengths)}")

                        # Ensure hobbies is a list of UUIDs
                        hobbies = (
                            [UUID(hobby) for hobby in json.loads(row[5])] if isinstance(row[5], str) else row[5]
                        )
                        logger.debug(f"Hobbies: {hobbies}")

                        # Ensure connections is a list of Connection objects
                        connections = (
                            [Connection.from_dict(item) for item in json.loads(row[6])]
                            if isinstance(row[6], str)
                            else row[6]
                        )
                        logger.debug(f"Connections: {connections}")

                        # Ensure get_to_know is a dictionary with lists of Phrase objects
                        get_to_know = (
                            {k: [Phrase.from_dict(p) for p in v] for k, v in json.loads(row[7]).items()}
                            if isinstance(row[7], str)
                            else row[7]
                        )
                        logger.debug(f"Get to know: {get_to_know}")

                        # Ensure company field is present

                        # Ensure picture_url is a valid URL or None
                        picture_url = AnyUrl(row[9]) if AnyUrl(row[9]) else None
                        if picture_url == "":
                            picture_url = None

                        work_history_summary = row[10] if row[10] else None

                        sales_criteria = (SalesCriteria.from_tuple(criteria) for criteria in row[11]) if row[11] else None

                        logger.info(f"About to create ProfileDTO from tuple: {row}")

                        profile_data = (
                            uuid,  # uuid
                            name,  # name
                            company,  # company
                            position,  # position
                            summary,  # summary
                            picture_url,  # picture_url
                            get_to_know,
                            connections,
                            strengths,
                            hobbies,
                            work_history_summary,
                            sales_criteria
                        )
                        profiles.append(ProfileDTO.from_tuple(profile_data))
                    return profiles
            except Exception as error:
                logger.error(f"Error fetching profiles by uuids: {error}")
                traceback.print_exc()
                return []

    def get_profile_data_by_email(self, email: str) -> Union[ProfileDTO, None]:
        select_query = """
        SELECT profiles.uuid, profiles.name, profiles.company, profiles.position, profiles.strengths, profiles.hobbies, profiles.connections, profiles.get_to_know, profiles.summary, profiles.picture_url, profiles.work_history_summary, profiles.sales_criteria
        FROM profiles
        JOIN persons on persons.uuid = profiles.uuid
        WHERE persons.email = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (email,))
                    row = cursor.fetchone()
                    if row:
                        logger.info(f"Got {row[0]} from database")
                        uuid = UUID(row[0])
                        name = row[1]
                        company = row[2]
                        position = row[3]
                        summary = row[8] if row[8] else None
                        picture_url = AnyUrl(row[9]) if AnyUrl(row[9]) else None
                        strengths = [Strength.from_dict(item) for item in row[4]]
                        hobbies = json.loads(row[5]) if isinstance(row[5], str) else row[5]
                        connections = [Connection.from_dict(item) for item in row[6]]
                        get_to_know = {k: [Phrase.from_dict(p) for p in v] for k, v in row[7].items()}
                        work_history_summary = row[10] if row[10] else None
                        sales_criteria = (SalesCriteria.from_tuple(criteria) for criteria in row[11]) if row[11] else None
                        profile_data = (
                            uuid,
                            name,
                            company,
                            position,
                            summary,
                            picture_url,
                            get_to_know,
                            connections,
                            strengths,
                            hobbies,
                            work_history_summary
                        )
                        return ProfileDTO.from_tuple(profile_data)
                    else:
                        logger.error(f"Error with getting profile data for {email}")
                        traceback.print_exc()
            except Exception as error:
                logger.error(f"Error fetching profile data by email: {error}")
                traceback.print_exc()
            return None

    def get_hobbies_by_email(self, email: str) -> list:
        if not email:
            return None
        select_query = """
        SELECT h.hobby_name, h.icon_url
        FROM profiles
        JOIN persons on persons.uuid = profiles.uuid
        JOIN LATERAL jsonb_array_elements_text(profiles.hobbies) AS hobby_uuid ON TRUE
        JOIN hobbies h ON hobby_uuid.value = h.uuid
        WHERE persons.email = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (email,))
                    rows = cursor.fetchall()
                    if rows:
                        logger.info(f"Got {len(rows)} hobbies from database")
                        hobbies = [Hobby(hobby_name=row[0], icon_url=row[1]) for row in rows]
                        return hobbies
                    else:
                        logger.info(f"Could not find hobbies for {email}")
                        return None
            except Exception as error:
                logger.error(f"Error fetching hobbies by email: {error}")
                traceback.print_exc()
                return None

    def get_connections_by_email(self, email: str) -> list:
        if not email:
            return None
        select_query = """
        SELECT connections  FROM profiles
        JOIN persons on persons.uuid = profiles.uuid
        WHERE persons.email = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (email,))
                    row = cursor.fetchone()
                    if row:
                        logger.info(f"Got connections from database: {row[0]}")
                        connections = []
                        for connection_object in row[0]:
                            connections.append(Connection.from_dict(connection_object))
                        return connections
                    else:
                        logger.info(f"Could not find connection for {email}")
                        return None
            except Exception as error:
                logger.error(f"Error fetching connections by email: {error}")
                traceback.print_exc()
                return None

    def get_profile_picture(self, uuid: str) -> Optional[str]:
        select_query = """
        SELECT picture_url
        FROM profiles
        WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid,))
                    row = cursor.fetchone()
                    if row:
                        logger.info(f"Got {row} from database")
                        return row[0]
                    else:
                        logger.error(f"Error with getting profile picture for {uuid}")
                        return None
            except Exception as error:
                logger.error(f"Error fetching profile pictures by uuid: {error}")
                traceback.print_exc()
                return None

    def update_profile_picture(self, uuid: str, picture_url: str):
        if "https://static.licdn.com" in picture_url:
            logger.info(f"Got static url for {uuid}. Skipping")
            return
        update_query = """
        UPDATE profiles
        SET picture_url = %s
        WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (picture_url, uuid))
                    conn.commit()
                    logger.info(f"Updated picture for {uuid}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating picture, because: {error.pgerror}")

    def get_all_profiles_without_profile_picture(self) -> list:
        select_query = f"""
        SELECT uuid
        FROM profiles
        WHERE picture_url IS NULL OR picture_url = ''
         OR picture_url = '{DEFAULT_PROFILE_PICTURE}'
         OR picture_url ILIKE 'https://static.licdn.com%'
         order by id desc;         
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query)
                    rows = cursor.fetchall()
                    if rows:
                        logger.info(f"Got {len(rows)} profiles from database")
                        return [row[0] for row in rows]
                    else:
                        logger.info(f"Could not find profiles without picture")
                        return []
            except Exception as error:
                logger.error(f"Error fetching profiles without picture: {error}")
                traceback.print_exc()
                return []

    def get_missing_profiles(self) -> list:
        select_query = """
        SELECT pd.uuid
        FROM personalData pd
        WHERE NOT EXISTS (
            SELECT 1
        FROM profiles p
        WHERE p.uuid = pd.uuid
        )
        AND NOT (pd.pdl_status = 'TRIED_BUT_FAILED' AND pd.apollo_status = 'TRIED_BUT_FAILED');
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query)
                    rows = cursor.fetchall()
                    if rows:
                        logger.info(f"Got {len(rows)} missing profiles from database")
                        return [row[0] for row in rows]
                    else:
                        logger.info(f"Could not find missing profiles")
                        return []
            except Exception as error:
                logger.error(f"Error fetching missing profiles: {error}")
                traceback.print_exc()
                return []

    def get_all_profiles_without_company_name(self) -> list:
        """
        Get all profiles without company name, and return their ProfileDTO objects.
        """
        select_query = """
        SELECT uuid, name, company, position, strengths, hobbies, connections, get_to_know, summary, picture_url, work_history_summary
        FROM profiles
        WHERE company IS NULL OR company = '';
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query)
                    rows = cursor.fetchall()
                    logger.info(f"Got {len(rows)} profiles without company name from database")
                    profiles = []
                    for row in rows:
                        uuid = UUID(row[0])
                        name = row[1]
                        company = row[2]
                        position = row[3]
                        summary = row[8] if row[8] else None
                        picture_url = AnyUrl(row[9]) if AnyUrl(row[9]) else None
                        strengths = [Strength.from_dict(item) for item in row[4]]
                        hobbies = json.loads(row[5]) if isinstance(row[5], str) else row[5]
                        connections = [Connection.from_dict(item) for item in row[6]]
                        get_to_know = {k: [Phrase.from_dict(p) for p in v] for k, v in row[7].items()}
                        work_history_summary = row[10] if row[10] else None
                        profile_data = (
                            uuid,
                            name,
                            company,
                            position,
                            summary,
                            picture_url,
                            get_to_know,
                            connections,
                            strengths,
                            hobbies,
                            work_history_summary,
                        )
                        profiles.append(ProfileDTO.from_tuple(profile_data))
                    return profiles
            except Exception as error:
                logger.error(f"Error fetching profiles without company name: {error}")
                traceback.print_exc()
                return []

    def get_strengths_by_email_list(self, emails: list[str]) -> list:
        select_query = """
        SELECT persons.email, profiles.strengths
        FROM profiles
        JOIN persons on persons.uuid = profiles.uuid
        WHERE persons.email = ANY(%s);
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (emails,))
                    rows = cursor.fetchall()
                    logger.info(f"Got {len(rows)} profiles from database")
                    email_strengths_list = []

                    for row in rows:
                        email = row[0]
                        strengths_data = row[1]
                        strengths = [Strength.from_dict(s) for s in strengths_data[:5]]  # Limit to 5 strengths
                        email_strengths_list.append({"email": email, "strengths": strengths})

                    return email_strengths_list

            except Exception as error:
                logger.error(f"Error fetching profiles by email list: {error}")
                traceback.print_exc()
                return []

    def get_profiles_by_email_list(self, emails: list[str]) -> list[dict]:
        select_query = """
        SELECT persons.email, profiles.uuid, profiles.name, profiles.company, profiles.position, profiles.strengths, profiles.hobbies, profiles.connections, profiles.get_to_know, profiles.summary, profiles.picture_url, profiles.work_history_summary
        FROM profiles
        JOIN persons on persons.uuid = profiles.uuid
        WHERE persons.email = ANY(%s);
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (emails,))
                    rows = cursor.fetchall()
                    logger.info(f"Got {len(rows)} profiles from database")
                    profiles = []

                    for row in rows:
                        email = row[0]
                        uuid = UUID(row[1])
                        name = row[2]
                        company = row[3]
                        position = row[4]
                        summary = row[9] if row[9] else None
                        picture_url = AnyUrl(row[10]) if AnyUrl(row[10]) else None
                        strengths = [Strength.from_dict(item) for item in row[5]]
                        hobbies = json.loads(row[6]) if isinstance(row[6], str) else row[6]
                        connections = [Connection.from_dict(item) for item in row[7]]
                        get_to_know = {k: [Phrase.from_dict(p) for p in v] for k, v in row[8].items()}
                        work_history_summary = row[11] if row[11] else None
                        profile_data = (
                            uuid,
                            name,
                            company,
                            position,
                            summary,
                            picture_url,
                            get_to_know,
                            connections,
                            strengths,
                            hobbies,
                            work_history_summary,
                        )
                        profiles.append({"email": email, "profile": ProfileDTO.from_tuple(profile_data)})
                    return profiles
            except Exception as error:
                logger.error(f"Error fetching profiles by email list: {error}")
                traceback.print_exc()
                return []

    def get_profiles_dto_by_email_list(self, emails: list[str]) -> list[ProfileDTO]:
        select_query = """
        SELECT profiles.uuid, profiles.name, profiles.company, profiles.position, profiles.strengths, profiles.hobbies, profiles.connections, profiles.get_to_know, profiles.summary, profiles.picture_url, profiles.work_history_summary
        FROM profiles
        JOIN persons on persons.uuid = profiles.uuid
        WHERE persons.email = ANY(%s);
        """
        with db_connection() as conn:

            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (emails,))
                    rows = cursor.fetchall()
                    logger.info(f"Got {len(rows)} profiles from database")
                    profiles = []

                    for row in rows:
                        uuid = UUID(row[0])
                        name = row[1]
                        company = row[2]
                        position = row[3]
                        summary = row[8] if row[8] else None
                        picture_url = AnyUrl(row[9]) if AnyUrl(row[9]) else None
                        strengths = [Strength.from_dict(item) for item in row[4]]
                        hobbies = json.loads(row[5]) if isinstance(row[5], str) else row[5]
                        connections = [Connection.from_dict(item) for item in row[6]]
                        get_to_know = {k: [Phrase.from_dict(p) for p in v] for k, v in row[7].items()}
                        work_history_summary = row[10] if row[10] else None
                        profile_data = (
                            uuid,
                            name,
                            company,
                            position,
                            summary,
                            picture_url,
                            get_to_know,
                            connections,
                            strengths,
                            hobbies,
                            work_history_summary,
                        )
                        profiles.append(ProfileDTO.from_tuple(profile_data))
                    return profiles
            except Exception as error:
                logger.error(f"Error fetching profiles by email list: {error}")
                traceback.print_exc()
                return []

    def insert_profile_without_strengths_and_get_to_know(self, person_data):
        insert_query = """
        INSERT INTO profiles (uuid, name, company, position)
        VALUES (%s, %s, %s, %s);
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        insert_query,
                        (
                            person_data["uuid"],
                            person_data["name"],
                            person_data["company"],
                            person_data["position"],
                        ),
                    )
                    conn.commit()
                    logger.info(f"Inserted profile without strengths and get to know to database")
            except psycopg2.Error as error:
                raise Exception(
                    f"Error inserting profile without strengths and get to know, because: {error.pgerror}"
                )

    def update_hobbies_by_email(self, email: str, hobbies: list[str]):
        update_query = """
        UPDATE profiles
        SET hobbies = %s
        FROM persons
        WHERE profiles.uuid = persons.uuid AND persons.email = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (json.dumps(hobbies), email))
                    conn.commit()
                    logger.info(f"Updated hobbies for {email}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating hobbies, because: {error.pgerror}")

    def update_connections_by_email(self, email: str, connections: list[Connection]):
        update_query = """
        UPDATE profiles
        SET connections = %s
        FROM persons
        WHERE profiles.uuid = persons.uuid AND persons.email = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        update_query,
                        (
                            json.dumps([con.to_dict() for con in connections]),
                            email,
                        ),
                    )
                    conn.commit()
                    logger.info(f"Updated connections for {email}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating connections, because: {error.pgerror}")

    def update_strengths(self, uuid, strengths):
        update_query = """
        UPDATE profiles
        SET strengths = %s
        WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (json.dumps(strengths), uuid))
                    conn.commit()
                    logger.info(f"Updated strengths for {uuid}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating strengths, because: {error.pgerror}")

    def update_get_to_know(self, uuid, get_to_know):
        update_query = """
        UPDATE profiles
        SET get_to_know = %s
        WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (json.dumps(get_to_know), uuid))
                    conn.commit()
                    logger.info(f"Updated get to know for {uuid}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating get to know, because: {error.pgerror}")

    def _insert(self, profile: ProfileDTO) -> Union[str, None]:
        insert_query = """
                INSERT INTO profiles (uuid, name, company, position, strengths, hobbies, connections, get_to_know, summary, picture_url, work_history_summary)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """
        profile_details = "\n".join([f"{k}: {v}" for k, v in profile.__dict__.items()])
        logger.info(f"About to insert profile: {profile_details}")

        profile_dict = profile.to_dict()
        profile_data = (
            str(profile_dict["uuid"]),
            profile_dict["name"],
            profile_dict["company"],
            profile_dict["position"],
            json.dumps([s if isinstance(s, dict) else s.to_dict() for s in profile_dict["strengths"]]),
            json.dumps(profile_dict["hobbies"]),
            json.dumps([c if isinstance(c, dict) else c.to_dict() for c in profile_dict["connections"]]),
            json.dumps(
                {
                    k: [p if isinstance(p, dict) else p.to_dict() for p in v]
                    for k, v in profile_dict["get_to_know"].items()
                }
            ),
            profile_dict["summary"] if profile_dict["summary"] else "",
            str(profile_dict["picture_url"]) if profile_dict["picture_url"] else DEFAULT_PROFILE_PICTURE,
            profile_dict["work_history_summary"] if profile_dict["work_history_summary"] else None,
        )
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, profile_data)
                    conn.commit()
                    profile_id = cursor.fetchone()[0]
                    logger.info(f"Inserted profile to database. profile id: {profile_id}")
                    return profile_id
            except psycopg2.Error as error:
                raise Exception(f"Error inserting profile, because: {error.pgerror}")

    def _update(self, profile: ProfileDTO):
        update_query = """
        UPDATE profiles
        SET name = %s, company = %s, position = %s, strengths = %s, hobbies = %s, connections = %s, get_to_know = %s, summary = %s, picture_url = %s, work_history_summary = %s, sales_criteria = %s
        WHERE uuid = %s;
        """
        profile_dict = profile.to_dict()
        logger.info(f"About to update profile: {profile_dict}")
        profile_data = (
            profile_dict["name"],
            profile_dict["company"],
            profile_dict["position"],
            json.dumps([s if isinstance(s, dict) else s.to_dict() for s in profile_dict["strengths"]]),
            json.dumps(profile_dict["hobbies"]),
            json.dumps([c if isinstance(c, dict) else c.to_dict() for c in profile_dict["connections"]]),
            json.dumps(
                {
                    k: [p if isinstance(p, dict) else p.to_dict() for p in v]
                    for k, v in profile_dict["get_to_know"].items()
                }
            ),
            profile_dict["summary"],
            str(profile_dict["picture_url"]) if profile_dict["picture_url"] else None,
            profile_dict["work_history_summary"] if profile_dict["work_history_summary"] else None,
            json.dumps([(criteria.to_dict() if isinstance(criteria, SalesCriteria) else criteria) for criteria in profile_dict["sales_criteria"]]) if profile_dict["sales_criteria"] else None,
            str(profile_dict["uuid"]),
            )

        logger.info(f"Persisting profile data {profile_data}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, profile_data)
                    conn.commit()
                    logger.info(f"Updated profile with uuid: {profile.uuid}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating profile, because: {error.pgerror}")

    def get_all_profiles_pictures(self):
        select_query = f"""
        SELECT name, picture_url
        FROM profiles
        WHERE not(picture_url IS NULL OR picture_url = ''
         OR picture_url = '{DEFAULT_PROFILE_PICTURE}');
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query)
                    rows = cursor.fetchall()
                    if rows:
                        logger.info(f"Got {len(rows)} profile pictures from database")
                        return [{"name": row[0], "picture_url": row[1]} for row in rows]
                    else:
                        logger.info(f"Could not find profile pictures")
                        return []
            except Exception as error:
                logger.error(f"Error fetching profile pictures: {error}")
                traceback.print_exc()
                return []

    def get_all_profiles_pictures_to_upload(self):
        select_query = f"""
        SELECT uuid, picture_url
        FROM profiles
        WHERE not(picture_url IS NULL OR picture_url = ''
        OR picture_url = '{DEFAULT_PROFILE_PICTURE}' OR picture_url LIKE 'https://static.licdn.com%' OR
        picture_url LIKE '%{BLOB_CONTAINER_PICTURES_NAME}%');
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query)
                    rows = cursor.fetchall()
                    if rows:
                        logger.info(f"Got {len(rows)} profile pictures from database")
                        return [{"uuid": row[0], "picture_url": row[1]} for row in rows]
                    else:
                        logger.info(f"Could not find profile pictures")
                        return []
            except Exception as error:
                logger.error(f"Error fetching profile pictures: {error}")
                traceback.print_exc()
                return []

    def get_sales_criteria(self, uuid):
        select_query = """
        SELECT sales_criteria
        FROM profiles
        WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid,))
                    row = cursor.fetchone()
                    if row:
                        logger.info(f"Got {row[0]} from database")
                        return [SalesCriteria.from_tuple(criteria) for criteria in row[0]] if row[0] else None
                    else:
                        logger.error(f"Error with getting sales criteria for {uuid}")
                        return None
            except Exception as error:
                logger.error(f"Error fetching sales criteria by uuid: {error}")
                traceback.print_exc()
                return None
