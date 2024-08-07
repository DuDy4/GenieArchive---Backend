import json
import traceback
from datetime import date, datetime
from typing import Union, Optional

import psycopg2
from pydantic import AnyUrl

from data.data_common.data_transfer_objects.profile_dto import (
    ProfileDTO,
    Strength,
    Connection,
    Phrase,
    UUID,
)
from loguru import logger


class ProfilesRepository:
    def __init__(self, conn):
        self.conn = conn
        self.create_table_if_not_exists()

    def __del__(self):
        if self.conn:
            self.conn.close()

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
            picture_url VARCHAR
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
        except Exception as error:
            logger.error(f"Error creating table: {error}")
            traceback.print_exc()

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

    def get_profile_id(self, uuid):
        select_query = "SELECT id FROM profiles WHERE uuid = %s;"
        try:
            with self.conn.cursor() as cursor:
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

    def get_profile_data(self, uuid: str) -> Union[ProfileDTO, None]:
        select_query = """
        SELECT uuid, name, company, position, strengths, hobbies, connections, get_to_know, summary, picture_url
        FROM profiles
        WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
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
                    get_to_know = {
                        k: [Phrase.from_dict(p) for p in v] for k, v in row[7].items()
                    }
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
                    )
                    return ProfileDTO.from_tuple(profile_data)
                else:
                    logger.error(f"Error with getting profile data for {uuid}")
                    traceback.print_exc()
        except Exception as error:
            logger.error(f"Error fetching profile data by uuid: {error}")
            traceback.print_exception(error)
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
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email,))
                rows = cursor.fetchall()
                if rows:
                    logger.info(f"Got {len(rows)} hobbies from database")
                    hobbies = [
                        {"hobby_name": row[0], "icon_url": row[1]} for row in rows
                    ]
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
        SELECT
            jsonb_array_elements(connections)->>'name' AS name,
            jsonb_array_elements(connections)->>'image_url' AS image_url,
            jsonb_array_elements(connections)->>'linkedin_url' AS linkedin_url
        FROM profiles
        JOIN persons on persons.uuid = profiles.uuid
        WHERE persons.email = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email,))
                rows = cursor.fetchall()
                if rows:
                    logger.info(f"Got {len(rows)} connections from database")
                    connections = [
                        {"name": row[0], "image_url": row[1], "linkedin_url": row[2]}
                        for row in rows
                    ]
                    return connections
                else:
                    logger.info(f"Could not find connections for {email}")
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
        try:
            with self.conn.cursor() as cursor:
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

    @staticmethod
    def json_serializer(obj):
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, AnyUrl):
            return str(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    def _insert(self, profile: ProfileDTO) -> Union[str, None]:
        insert_query = """
            INSERT INTO profiles (uuid, name, company, position, strengths, hobbies, connections, get_to_know, summary, picture_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            json.dumps(
                [
                    s if isinstance(s, dict) else s.to_dict()
                    for s in profile_dict["strengths"]
                ]
            ),
            json.dumps(profile_dict["hobbies"]),
            json.dumps(
                [
                    c if isinstance(c, dict) else c.to_dict()
                    for c in profile_dict["connections"]
                ]
            ),
            json.dumps(
                {
                    k: [p if isinstance(p, dict) else p.to_dict() for p in v]
                    for k, v in profile_dict["get_to_know"].items()
                }
            ),
            profile_dict["summary"],
            str(profile_dict["picture_url"]) if profile_dict["picture_url"] else None,
        )

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, profile_data)
                self.conn.commit()
                profile_id = cursor.fetchone()[0]
                logger.info(f"Inserted profile to database. profile id: {profile_id}")
                return profile_id
        except psycopg2.Error as error:
            raise Exception(f"Error inserting profile, because: {error.pgerror}")

    def _update(self, profile: ProfileDTO):
        update_query = """
        UPDATE profiles
        SET name = %s, company = %s, position = %s, strengths = %s, hobbies = %s, connections = %s, get_to_know = %s, summary = %s, picture_url = %s
        WHERE uuid = %s;
        """
        profile_dict = profile.to_dict()
        profile_data = (
            profile_dict["name"],
            profile_dict["company"],
            profile_dict["position"],
            json.dumps(
                [
                    s if isinstance(s, dict) else s.to_dict()
                    for s in profile_dict["strengths"]
                ]
            ),
            json.dumps(profile_dict["hobbies"]),
            json.dumps(
                [
                    c if isinstance(c, dict) else c.to_dict()
                    for c in profile_dict["connections"]
                ]
            ),
            json.dumps(
                {
                    k: [p if isinstance(p, dict) else p.to_dict() for p in v]
                    for k, v in profile_dict["get_to_know"].items()
                }
            ),
            profile_dict["summary"],
            str(profile_dict["picture_url"]) if profile_dict["picture_url"] else None,
            str(profile_dict["uuid"]),
        )

        logger.info(f"Persisting profile data {profile_data}")
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, profile_data)
                self.conn.commit()
                logger.info(f"Updated profile with uuid: {profile.uuid}")
        except psycopg2.Error as error:
            raise Exception(f"Error updating profile, because: {error.pgerror}")
