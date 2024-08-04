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
    NewsData,
    Phrase,
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
            news JSONB,
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

    def insert_profile(self, profile: ProfileDTO) -> Union[str, None]:
        insert_query = """
            INSERT INTO profiles (uuid, name, company, position, strengths, hobbies, connections, news, get_to_know, summary, picture_url)
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
                [self.serialize_news(n) for n in profile_dict["news"]],
                default=self.json_serializer,
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
        SELECT uuid, name, company, position, strengths, hobbies, connections, news, get_to_know, summary, picture_url
        FROM profiles
        WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got {row[0]} from database")
                    strengths = [
                        Strength.from_dict(item) for item in json.loads(row[4])
                    ]
                    hobbies = json.loads(row[5])
                    connections = [
                        Connection.from_dict(item) for item in json.loads(row[6])
                    ]
                    news = [self.deserialize_news(item) for item in json.loads(row[7])]
                    get_to_know = {
                        k: [Phrase.from_dict(p) for p in v]
                        for k, v in json.loads(row[8]).items()
                    }
                    profile_data = (
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        strengths,
                        hobbies,
                        connections,
                        news,
                        get_to_know,
                        row[9],
                        row[10],
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

    def get_news_by_email(self, email: str) -> list:
        if not email:
            return None
        select_query = """
        SELECT
            jsonb_array_elements(news)->>'title' AS title,
            jsonb_array_elements(news)->>'link' AS link,
            jsonb_array_elements(news)->>'media' AS media
        FROM profiles
        JOIN persons on persons.uuid = profiles.uuid
        WHERE persons.email = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email,))
                rows = cursor.fetchall()
                if rows:
                    logger.info(f"Got {len(rows)} news articles from database")
                    news = [
                        {"title": row[0], "link": row[1], "source": row[2]}
                        for row in rows
                    ]
                    return news
                else:
                    logger.info(f"Could not find news for {email}")
                    return None
        except Exception as error:
            logger.error(f"Error fetching news by email: {error}")
            traceback.print_exc()
            return None

    def update(self, profile: ProfileDTO):
        update_query = """
        UPDATE profiles
        SET name = %s, company = %s, position = %s, strengths = %s, hobbies = %s, connections = %s, news = %s, get_to_know = %s, summary = %s, picture_url = %s
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
            json.dumps([self.serialize_news(n) for n in profile_dict["news"]]),
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

    def save_profile(self, profile: ProfileDTO):
        self.create_table_if_not_exists()
        logger.debug(f"About to save profile: {profile}")
        if self.exists(str(profile.uuid)):
            self.update(profile)
        else:
            self.insert_profile(profile)

    def get_profiles_from_list(self, uuids: list, search: Optional[str] = None) -> list:
        """
        Retrieve profiles from a list of UUIDs with optional search on profile names.

        :param uuids: List of profile UUIDs.
        :param search: Optional partial text to search profile names.
        :return: List of ProfileDTO objects.
        """
        try:
            logger.debug(
                f"About to get profiles from list: {uuids} with search: {search}"
            )
            with self.conn.cursor() as cursor:
                if search:
                    select_query = """
                    SELECT uuid, name, company, position, strengths, hobbies, connections, news, get_to_know, summary, picture_url
                    FROM profiles
                    WHERE uuid = ANY(%s) AND name ILIKE %s;
                    """
                    search_pattern = f"%{search}%"
                    cursor.execute(select_query, (uuids, search_pattern))
                else:
                    select_query = """
                    SELECT uuid, name, company, position, strengths, hobbies, connections, news, get_to_know, summary, picture_url
                    FROM profiles
                    WHERE uuid = ANY(%s);
                    """
                    cursor.execute(select_query, (uuids,))

                rows = cursor.fetchall()
                profiles = [ProfileDTO.from_tuple(row) for row in rows]
                return profiles
        except Exception as error:
            logger.error(f"Error fetching profiles by uuids: {error}")
            return []

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

    def serialize_news(self, news: Union[NewsData, dict]) -> dict:
        if isinstance(news, dict):
            return news
        news_dict = news.to_dict()
        news_dict["date"] = news_dict["date"].isoformat()  # Convert date to string
        return news_dict

    def deserialize_news(self, news: dict) -> NewsData:
        news["date"] = date.fromisoformat(news["date"])  # Convert string back to date
        return NewsData.from_dict(news)

    @staticmethod
    def json_serializer(obj):
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, AnyUrl):
            return str(obj)
        raise TypeError(f"Type {type(obj)} not serializable")
