import json
import traceback
from typing import Union

import psycopg2

from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from loguru import logger


class ProfilesRepository:
    def __init__(self, conn):
        self.conn = conn

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
            challenges JSONB,
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
                logger.info("Created profiles table in database")
        except Exception as error:
            logger.error("Error creating table:", error)

    def insert_profile(self, profile: ProfileDTO) -> Union[str, None]:
        """
        :param profile: ProfileDTO object with profile data to insert into database
        :return the id of the newly created profile in database:
        """
        insert_query = """
        INSERT INTO profiles (uuid, name, company, position, challenges, strengths, hobbies, connections, news, get_to_know, summary, picture_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert profile: {profile}")
        profile_data = profile.to_tuple()

        logger.info(f"About to insert profile data: {profile_data}")

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
            logger.error("Error fetching id by uuid:", error)
        return None

    def get_profile_data(self, uuid: str) -> Union[ProfileDTO, None]:
        select_query = """
        SELECT uuid, name, company, position, challenges, strengths, hobbies, connections, news, get_to_know, summary, picture_url
        FROM profiles
        WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got {row[0]} from database")
                    return ProfileDTO.from_tuple(row)
                else:
                    logger.error(f"Error with getting profile data for {uuid}")
                    traceback.print_exc()
        except Exception as error:
            logger.error("Error fetching profile data by uuid:", error)
            traceback.print_exception(error)
        return None

    def update(self, profile: ProfileDTO):
        update_query = """
        UPDATE profiles
        SET name = %s, company = %s, position = %s, challenges = %s, strengths = %s, hobbies = %s, connections = %s, news = %s, get_to_know = %s, summary = %s, picture_url = %s
        WHERE uuid = %s;
        """
        profile_data = profile.to_tuple()
        profile_data = profile_data[1:] + (profile_data[0],)  # move uuid to the end
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
        profile.challenges = json.dumps(profile.challenges)
        profile.strengths = json.dumps(profile.strengths)
        profile.hobbies = json.dumps(profile.hobbies)
        profile.connections = json.dumps(profile.connections)
        profile.news = json.dumps(profile.news)
        profile.get_to_know = json.dumps(profile.get_to_know)
        if self.exists(profile.uuid):
            self.update(profile)
        else:
            self.insert_profile(profile)

    def get_profiles_from_list(self, uuids: list) -> list:
        select_query = """
        SELECT uuid, name, company, position, challenges, strengths, hobbies, connections, news, get_to_know, summary, picture_url
        FROM profiles
        WHERE uuid = ANY(%s);
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuids,))
                rows = cursor.fetchall()
                profiles = [ProfileDTO.from_tuple(row) for row in rows]
                return profiles
        except Exception as error:
            logger.error("Error fetching profiles by uuids:", error)
            return []

    def get_profile_picture(self, uuid: str) -> list:
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
                    return ""
        except Exception as error:
            logger.error("Error fetching profile pictures by uuids:", error)
            traceback.print_exc()
            return []
