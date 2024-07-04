import json
import traceback
from typing import Union

import psycopg2

from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from loguru import logger


class ProfilesRepository:
    def __init__(self, conn):
        self.conn = conn
        # self.cursor = conn.cursor()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS profiles (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            tenant_id VARCHAR,
            name VARCHAR,
            company VARCHAR,
            position VARCHAR,
            challenges JSONB,
            strengths JSONB,
            summary TEXT,
            picture_url VARCHAR
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
                logger.info(f"Created profiles table in database")
        except Exception as error:
            logger.error("Error creating table:", error)
            # self.conn.rollback()

    def insert_profile(self, profile: ProfileDTO) -> str | None:
        """
        :param profile: ProfileDTO object with profile data to insert into database
        :return the id of the newly created profile in database:
        """
        insert_query = """
        INSERT INTO profiles (uuid, tenant_id, name, company, position, challenges, strengths, summary, picture_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            # self.conn.rollback()
            raise Exception(f"Error inserting profile, because: {error.pgerror}")

    def exists(self, uuid: str) -> bool:
        logger.info(f"About to check if uuid exists: {uuid}")
        exists_query = "SELECT 1 FROM profiles WHERE uuid = %s;"
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

    def exists_tenant(self, tenant_id: str) -> bool:
        logger.info(f"About to check if tenant_id exists: {tenant_id}")
        exists_query = "SELECT uuid FROM profiles WHERE tenant_id = %s;"
        try:
            with self.conn.cursor() as cursor:
                logger.info(f"about to execute check if tenant_id exists: {tenant_id}")

                cursor.execute(exists_query, (tenant_id,))
                result = cursor.fetchone() is not None
                logger.info(f"{tenant_id} existence in database: {result}")
                return result
        except psycopg2.Error as error:
            logger.error(f"Error checking existence of tenant_id {tenant_id}: {error}")
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
                    logger.info(f"Got {row[2]} from database")
                    return
                else:
                    logger.error(f"Error with getting profile id for {uuid}")

        except Exception as error:
            logger.error("Error fetching id by uuid:", error)
        return None

    def get_profile_data(self, uuid: str) -> Union[ProfileDTO, None]:
        select_query = """
        SELECT uuid, name, company, position, challenges, strengths, summary, picture_url
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
        except Exception as error:
            logger.error("Error fetching profile data by uuid:", error)
            traceback.print_exception(error)
        return None

    def get_all_profiles_by_tenant_id(self, tenant_id: str) -> list[ProfileDTO]:
        select_query = """
        SELECT uuid, name, company, position, challenges, strengths, summary, picture_url
        FROM profiles
        WHERE tenant_id = %s;
        """
        try:
            self.create_table_if_not_exists()
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (tenant_id,))
                rows = cursor.fetchall()
                if rows:
                    logger.info(f"Got {len(rows)} profiles from database")
                    logger.debug(f"Got profiles: {rows}")
                    return [
                        ProfileDTO.from_tuple(
                            (
                                row[0],
                                tenant_id,
                            )
                            + row[1:]
                        )
                        for row in rows
                    ]
                else:
                    logger.error(
                        f"Error with getting profile data for tenant_id {tenant_id}"
                    )
        except Exception as error:
            logger.error("Error fetching profile data by tenant_id:", error)
            traceback.print_exception(error)
        return []

    def update(self, profile):
        update_query = """
        UPDATE profiles
        SET name = %s, company = %s, position = %s, challenges = %s, strengths = %s, summary = %s
        WHERE uuid = %s;
        """
        profile_data = profile.to_tuple()
        profile_data = profile_data[2:] + (profile_data[0],)  # move uuid to the end
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, profile_data)
                self.conn.commit()
                logger.info(f"Updated profile with uuid: {profile.uuid}")
        except psycopg2.Error as error:
            # self.conn.rollback()
            raise Exception(f"Error updating profile, because: {error.pgerror}")

    def save_profile(self, profile: ProfileDTO):
        self.create_table_if_not_exists()
        profile.strengths = json.dumps(
            profile.strengths
        )  # convert to json to insert to JSONB
        if self.exists(profile.uuid):
            self.update(profile)
        else:
            self.insert_profile(profile)
