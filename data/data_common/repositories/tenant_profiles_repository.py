import json
import traceback
from datetime import date, datetime
from typing import Union, Optional

import psycopg2
from pydantic import AnyUrl

from common.utils.str_utils import get_uuid4
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.data_transfer_objects.profile_dto import (
    ProfileDTO,
    Strength,
    Connection,
    Phrase,
    Hobby,
    UUID,
)
from common.genie_logger import GenieLogger

logger = GenieLogger()


class TenantProfilesRepository:
    def __init__(self, conn):
        self.conn = conn
        self.create_table_if_not_exists()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS tenant_profiles (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            profile_uuid VARCHAR UNIQUE NOT NULL,
            tenant_id VARCHAR NOT NULL,
            connections JSONB default '[]',
            get_to_know JSONB default '{}'
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
        except Exception as error:
            logger.error(f"Error creating table: {error}")
            traceback.print_exc()



    def exists(self, profile_uuid: str, tenant_id: str) -> bool:
        logger.info(f"About to check if uuid exists: {profile_uuid}")
        exists_query = "SELECT 1 FROM tenant_profiles WHERE uuid = %s AND tenant_id = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, (profile_uuid, tenant_id))
                result = cursor.fetchone() is not None
                logger.info(f"{profile_uuid} existence in database: {result}")
                return result
        except psycopg2.Error as error:
            logger.error(f"Error checking existence of uuid {profile_uuid}: {error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    
    def delete_by_email(self, email: str):
        delete_query = """
        DELETE FROM tenant_profiles
        WHERE profile_uuid = (SELECT uuid FROM persons WHERE email = %s);
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_query, (email,))
                self.conn.commit()
                logger.info(f"Deleted profile for {email}")
        except psycopg2.Error as error:
            raise Exception(f"Error deleting profile, because: {error.pgerror}")


    def get_get_to_know(self, uuid: str, tenant_id: str) -> dict:
        select_query = """
        SELECT get_to_know
        FROM tenant_profiles
        WHERE profile_uuid = %s
        AND tenant_id = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid, tenant_id))
                row = cursor.fetchone()
                if row:
                    get_to_know = {k: [Phrase.from_dict(p) for p in v] for k, v in row[0].items()}
                    return get_to_know
                else:
                    logger.error(f"Error with getting get to know for {uuid}")
                    traceback.print_exc()
        except Exception as error:
            logger.error(f"Error fetching get to know by uuid: {error}")
            traceback.print_exception(error)
        return None

    def get_connections_by_email(self, email: str) -> list:
        if not email:
            return None
        select_query = """
        SELECT connections FROM tenant_profiles
        JOIN persons on persons.uuid = tenant_profiles.profile_uuid
        WHERE persons.email = %s;
        """
        try:
            with self.conn.cursor() as cursor:
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



    def update_connections_by_email(self, email: str, connections: list[Connection]):
        update_query = """
        UPDATE tenant_profiles
        SET connections = %s
        FROM persons
        WHERE tenant_profiles.profile_uuid = persons.uuid AND persons.email = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    update_query,
                    (
                        json.dumps([con.to_dict() for con in connections]),
                        email,
                    ),
                )
                self.conn.commit()
                logger.info(f"Updated connections for {email}")
        except psycopg2.Error as error:
            raise Exception(f"Error updating connections, because: {error.pgerror}")



    def update_get_to_know(self, uuid, get_to_know, tenant_id):
        update_query = """
        UPDATE tenant_profiles
        SET get_to_know = %s
        WHERE profile_uuid = %s;
        """
        try:
            if not self.exists(uuid, tenant_id):
                self._insert(uuid, tenant_id)
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (json.dumps(get_to_know), uuid))
                self.conn.commit()
                logger.info(f"Updated get to know for {uuid}")
        except psycopg2.Error as error:
            raise Exception(f"Error updating get to know, because: {error.pgerror}")

    def _insert(self, profile_uuid: str, tenant_id: str) -> Union[str, None]:
        insert_query = """
                INSERT INTO tenant_profiles (uuid, profile_uuid, tenant_id)
                VALUES (%s, %s, %s)
                RETURNING id;
                """

        profile_data = (
            get_uuid4(),
            profile_uuid,
            tenant_id
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

    def _update(self, profile: ProfileDTO, tenant_id: str):
        update_query = """
        UPDATE tenant_profiles
        SET connections = %s, get_to_know = %s
        WHERE profile_uuid = %s
        AND tenant_id = %s;
        """
        profile_dict = profile.to_dict()
        profile_data = (
            json.dumps([c if isinstance(c, dict) else c.to_dict() for c in profile_dict["connections"]]),
            json.dumps(
                {
                    k: [p if isinstance(p, dict) else p.to_dict() for p in v]
                    for k, v in profile_dict["get_to_know"].items()
                }
            ),
            str(profile_dict["uuid"]),
            tenant_id
        )

        logger.info(f"Persisting profile data {profile_data}")
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, profile_data)
                self.conn.commit()
                logger.info(f"Updated profile with uuid: {profile.uuid}")
        except psycopg2.Error as error:
            raise Exception(f"Error updating profile, because: {error.pgerror}")