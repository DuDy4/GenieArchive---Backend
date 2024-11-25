import json
import traceback
from typing import Union

import psycopg2

from common.utils.str_utils import get_uuid4
from data.data_common.data_transfer_objects.profile_dto import (
    ProfileDTO,
    Connection,
    Phrase,
    SalesCriteria,
)
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection

logger = GenieLogger()


class TenantProfilesRepository:
    def __init__(self):
        self.create_table_if_not_exists()


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
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
            except Exception as error:
                logger.error(f"Error creating table: {error}")
                traceback.print_exc()

    def exists(self, profile_uuid: str, tenant_id: str) -> bool:
        logger.info(f"About to check if uuid exists: {profile_uuid}")
        exists_query = "SELECT 1 FROM tenant_profiles WHERE profile_uuid = %s AND tenant_id = %s;"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
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
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(delete_query, (email,))
                    conn.commit()
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
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
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


    def get_sales_criteria(self, uuid: str, tenant_id: str) -> list[SalesCriteria]:
        select_query = """
            SELECT sales_criteria
            FROM tenant_profiles
            WHERE profile_uuid = %s
            AND tenant_id = %s;
            """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid, tenant_id))
                    row = cursor.fetchone()
                    if row:
                        sales_criteria = [SalesCriteria.from_dict(criteria) for criteria in row[0]]
                        return sales_criteria
                    else:
                        logger.error(f"Error with getting sales criteria for {uuid}")
                        traceback.print_exc()
            except Exception as error:
                logger.error(f"Error fetching sales criteria by uuid: {error}")
                traceback.print_exception(error)
        return None
            
    def update_sales_criteria(self, uuid: str, tenant_id, sales_criteria: list[SalesCriteria]):
        update_query = """
            UPDATE tenant_profiles
            SET sales_criteria = %s
            FROM persons
            WHERE tenant_profiles.profile_uuid = %s AND tenant_profiles.tenant_id = %s;
            """
        with db_connection() as conn:
            try:
                if not self.exists(uuid, tenant_id):
                    self._insert(uuid, tenant_id)
                with conn.cursor() as cursor:
                    cursor.execute(
                        update_query,
                        (
                            json.dumps([sc.to_dict() for sc in sales_criteria]),
                            uuid,
                            tenant_id,
                        ),
                    )
                    conn.commit()
                    logger.info(f"Updated sales criteria for {uuid}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating sales criteria, because: {error.pgerror}")


    def update_get_to_know(self, uuid, get_to_know, tenant_id):
        update_query = """
            UPDATE tenant_profiles
            SET get_to_know = %s
            WHERE profile_uuid = %s;
            """
        with db_connection() as conn:
            try:
                if not self.exists(uuid, tenant_id):
                    self._insert(uuid, tenant_id)
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (json.dumps(get_to_know), uuid))
                    conn.commit()
                    logger.info(f"Updated get to know for {uuid}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating get to know, because: {error.pgerror}")

    def _insert(self, profile_uuid: str, tenant_id: str) -> Union[str, None]:
        insert_query = """
                    INSERT INTO tenant_profiles (uuid, profile_uuid, tenant_id)
                    VALUES (%s, %s, %s)
                    RETURNING id;
                    """
        profile_data = (get_uuid4(), profile_uuid, tenant_id)

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

    def _update(self, profile: ProfileDTO, tenant_id: str):
        update_query = """
            UPDATE tenant_profiles
            SET connections = %s, get_to_know = %s, sales_criteria = %s
            WHERE profile_uuid = %s
            AND tenant_id = %s;
            """
        profile_dict = profile.to_dict()
        profile_data = (
            json.dumps([c if isinstance(c, dict) else c.to_dict() for c in profile_dict["connections"]]),
            json.dumps({k: [p if isinstance(p, dict) else p.to_dict() for p in v] for k, v in profile_dict["get_to_know"].items()}),
            json.dumps({k: [p if isinstance(p, dict) else p.to_dict() for p in v] for k, v in profile_dict["sales_criteria"].items()}),
            str(profile_dict["uuid"]),
            tenant_id
        )
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, profile_data)
                    conn.commit()
                    logger.info(f"Updated profile with uuid: {profile.uuid}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating profile, because: {error.pgerror}")
