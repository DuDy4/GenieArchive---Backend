import json
import traceback
from typing import Union

from select import select

from data.data_common.data_transfer_objects.sales_action_item_dto import SalesActionItem
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
                profile_uuid VARCHAR NOT NULL,
                tenant_id VARCHAR NOT NULL,
                connections JSONB default '[]',
                get_to_know JSONB default '{}',
                sales_criteria JSONB default '[]',
                action_items JSONB default '[]'
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
        logger.info(f"About to check if exists uuid: {profile_uuid} and tenant_id: {tenant_id}")
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
                        logger.warning(f"Error with getting get to know for {uuid}")
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
                    logger.info(f"Row: {row}")
                    if row and row[0]:
                        sales_criteria = [SalesCriteria.from_dict(criteria) for criteria in row[0]]
                        return sales_criteria
                    else:
                        logger.warning(f"Couldn't find sales criteria for {uuid}")
            except Exception as error:
                logger.error(f"Error fetching sales criteria by uuid: {error}")
                traceback.print_exception(error)
        return None

    def get_sales_action_items(self, uuid: str, tenant_id: str) -> list[SalesActionItem]:
        select_query = """
            SELECT action_items
            FROM tenant_profiles
            WHERE profile_uuid = %s
            AND tenant_id = %s;
            """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid, tenant_id))
                    row = cursor.fetchone()
                    if row and row[0]:
                        action_items = [SalesActionItem.from_dict(item) for item in row[0]]
                        return action_items
                    else:
                        logger.warning(f"Couldn't find action items for {uuid}")
            except Exception as error:
                logger.error(f"Error fetching action items by uuid: {error}")
                traceback.print_exception(error)
        return None

    def get_sales_criteria_and_action_items(self, uuid: str, tenant_id: str) -> (
            tuple)[list[SalesCriteria] | None, list[SalesActionItem] | None]:
        select_query = """
            SELECT sales_criteria, action_items
            FROM tenant_profiles
            WHERE profile_uuid = %s AND tenant_id = %s;
            """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid, tenant_id))
                    row = cursor.fetchone()
                    if row:
                        sales_criteria = [SalesCriteria.from_dict(criteria) for criteria in row[0]]
                        action_items = [SalesActionItem.from_dict(item) for item in row[1]]
                        return sales_criteria, action_items
                    else:
                        logger.warning(f"Couldn't find sales criteria and action items for {uuid}")
                        return None, None
            except Exception as error:
                logger.error(f"Error fetching sales criteria and action items by uuid: {error}")
                traceback.print_exception(error)

    def get_all_uuids_and_tenants_id_without_action_items(self, forced: bool = False):
        select_query = f"""
            SELECT o.person_uuid, o.tenant_id FROM ownerships o
            join tenants t on t.tenant_id = o.tenant_id
            join persons p on p.uuid = o.person_uuid
            join profiles pr on pr.uuid = p.uuid
            left join tenant_profiles tp on p.uuid = tp.profile_uuid AND o.tenant_id = tp.tenant_id
            {f"WHERE tp.action_items = '[]' OR tp.action_items IS NULL" if not forced else ""}
            ORDER BY o.id desc;            
            """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query)
                    rows = cursor.fetchall()
                    result_object_list = []
                    for row in rows:
                        result_object_list.append({
                            "profile_uuid": row[0],
                            "tenant_id": row[1]
                        })
                    return result_object_list
            except psycopg2.Error as error:
                raise Exception(f"Error fetching uuids and tenants id without action items, because: {error.pgerror}")

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


    def update_sales_action_items(self, uuid: str, tenant_id: str, action_items: list[SalesActionItem]):
        update_query = """
            UPDATE tenant_profiles
            SET action_items = %s
            WHERE profile_uuid = %s
            AND tenant_id = %s;
            """
        with db_connection() as conn:
            try:
                if not self.exists(uuid, tenant_id):
                    self._insert(uuid, tenant_id)
                with conn.cursor() as cursor:
                    cursor.execute(
                        update_query,
                        (
                            json.dumps([action_item.to_dict() for action_item in action_items]),
                            uuid,
                            tenant_id,
                        ),
                    )
                    conn.commit()
                    logger.info(f"Updated action items for {uuid}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating action items, because: {error.pgerror}")


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
