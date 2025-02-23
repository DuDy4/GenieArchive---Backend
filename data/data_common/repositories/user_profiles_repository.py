import json
import traceback
from typing import Union

from data.data_common.data_transfer_objects.profile_category_dto import ProfileCategoryReasoning
import psycopg2

from common.utils.str_utils import get_uuid4

from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection
from data.data_common.data_transfer_objects.sales_action_item_dto import SalesActionItem
from data.data_common.data_transfer_objects.profile_dto import (
    ProfileDTO,
    Connection,
    Phrase,
    SalesCriteria,
)

logger = GenieLogger()


class UserProfilesRepository:
    def __init__(self):
        self.create_table_if_not_exists()


    def create_table_if_not_exists(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS user_profiles (
                id SERIAL PRIMARY KEY,
                uuid VARCHAR UNIQUE NOT NULL,
                profile_uuid VARCHAR NOT NULL,
                user_id VARCHAR,
                tenant_id VARCHAR NOT NULL,
                connections JSONB default '[]',
                get_to_know JSONB default '{}',
                sales_criteria JSONB default '[]',
                action_items JSONB default '[]'
                reasoning JSONB default '[]'
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

    def exists(self, profile_uuid: str, user_id: str) -> bool:
        logger.info(f"About to check if exists uuid: {profile_uuid} and user_id: {user_id}")
        exists_query = "SELECT 1 FROM user_profiles WHERE profile_uuid = %s AND user_id = %s;"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(exists_query, (profile_uuid, user_id))
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
            DELETE FROM user_profiles
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

    def get_get_to_know(self, uuid: str, user_id: str) -> dict:
        select_query = """
            SELECT get_to_know
            FROM user_profiles
            WHERE profile_uuid = %s
            AND user_id = %s;
            """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid, user_id))
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


    def get_sales_criteria(self, uuid: str, user_id: str) -> list[SalesCriteria]:
        select_query = """
            SELECT sales_criteria
            FROM user_profiles
            WHERE profile_uuid = %s
            AND user_id = %s;
            """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid, user_id))
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

    def get_sales_action_items(self, uuid: str, user_id: str) -> list[SalesActionItem]:
        select_query = """
            SELECT action_items
            FROM user_profiles
            WHERE profile_uuid = %s
            AND user_id = %s;
            """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid, user_id))
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

    def get_reasonings(self, uuid: str, user_id: str) -> list[ProfileCategoryReasoning]:
        select_query = """
            SELECT reasoning
            FROM user_profiles
            WHERE profile_uuid = %s
            AND user_id = %s;
            """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid, user_id))
                    row = cursor.fetchone()
                    if row and row[0]:
                        action_items = [ProfileCategoryReasoning.from_dict(item) for item in row[0]]
                        return action_items
                    else:
                        logger.warning(f"Couldn't find reasoning for {uuid}")
            except Exception as error:
                logger.error(f"Error fetching reasoning by uuid: {error}")
                traceback.print_exception(error)
        return None

    def get_sales_criteria_and_action_items(self, uuid: str, user_id: str) -> (
            tuple)[list[SalesCriteria] | None, list[SalesActionItem] | None]:
        select_query = """
            SELECT sales_criteria, action_items
            FROM user_profiles
            WHERE profile_uuid = %s AND user_id = %s;
            """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid, user_id))
                    row = cursor.fetchone()
                    if row:
                        sales_criteria = [SalesCriteria.from_dict(criteria) for criteria in row[0]]
                        action_items = [SalesActionItem.from_dict(item) for item in row[1]] if row[1] else []
                        return sales_criteria, action_items
                    else:
                        logger.warning(f"Couldn't find sales criteria and action items for {uuid}")
                        return None, None
            except Exception as error:
                logger.error(f"Error fetching sales criteria and action items by uuid: {error}")
                traceback.print_exception(error)
                return None, None


    def update_sales_criteria(self, uuid: str, user_id: str, sales_criteria: list[SalesCriteria]):
        update_query = """
                UPDATE user_profiles
                SET sales_criteria = %s
                WHERE profile_uuid = %s AND user_id = %s;
                """
        with db_connection() as conn:
            try:
                if not self.exists(uuid, user_id):
                    self._insert(uuid, user_id)
                with conn.cursor() as cursor:
                    cursor.execute(
                        update_query,
                        (
                            json.dumps([sc.to_dict() for sc in sales_criteria]),
                            uuid,
                            user_id,
                        ),
                    )
                    conn.commit()
                    logger.info(f"Updated sales criteria for {uuid}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating sales criteria, because: {error.pgerror}")


    def update_sales_action_items(self, uuid: str, user_id: str, action_items: list[SalesActionItem]):
        update_query = """
            UPDATE user_profiles
            SET action_items = %s
            WHERE profile_uuid = %s
            AND user_id = %s;
            """
        with db_connection() as conn:
            try:
                if not self.exists(uuid, user_id):
                    self._insert(uuid, user_id)
                with conn.cursor() as cursor:
                    cursor.execute(
                        update_query,
                        (
                            json.dumps([action_item.to_dict() for action_item in action_items]),
                            uuid,
                            user_id,
                        ),
                    )
                    conn.commit()
                    logger.info(f"Updated action items for {uuid}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating action items, because: {error.pgerror}")


    def update_reasonings(self, uuid: str, user_id: str, reasonings: list[ProfileCategoryReasoning]):
        update_query = """
            UPDATE user_profiles
            SET reasoning = %s
            WHERE profile_uuid = %s
            AND user_id = %s;
            """
        with db_connection() as conn:
            try:
                if not self.exists(uuid, user_id):
                    self._insert(uuid, user_id)
                with conn.cursor() as cursor:
                    cursor.execute(
                        update_query,
                        (
                            json.dumps([reasoning.to_dict() for reasoning in reasonings]),
                            uuid,
                            user_id,
                        ),
                    )
                    conn.commit()
                    logger.info(f"Updated reasoning for {uuid}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating reasoning, because: {error.pgerror}")


    def update_get_to_know(self, uuid, get_to_know, user_id):
        update_query = """
            UPDATE user_profiles
            SET get_to_know = %s
            WHERE profile_uuid = %s AND user_id = %s;
            """
        with db_connection() as conn:
            try:
                if not self.exists(uuid, user_id):
                    self._insert(uuid, user_id)
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (json.dumps(get_to_know), uuid))
                    conn.commit()
                    logger.info(f"Updated get to know for {uuid}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating get to know, because: {error.pgerror}")

    def update_sales_action_item_description(self, user_id: str, uuid: str, criteria: str, action_item: str):
        update_query = """
        UPDATE user_profiles
        SET action_items = (
            SELECT jsonb_agg(
                CASE
                    WHEN item->>'criteria' = %s THEN
                        jsonb_set(item, '{action_item}', %s::jsonb)
                    ELSE
                        item
                END
            )
            FROM jsonb_array_elements(action_items) AS item
        )
        WHERE profile_uuid = %s
        AND user_id = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    # Ensure action_item is a valid JSON string
                    action_item_json = json.dumps(action_item)
                    cursor.execute(update_query, (criteria, action_item_json, uuid, user_id))
                    conn.commit()
                    logger.info(f"Updated action item description for {uuid}")
                    return True
            except psycopg2.Error as error:
                raise Exception(f"Error updating action item description, because: {error.pgerror}")

    def _insert(self, profile_uuid: str, user_id: str, tenant_id: str = None) -> Union[str, None]:
        insert_query = """
                    INSERT INTO user_profiles (uuid, profile_uuid, user_id, tenant_id)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id;
                    """
        if not tenant_id:
            tenant_id = logger.get_tenant_id()
        profile_data = (get_uuid4(), profile_uuid, user_id, tenant_id)

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

    # def _update(self, profile: ProfileDTO, user_id: str):
    #     update_query = """
    #         UPDATE user_profiles
    #         SET connections = %s, get_to_know = %s, sales_criteria = %s
    #         WHERE profile_uuid = %s AND user_id = %s;
    #         """
    #     profile_dict = profile.to_dict()
    #     profile_data = (
    #         json.dumps([c if isinstance(c, dict) else c.to_dict() for c in profile_dict["connections"]]),
    #         json.dumps({k: [p if isinstance(p, dict) else p.to_dict() for p in v] for k, v in profile_dict["get_to_know"].items()}),
    #         json.dumps({k: [p if isinstance(p, dict) else p.to_dict() for p in v] for k, v in profile_dict["sales_criteria"].items()}),
    #         str(profile_dict["uuid"]),
    #         user_id
    #     )
    #     with db_connection() as conn:
    #         try:
    #             with conn.cursor() as cursor:
    #                 cursor.execute(update_query, profile_data)
    #                 conn.commit()
    #                 logger.info(f"Updated profile with uuid: {profile.uuid}")
    #         except psycopg2.Error as error:
    #             raise Exception(f"Error updating profile, because: {error.pgerror}")
