import traceback
import psycopg2
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection
from typing import List, Optional
from data.data_common.data_transfer_objects.deal_dto import (
    DealDTO,
    DealCriteriaDTO
)

logger = GenieLogger()


class DealsRepository:
    def __init__(self):
        self.create_tables_if_not_exists()

    def create_tables_if_not_exists(self):
        deal_table_query = """
        CREATE TABLE IF NOT EXISTS deals (
            id SERIAL PRIMARY KEY,
            uuid UUID PRIMARY KEY,
            name VARCHAR NOT NULL,
            description TEXT,
            tenant_id VARCHAR,
            company_uuid VARCHAR,
            criterias JSONB,
            status VARCHAR DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(deal_table_query)
                    conn.commit()
            except Exception as error:
                logger.error(f"Error creating tables: {error}")
                traceback.print_exc()

    # Deal Methods
    def insert_deal(self, deal: DealDTO) -> Optional[str]:
        insert_query = """
        INSERT INTO deals (uuid, name, description, tenant_id, company_uuid, criterias, created_at, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s,  current_timestamp, current_timestamp)
        RETURNING uuid;
        """
        deal_data = deal.to_tuple()

        logger.info(f"About to insert deal data: {deal_data}")

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, deal_data)
                    conn.commit()
                    uuid = cursor.fetchone()[0]
                    logger.info(f"Inserted deal into database. Deal ID: {uuid}")
                    return uuid
            except psycopg2.Error as error:
                logger.error(f"Error inserting deal: {error.pgerror}")
                traceback.print_exc()
                return None

    def exists(self, uuid: str):
        select_query = """
        SELECT uuid FROM deals WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid,))
                    return cursor.fetchone() is not None
            except psycopg2.Error as error:
                logger.error(f"Error checking if deal exists: {error.pgerror}")
                traceback.print_exc()
                return False

    def get_deal(self, uuid: str):
        select_query = """
        SELECT uuid, name, description, tenant_id, company_uuid, criterias, FROM deals WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid,))
                    deal_data = cursor.fetchone()
                    if deal_data:
                        return DealDTO.from_tuple(deal_data)
                    return None
            except psycopg2.Error as error:
                logger.error(f"Error getting deal: {error.pgerror}")
                traceback.print_exc()
                return None

    def update_deal(self, deal: DealDTO):
        update_query = """
        UPDATE deals SET name = %s, description = %s, tenant_id = %s, company_uuid = %s, criterias = %s, last_updated = current_timestamp WHERE uuid = %s;
        """
        deal_data = deal.to_tuple()

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, deal_data[1:] + (deal_data[0],))
                    conn.commit()
                    logger.info(f"Updated deal in database. Deal ID: {deal_data[0]}")
            except psycopg2.Error as error:
                logger.error(f"Error updating deal: {error.pgerror}")
                traceback.print_exc()

    def save_deal(self, deal: DealDTO):
        if self.exists(str(deal.uuid)):
            self.update_deal(deal)
        else:
            self.insert_deal(deal)

    def update_status(self, uuid: str, status: str):
        update_query = """
        UPDATE deals SET status = %s WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (status, uuid))
                    conn.commit()
                    logger.info(f"Updated deal status in database. Deal ID: {uuid}")
            except psycopg2.Error as error:
                logger.error(f"Error updating deal status: {error.pgerror}")
                traceback.print_exc()

    # def get_deal_criteria
