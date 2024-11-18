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
            deal_id UUID PRIMARY KEY,
            name VARCHAR NOT NULL,
            description TEXT,
            criterias JSONB NOT NULL,
            tenant_id VARCHAR,
            company_id VARCHAR,
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
        INSERT INTO deals (deal_id, name, description, criterias, tenant_id, company_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING deal_id;
        """
        deal_data = deal.to_tuple()

        logger.info(f"About to insert deal data: {deal_data}")

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, deal_data)
                    conn.commit()
                    deal_id = cursor.fetchone()[0]
                    logger.info(f"Inserted deal into database. Deal ID: {deal_id}")
                    return deal_id
            except psycopg2.Error as error:
                logger.error(f"Error inserting deal: {error.pgerror}")
                traceback.print_exc()
                return None

