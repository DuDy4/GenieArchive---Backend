import traceback
from data.data_common.data_transfer_objects.profile_dto import ProfileDTO
from data.data_common.utils.str_utils import get_uuid4
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

    def get_deal(self, tenant_id, company_id) -> Optional[DealDTO]:
        select_query = """
        SELECT * FROM deals
        WHERE tenant_id = %s
        AND company_id = %s;
        """
        logger.info(f"About to get deal data for company -  {company_id} and tenant - {tenant_id}")

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (tenant_id, company_id))
                    deal_data = cursor.fetchone()
                    if not deal_data:
                        return None
                    return DealDTO.from_tuple(deal_data)
            except psycopg2.Error as error:
                logger.error(f"Error getting deal: {error.pgerror}")
                traceback.print_exc()
                return None
            
    def get_deal_profiles(self, tenant_id, company_id) -> List[ProfileDTO]:
        select_query = """

        SELECT p.uuid from companies c
        JOIN persons pe on pe.email LIKE '%' || c.domain
        JOIN profiles p on p.uuid = pe.uuid
        JOIN ownerships o on o.person_uuid = p.uuid
        WHERE o.tenant_id = %s
        AND c.uuid = %s;
        """
        logger.info(f"About to get profiles for deal -  {company_id} and tenant - {tenant_id}")

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (tenant_id, company_id))
                    profiles_data = cursor.fetchall()
                    if not profiles_data:
                        return []
                    return [profile_data[0] for profile_data in profiles_data]
            except psycopg2.Error as error:
                logger.error(f"Error getting profiles: {error.pgerror}")
                traceback.print_exc()
                return []

    # Deal Methods
    def insert_deal(self, tenant_id, company_id) -> Optional[str]:
        insert_query = """
        INSERT INTO deals (deal_id, tenant_id, company_id)
        VALUES (%s, %s)
        RETURNING deal_id;
        """
        logger.info(f"About to insert deal data for company -  {company_id} and tenant - {tenant_id}")

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    uuid = get_uuid4()
                    cursor.execute(insert_query, (uuid, tenant_id, company_id))
                    conn.commit()
                    deal_id = cursor.fetchone()[0]
                    logger.info(f"Inserted deal into database. Deal ID: {deal_id}")
                    return deal_id
            except psycopg2.Error as error:
                logger.error(f"Error inserting deal: {error.pgerror}")
                traceback.print_exc()
                return None
            
    def update_deal_criteria(self, tenant_id: str, company_id: str, criterias: List[DealCriteriaDTO]) -> bool:
        update_query = """
        UPDATE deals
        SET criterias = %s
        WHERE tenant_id = %s
        AND company_id = %s;
        """
        logger.info(f"About to update deal criteria for deal - {company_id}")

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (criterias, tenant_id, company_id))
                    conn.commit()
                    logger.info(f"Updated deal criteria for deal - {company_id}")
                    return True
            except psycopg2.Error as error:
                logger.error(f"Error updating deal criteria: {error.pgerror}")
                traceback.print_exc()
                return False

