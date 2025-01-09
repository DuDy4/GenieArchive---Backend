import traceback
from uuid import UUID

import psycopg2
from datetime import timedelta, datetime

from data.data_common.data_transfer_objects.badges_dto import BadgesEventTypes
from data.data_common.data_transfer_objects.stats_dto import StatsDTO
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection

logger = GenieLogger()


class StatsRepository:
    def __init__(self):
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stats (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            action VARCHAR,
            entity VARCHAR,
            entity_id VARCHAR,
            timestamp TIMESTAMP,
            email VARCHAR,
            tenant_id VARCHAR,
            user_id VARCHAR NOT NULL
        );
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
            except Exception as error:
                logger.error("Error creating table:", error)

    def insert(self, stats: StatsDTO) -> str | None | UUID:
        insert_query = """
        INSERT INTO stats (uuid, action, entity, entity_id, timestamp, email, tenant_id, user_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        stats_data = stats.to_tuple()

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, stats_data)
                    conn.commit()
                    stats_id = cursor.fetchone()[0]
                    logger.info(f"Inserted stats to database. Stats id: {stats_id}")
                    return stats.uuid
            except psycopg2.Error as error:
                logger.error(f"Error inserting stats: {error.pgerror}")
                traceback.print_exc()
                raise Exception(f"Error inserting stats, because: {error.pgerror}")

    def exists(self, stats: StatsDTO) -> bool:
        exists_query = """SELECT uuid FROM stats 
                WHERE action = %s
                AND entity = %s
                AND entity_id = %s
                AND email = %s;
        """

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(exists_query, (stats.action, stats.entity, stats.entity_id, stats.email))
                    result = cursor.fetchone()
                    return result[0] if result else None
            except psycopg2.Error as error:
                logger.error(f"Error checking existence of stats ({stats}): {error}")
                return False
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return False

    def should_log_event(self, stats_dto: StatsDTO) -> bool:
        start_of_hour = stats_dto.timestamp.replace(minute=0, second=0, microsecond=0)
        query = """
            SELECT 1 FROM stats
            WHERE email = %s AND action = %s AND entity = %s AND entity_id = %s 
            AND timestamp >= %s AND timestamp < %s;
        """
        params = (stats_dto.email, stats_dto.action, stats_dto.entity, stats_dto.entity_id, start_of_hour, start_of_hour + timedelta(hours=1))

        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                existing_event = cursor.fetchone()

        return existing_event is None

    def count_events_from_date(self, stats: StatsDTO, timestamp: datetime) -> bool:
        query = """
            SELECT COUNT(id) FROM stats
            WHERE email = %s AND action = %s AND entity = %s AND entity_id = %s 
            AND timestamp >= %s;
        """
        params = (stats.email, stats.action, stats.entity, stats.entity_id, timestamp)

        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                return result[0] if result else 0

    def get_stats_by_email(self, email: str) -> StatsDTO | None:
        select_query = """
        SELECT * FROM stats WHERE email = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (email,))
                    stats = cursor.fetchone()
                    if stats:
                        logger.info(f"Got stats with email {email}")
                        return StatsDTO.from_tuple(stats[1:])
                    logger.info(f"Stats with email {email} does not exist")
                    return None
            except psycopg2.Error as error:
                logger.error(f"Error getting person by email: {error}")
                traceback.print_exc()
                return None

    def get_file_categories_stats(self, email):
        query = """
            SELECT entity_id FROM stats
            WHERE email = %s AND entity = 'FILE_CATEGORY';
        """
        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (email,))
                file_categories = cursor.fetchall()
                logger.info(f"Got file categories for email {email}")
                file_categories = [category[0] for category in file_categories]
                logger.info(f"File categories: {file_categories}")
                return list(set(file_categories))