import traceback
import psycopg2
from datetime import timedelta
from data.data_common.data_transfer_objects.stats_dto import StatsDTO
from common.genie_logger import GenieLogger

logger = GenieLogger()


class StatsRepository:
    def __init__(self, conn):
        self.conn = conn
        self.create_table_if_not_exists()

    def __del__(self):
        if self.conn:
            self.conn.close()

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
            tenant_id VARCHAR
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
        except Exception as error:
            logger.error("Error creating table:", error)

    def insert(self, stats: StatsDTO) -> str | None:
        """
        :param stats: StatsDTO object with stats data to insert into database
        :return the id of the newly created stats in database:
        """
        insert_query = """
        INSERT INTO stats (uuid, action, entity, entity_id, timestamp, email, tenant_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        stats_data = stats.to_tuple()

        logger.info(f"About to insert stats data: {stats_data}")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, stats_data)
                self.conn.commit()
                stats_id = cursor.fetchone()[0]
                logger.info(f"Inserted stats to database. Stats id: {stats_id}")
                return stats.uuid
        except psycopg2.Error as error:
            logger.error(f"Error inserting stats: {error.pgerror}")
            traceback.print_exc()
            raise Exception(f"Error inserting stats, because: {error.pgerror}")

    def exists(self, stats: StatsDTO) -> bool:
        logger.info(f"About to check if stats exists: {stats}")
        exists_query = """SELECT uuid FROM stats 
                WHERE action = %s
                AND entity = %s
                AND entity_id = %s
                AND email = %s;
        """

        try:
            with self.conn.cursor() as cursor:
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
        """
        Check if a similar event exists within the same hour.
        
        :param stats_dto: StatsDTO object containing event details.
        :return: True if the event should be logged, False otherwise.
        """
        # Extract relevant fields from StatsDTO
        email = stats_dto.email
        action = stats_dto.action
        entity = stats_dto.entity
        entity_id = stats_dto.entity_id
        timestamp = stats_dto.timestamp

        start_of_hour = timestamp.replace(minute=0, second=0, microsecond=0)

        query = """
            SELECT 1 FROM stats
            WHERE email = %s AND action = %s AND entity = %s AND entity_id = %s 
            AND timestamp >= %s AND timestamp < %s;
        """
        
        params = (email, action, entity, entity_id, start_of_hour, start_of_hour + timedelta(hours=1))

        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            existing_event = cursor.fetchone()

        return existing_event is None


    def get_stats_by_email(self, email: str) -> list[StatsDTO] | None:
        select_query = """
        SELECT * FROM stats WHERE email = %s;
        """
        try:
            with self.conn.cursor() as cursor:
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



