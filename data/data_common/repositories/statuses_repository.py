from datetime import datetime, timezone
from uuid import UUID
import psycopg2
import traceback
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection
from data.data_common.data_transfer_objects.status_dto import StatusDTO, StatusEnum

logger = GenieLogger()

class StatusesRepository:
    def __init__(self):
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS statuses (
                id SERIAL PRIMARY KEY,
                person_uuid VARCHAR NOT NULL,
                tenant_id VARCHAR NOT NULL,
                current_event VARCHAR,
                current_event_start_time TIMESTAMP WITH TIME ZONE,
                status VARCHAR
            );
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
            except psycopg2.Error as error:
                logger.error(f"Error creating table: {error.pgerror}")
                traceback.format_exc()

    def save_status(self, profile_uuid: str | UUID, tenant_id: str, event_topic: str, status: StatusEnum):
        if self.exists(str(profile_uuid), tenant_id):
            self.update_status(str(profile_uuid), tenant_id, event_topic, status)

        else:
            status_dto = StatusDTO(
                person_uuid=UUID(profile_uuid),
                tenant_id=tenant_id,
                current_event=event_topic,
                current_event_start_time=datetime.now(timezone.utc),
                status=status,
            )
            self.insert_status(status_dto)


    def exists(self, person_uuid: str, tenant_id: str) -> bool:
        query = """
            SELECT EXISTS(SELECT 1 FROM statuses WHERE person_uuid = %s AND tenant_id = %s);
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (person_uuid, tenant_id))
                    result = cursor.fetchone()
                    return result[0]
            except psycopg2.Error as error:
                logger.error(f"Error checking if status exists: {error.pgerror}")
                traceback.format_exc()

    def insert_status(self, status_dto: StatusDTO):
        query = """
            INSERT INTO statuses (person_uuid, tenant_id, current_event, current_event_start_time, status)
            VALUES (%s, %s, %s, %s, %s);
        """
        args = status_dto.to_tuple()
        logger.info(f"Inserting status: {args}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, args)
                    conn.commit()
            except psycopg2.Error as error:
                logger.error(f"Error inserting status: {error.pgerror}")
                traceback.format_exc()

    def update_status(self, person_uuid: str, tenant_id: str, event_topic: str, status: StatusEnum):
        query = """
            UPDATE statuses
            SET current_event = %s, current_event_start_time = %s, status = %s
            WHERE person_uuid = %s AND tenant_id = %s;
        """
        args = (event_topic, datetime.now(timezone.utc), status.value, person_uuid, tenant_id)
        logger.info(f"Updating status: {args}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, args)
                    conn.commit()
            except psycopg2.Error as error:
                logger.error(f"Error updating status: {error.pgerror}")
                traceback.format_exc()

    def get_status(self, person_uuid: str, tenant_id: str) -> StatusDTO:
        query = """
            SELECT person_uuid, tenant_id, current_event, current_event_start_time, status
            FROM statuses WHERE person_uuid = %s AND tenant_id = %s;
        """
        logger.info(f"Fetching status for person_uuid={person_uuid} and tenant_id={tenant_id}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (person_uuid, tenant_id))
                    result = cursor.fetchone()
                    if result:
                        logger.info(f"Status fetched: {result}")
                        return StatusDTO.from_tuple(result)
                    else:
                        logger.info(f"No status found for person_uuid={person_uuid} and tenant_id={tenant_id}")
                        return None
            except psycopg2.Error as error:
                logger.error(f"Error fetching status: {error.pgerror}")
                traceback.format_exc()
