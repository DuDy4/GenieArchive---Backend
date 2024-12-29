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
                event_topic VARCHAR,
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
        if self._exists(str(profile_uuid), tenant_id, event_topic):
            self._update_status(str(profile_uuid), tenant_id, event_topic, status)

        else:
            status_dto = StatusDTO(
                person_uuid=UUID(profile_uuid),
                tenant_id=tenant_id,
                event_topic=event_topic,
                current_event_start_time=datetime.now(timezone.utc),
                status=status,
            )
            self._insert_status(status_dto)


    def _exists(self, person_uuid: str, tenant_id: str, topic: str) -> bool:
        query = """
            SELECT EXISTS(SELECT 1 FROM statuses WHERE person_uuid = %s AND tenant_id = %s and event_topic = %s);
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (person_uuid, tenant_id, topic))
                    result = cursor.fetchone()
                    return result[0]
            except psycopg2.Error as error:
                logger.error(f"Error checking if status exists: {error.pgerror}")
                traceback.format_exc()

    def _insert_status(self, status_dto: StatusDTO):
        query = """
            INSERT INTO statuses (person_uuid, tenant_id, event_topic, current_event_start_time, status)
            VALUES (%s, %s, %s, %s, %s);
        """
        args = status_dto.to_tuple()
        logger.info(f"Inserting status: {args}")
        with db_connection() as conn:
            try:
                if self._exists(str(status_dto.person_uuid), status_dto.tenant_id, status_dto.event_topic):
                    logger.error(f"Status already exists for person_uuid={status_dto.person_uuid} and tenant_id={status_dto.tenant_id}")
                    return
                with conn.cursor() as cursor:
                    cursor.execute(query, args)
                    conn.commit()
            except psycopg2.Error as error:
                logger.error(f"Error inserting status: {error.pgerror}")
                traceback.format_exc()

    def _update_status(self, person_uuid: str, tenant_id: str, event_topic: str, status: StatusEnum):
        query = """
            UPDATE statuses
            SET current_event_start_time = %s, status = %s
            WHERE person_uuid = %s AND tenant_id = %s AND event_topic = %s;
        """
        args = (datetime.now(timezone.utc), status.value, person_uuid, tenant_id, event_topic)
        logger.info(f"Updating status: {args}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, args)
                    conn.commit()
            except psycopg2.Error as error:
                logger.error(f"Error updating status: {error.pgerror}")
                traceback.format_exc()

    def get_status(self, person_uuid: str, tenant_id: str, event_topic: str) -> StatusDTO:
        query = """
            SELECT person_uuid, tenant_id, event_topic, current_event_start_time, status
            FROM statuses WHERE person_uuid = %s AND tenant_id = %s AND event_topic = %s;
        """
        logger.info(f"Fetching status for person_uuid={person_uuid} and tenant_id={tenant_id}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (person_uuid, tenant_id, event_topic))
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

    def delete_status(self, person_uuid: str, tenant_id: str, event_topic: str):
        query = """
            DELETE FROM statuses WHERE person_uuid = %s AND tenant_id = %s AND event_topic = %s;
        """
        logger.info(f"Deleting status for person_uuid={person_uuid} and tenant_id={tenant_id} and event_topic={event_topic}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (person_uuid, tenant_id, event_topic))
                    conn.commit()
                    logger.info(f"Rows affected: {cursor.rowcount}")
                    if cursor.rowcount == 0:
                        logger.warning(f"No status found to delete for person_uuid={person_uuid}, tenant_id={tenant_id}, event_topic={event_topic}")
            except psycopg2.Error as error:
                logger.error(f"Error deleting status: {error}")
                logger.error(traceback.format_exc())
