from datetime import datetime, timezone
from dbm import error
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
                ctx_id VARCHAR,
                object_id VARCHAR NOT NULL,
                object_type VARCHAR,
                tenant_id VARCHAR NOT NULL,
                event_topic VARCHAR,
                previous_event_topic VARCHAR,
                current_event_start_time TIMESTAMP WITH TIME ZONE,
                status VARCHAR,
                error_message TEXT
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


    def start_status(self, ctx_id: str, object_id: str, object_type: str, tenant_id: str, previous_event_topic: str, next_event_topic: str):
        """
        This function should happen on the creation of event, when it is sent to eventhub (before the consumer processes it)
        """
        insert_query = """
            INSERT INTO statuses (ctx_id, object_id, object_type, tenant_id, event_topic, previous_event_topic, current_event_start_time, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        args = (str(ctx_id), object_id, object_type, tenant_id, next_event_topic, previous_event_topic, datetime.now(timezone.utc), StatusEnum.STARTED.value)
        logger.info(f"Starting status: {args}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, args)
                    conn.commit()
            except psycopg2.Error as error:
                logger.error(f"Error starting status: {error.pgerror}")
                traceback.format_exc()


    def update_status(self, ctx_id:str, object_id: str, tenant_id: str, event_topic: str, status: StatusEnum, error_message: str = None):
        query = """
            UPDATE statuses
            SET current_event_start_time = %s, status = %s, error_message = %s
            WHERE ctx_id = %s AND object_id = %s AND tenant_id = %s AND event_topic = %s;
        """
        args = (datetime.now(timezone.utc), status.value, error_message, str(ctx_id), object_id, tenant_id, event_topic)
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, args)
                    conn.commit()
            except psycopg2.Error as error:
                logger.error(f"Error updating status: {error.pgerror}")
                traceback.format_exc()


    def _exists(self, ctx_id: str, object_id: str, tenant_id: str, topic: str) -> bool:
        query = """
            SELECT EXISTS(SELECT 1 FROM statuses 
                          WHERE ctx_id = %s AND object_id = %s AND tenant_id = %s and event_topic = %s);
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (ctx_id, object_id, tenant_id, topic))
                    result = cursor.fetchone()
                    return result[0]
            except psycopg2.Error as error:
                logger.error(f"Error checking if status exists: {error.pgerror}")
                traceback.format_exc()


    def get_status(self, ctx_id: str, object_id: str, tenant_id: str, event_topic: str) -> StatusDTO:
        query = """
            SELECT ctx_id, object_id, object_type, tenant_id, event_topic, previous_event_topic, current_event_start_time, status
            FROM statuses WHERE ctx_id = %s AND object_id = %s AND tenant_id = %s AND event_topic = %s;
        """
        logger.info(f"Fetching status for object_id={object_id} and tenant_id={tenant_id}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (str(ctx_id), object_id, tenant_id, event_topic))
                    result = cursor.fetchone()
                    if result:
                        return StatusDTO.from_tuple(result)
                    else:
                        logger.info(f"No status found for object_id={object_id} and tenant_id={tenant_id}")
                        return None
            except psycopg2.Error as error:
                logger.error(f"Error fetching status: {error.pgerror}")
                traceback.format_exc()

    def get_error_message(self, ctx_id: str, object_id: str, tenant_id: str, event_topic: str) -> str:
        query = """
            SELECT error_message FROM statuses WHERE ctx_id = %s AND object_id = %s AND tenant_id = %s AND event_topic = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (ctx_id, object_id, tenant_id, event_topic))
                    result = cursor.fetchone()
                    if result:
                        logger.info(f"Error message fetched: {result}")
                        return result[0]
                    else:
                        logger.info(f"No error message found for object_id={object_id} and tenant_id={tenant_id}")
                        return None
            except psycopg2.Error as error:
                logger.error(f"Error fetching error message: {error.pgerror}")
                traceback.format_exc()

    def delete_status(self, ctx_id: str, object_id: str, tenant_id: str, event_topic: str):
        query = """
            DELETE FROM statuses WHERE ctx_id = %s AND object_id = %s AND tenant_id = %s AND event_topic = %s;
        """
        logger.info(f"Deleting status for object_id={object_id} and tenant_id={tenant_id} and event_topic={event_topic}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (ctx_id, object_id, tenant_id, event_topic))
                    conn.commit()
                    logger.info(f"Rows affected: {cursor.rowcount}")
                    if cursor.rowcount == 0:
                        logger.warning(f"No status found to delete for object_id={object_id}, tenant_id={tenant_id}, event_topic={event_topic}")
            except psycopg2.Error as error:
                logger.error(f"Error deleting status: {error}")
                logger.error(traceback.format_exc())
