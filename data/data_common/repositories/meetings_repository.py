import traceback
from typing import Optional
import psycopg2
from loguru import logger
import json

from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO


class MeetingsRepository:
    def __init__(self, conn):
        self.conn = conn

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS meetings (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            tenant_id VARCHAR,
            participants_emails VARCHAR,
            location VARCHAR,
            subject VARCHAR,
            start_time INT,
            end_time INT
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
                logger.info("Table meetings created successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f"Error: {error}")
            traceback.print_exc()
            self.conn.rollback()

    def insert_meeting(self, meeting: MeetingDTO) -> Optional[str]:
        insert_query = """
        INSERT INTO meetings (uuid, tenant_id, participants_emails, location, subject, start_time, end_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert meeting: {meeting}")
        meeting_data = meeting.to_tuple()

        logger.info(f"About to insert meeting data: {meeting_data}")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, meeting_data)
                self.conn.commit()
                meeting_id = cursor.fetchone()[0]
                logger.info(f"Inserted meeting to database. Meeting id: {meeting_id}")
                return meeting_id
        except psycopg2.Error as error:
            self.conn.rollback()
            raise Exception(f"Error inserting meeting, because: {error.pgerror}")

    def exists(self, uuid: str) -> bool:
        logger.info(f"About to check if uuid exists: {uuid}")
        exists_query = "SELECT 1 FROM meetings WHERE uuid = %s;"
        try:
            with self.conn.cursor() as cursor:
                logger.info(f"About to execute check if uuid exists: {uuid}")
                cursor.execute(exists_query, (uuid,))
                result = cursor.fetchone() is not None
                logger.info(f"{uuid} existence in database: {result}")
                return result
        except psycopg2.Error as error:
            logger.error(f"Error checking existence of uuid {uuid}: {error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def exists_tenant(self, tenant_id: str) -> bool:
        logger.info(f"About to check if tenant_id exists: {tenant_id}")
        exists_query = "SELECT uuid FROM meetings WHERE tenant_id = %s;"
        try:
            with self.conn.cursor() as cursor:
                logger.info(f"About to execute check if tenant_id exists: {tenant_id}")
                cursor.execute(exists_query, (tenant_id,))
                result = cursor.fetchone() is not None
                logger.info(f"{tenant_id} existence in database: {result}")
                return result
        except psycopg2.Error as error:
            logger.error(f"Error checking existence of tenant_id {tenant_id}: {error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def get_meeting_id(self, uuid: str) -> Optional[int]:
        select_query = "SELECT id FROM meetings WHERE uuid = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got meeting id {row[0]} from database")
                    return row[0]
                else:
                    logger.error(f"Error with getting meeting id for {uuid}")
        except Exception as error:
            logger.error("Error fetching id by uuid:", error)
        return None

    def get_meeting_data(self, uuid: str) -> Optional[MeetingDTO]:
        select_query = """
        SELECT uuid, tenant_id, participants_emails, location, subject, start_time, end_time
        FROM meetings
        WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got meeting data {row[0]} from database")
                    return MeetingDTO.from_tuple(row)
                else:
                    logger.error(f"Error with getting meeting data for {uuid}")
        except Exception as error:
            logger.error("Error fetching meeting data by uuid:", error)
            traceback.print_exception(error)
        return None

    def get_all_meetings_by_tenant_id(self, tenant_id: str) -> list[MeetingDTO]:
        select_query = """
        SELECT uuid, tenant_id, participants_emails, location, subject, start_time, end_time
        FROM meetings
        WHERE tenant_id = %s;
        """
        try:
            self.create_table_if_not_exists()
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (tenant_id,))
                rows = cursor.fetchall()
                if rows:
                    logger.info(f"Got {len(rows)} meetings from database")
                    logger.debug(f"Got meetings: {rows}")
                    return [MeetingDTO.from_tuple(row) for row in rows]
                else:
                    logger.error(f"No meetings found for tenant_id: {tenant_id}")
                    return []
        except Exception as error:
            logger.error("Error fetching meeting data by tenant_id:", error)
            traceback.print_exception(error)
            return []

    def update(self, meeting: MeetingDTO):
        update_query = """
        UPDATE meetings
        SET tenant_id = %s, participants_emails = %s, location = %s, subject = %s, start_time = %s, end_time = %s
        WHERE uuid = %s;
        """
        meeting_data = meeting.to_tuple()
        meeting_data = meeting_data[1:] + (meeting_data[0],)  # move uuid to the end
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, meeting_data)
                self.conn.commit()
                logger.info(f"Updated meeting with uuid: {meeting.uuid}")
        except psycopg2.Error as error:
            self.conn.rollback()
            raise Exception(f"Error updating meeting, because: {error.pgerror}")

    def save_meeting(self, meeting: MeetingDTO):
        self.create_table_if_not_exists()
        if self.exists(meeting.uuid):
            self.update(meeting)
        else:
            self.insert_meeting(meeting)
