import traceback
from typing import Optional
import psycopg2
from loguru import logger
import json

from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO


class MeetingsRepository:
    def __init__(self, conn):
        self.conn = conn
        self.create_table_if_not_exists()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS meetings (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            google_calendar_id VARCHAR,
            tenant_id VARCHAR,
            participants_emails JSONB,
            link VARCHAR,
            subject VARCHAR,
            start_time VARCHAR,
            end_time VarCHAR
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
        logger.debug(f"Meeting to insert: {meeting}")
        if self.exists(meeting.google_calendar_id):
            logger.info(
                f"Meeting with google_calendar_id {meeting.google_calendar_id} already exists"
            )
            return None
        insert_query = """
        INSERT INTO meetings (uuid, google_calendar_id, tenant_id, participants_emails, link, subject, start_time, end_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        logger.info(f"About to insert meeting: {meeting}")

        # Convert the participants_emails to JSON string
        meeting_data = meeting.to_tuple()
        meeting_data = (
            meeting_data[:3] + (json.dumps(meeting_data[3]),) + meeting_data[4:]
        )

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
            raise Exception(f"Error inserting meeting, because: {error}")

    def exists(self, google_calendar_id: str) -> bool:
        logger.info(f"About to check if uuid exists: {google_calendar_id}")
        exists_query = "SELECT 1 FROM meetings WHERE google_calendar_id = %s;"
        try:
            with self.conn.cursor() as cursor:
                logger.info(
                    f"About to execute check if uuid exists: {google_calendar_id}"
                )
                cursor.execute(exists_query, (google_calendar_id,))
                result = cursor.fetchone() is not None
                logger.info(f"{google_calendar_id} existence in database: {result}")
                return result
        except psycopg2.Error as error:
            logger.error(
                f"Error checking existence of uuid {google_calendar_id}: {error}"
            )
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
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, link, subject, start_time, end_time
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
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, link, subject, start_time, end_time
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

    def get_meetings_by_participants_emails(self, emails: list[str]) -> list[dict]:
        """
        Get a list of meetings that have participants with the given emails.

        :param emails: List of emails to search for in participants_emails.
        :return: List of meetings with participants having the given emails.
        """
        if not emails:
            return []

        query = """
        SELECT * FROM meetings
        WHERE participants_emails ?| array[%s]
        """

        formatted_emails = ",".join(emails)

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (formatted_emails,))
                meetings = cursor.fetchall()
                logger.info(f"Retrieved meetings for participants emails: {emails}")
                return [MeetingDTO.from_tuple(meeting[1:]) for meeting in meetings]
        except psycopg2.Error as error:
            logger.error(
                f"Error fetching meetings by participants emails: {error.pgerror}"
            )
            traceback.print_exc()
            return []
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
            return []

    def update(self, meeting: MeetingDTO):
        update_query = """
        UPDATE meetings
        SET tenant_id = %s, participants_emails = %s, link = %s, subject = %s, start_time = %s, end_time = %s
        WHERE google_calendar_id = %s;
        """
        meeting_data = meeting.to_tuple()
        meeting_data = (
            meeting_data[:3] + (json.dumps(meeting_data[3]),) + meeting_data[4:]
        )
        meeting_data = meeting_data[2:] + (meeting_data[1],)  # move uuid to the end
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
