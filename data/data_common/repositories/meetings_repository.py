import traceback
from typing import Optional, List
import psycopg2
import json
import hashlib

from common.genie_logger import GenieLogger

logger = GenieLogger()

from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO, AgendaItem


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
            participants_hash VARCHAR,
            link VARCHAR,
            subject VARCHAR,
            location VARCHAR,
            start_time VARCHAR,
            end_time VARCHAR,
            agenda JSONB
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f"Error: {error}")
            traceback.print_exc()
            # self.conn.rollback()

    def insert_meeting(self, meeting: MeetingDTO) -> Optional[str]:
        logger.debug(f"Meeting to insert: {meeting}")
        if self.exists(meeting.google_calendar_id):
            logger.info(f"Meeting with google_calendar_id {meeting.google_calendar_id} already exists")
            return None
        insert_query = """
        INSERT INTO meetings (uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """

        # Convert the participants_emails to JSON string
        meeting_data = (
            meeting.uuid,
            meeting.google_calendar_id,
            meeting.tenant_id,
            json.dumps(meeting.participants_emails),
            hash_participants(meeting.participants_emails),
            meeting.link,
            meeting.subject,
            meeting.location,
            meeting.start_time,
            meeting.end_time,
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
            # self.conn.rollback()
            logger.error(f"Error inserting meeting: {error.pgerror}")
            traceback.print_exc()
            raise Exception(f"Error inserting meeting, because: {error}")
        except Exception as e:
            # self.conn.rollback()
            logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
            raise Exception(f"Unexpected error: {e}")

    def save_agenda(self, uuid: str, agenda_list: List[AgendaItem]):
        if not agenda_list:
            logger.error(f"Invalid agenda data: {agenda_list}, skip saving agenda")
            return None
        agenda_dicts = [
            agenda.to_dict() if isinstance(agenda, AgendaItem) else agenda for agenda in agenda_list
        ]
        update_query = """
        UPDATE meetings
        SET agenda = %s
        WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (json.dumps(agenda_dicts), uuid))
                self.conn.commit()
                logger.info(f"Updated agenda in database for meeting uuid {uuid}")
        except psycopg2.Error as error:
            raise Exception(f"Error updating agenda, because: {error.pgerror}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def exists(self, google_calendar_id: str) -> bool:
        exists_query = "SELECT 1 FROM meetings WHERE google_calendar_id = %s;"
        try:
            with self.conn.cursor() as cursor:
                logger.info(f"About to execute check if uuid exists: {google_calendar_id}")
                cursor.execute(exists_query, (google_calendar_id,))
                result = cursor.fetchone() is not None
                logger.info(f"{google_calendar_id} existence in database: {result}")
                return result
        except psycopg2.Error as error:
            logger.error(f"Error checking existence of uuid {google_calendar_id}: {error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
            return False

    def exists_without_changes(self, meeting: MeetingDTO) -> bool:
        participants_hash = hash_participants(meeting.participants_emails)
        exists_query = """
        SELECT 1 FROM meetings
        WHERE google_calendar_id = %s AND participants_hash = %s AND start_time = %s AND link = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                logger.info(
                    f"About to execute check if meeting exists without changes: {meeting.google_calendar_id}({meeting.subject})"
                )
                cursor.execute(
                    exists_query,
                    (
                        meeting.google_calendar_id,
                        participants_hash,
                        meeting.start_time,
                        meeting.link,
                    ),
                )
                result = cursor.fetchone() is not None
                logger.info(f"{meeting.google_calendar_id} existence in database without changes: {result}")
                return result
        except psycopg2.Error as error:
            logger.error(
                f"Error checking existence of meeting without changes for google_calendar_id {meeting.google_calendar_id}: {error}"
            )
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
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
            traceback.print_exc()
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
                    logger.error(f"Could not find meeting id for {uuid}")
        except Exception as error:
            logger.error("Error fetching id by uuid:", error)
            traceback.print_exception(error)
        return None

    def get_meeting_data(self, uuid: str) -> Optional[MeetingDTO]:
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda
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
                    logger.error(f"Meeting not found for {uuid}")
                    traceback.print_exc()
        except Exception as error:
            logger.error("Error fetching meeting data by uuid:", error)
            traceback.print_exception(error)
            traceback.print_exc()
        return None

    def get_meeting_by_google_calendar_id(self, google_calendar_id: str) -> Optional[MeetingDTO]:
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda
        FROM meetings
        WHERE google_calendar_id = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (google_calendar_id,))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got meeting data {row[0]} from database")
                    return MeetingDTO.from_tuple(row)
                else:
                    logger.error(f"Meeting not found for {google_calendar_id}")
                    traceback.print_exc()
        except Exception as error:
            logger.error("Error fetching meeting data by google_calendar_id:", error)
            traceback.print_exception(error)
            traceback.print_exc()
        return None

    def get_all_meetings_by_tenant_id(self, tenant_id: str) -> list[MeetingDTO]:
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda
        FROM meetings
        WHERE tenant_id = %s;
        """
        try:
            self.create_table_if_not_exists()
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (tenant_id,))
                meetings = cursor.fetchall()
                if meetings:
                    logger.info(f"Got {len(meetings)} meetings from database")
                    return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
                else:
                    logger.error(f"No meetings found for tenant_id: {tenant_id}")
                    return []
        except Exception as error:
            logger.error("Error fetching meeting data by tenant_id:", error)
            traceback.print_exception(error)
            return []

    def get_meetings_by_participants_emails(self, emails: list[str]) -> list[MeetingDTO]:
        """
        Get a list of meetings that have participants with the given emails.

        :param emails: List of emails to search for in participants_emails.
        :return: List of meetings with participants having the given emails.
        """
        if not emails:
            return []
        query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda
        FROM meetings
        WHERE participants_emails ?| array[%s]
        """
        formatted_emails = ",".join(emails)
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (formatted_emails,))
                meetings = cursor.fetchall()
                logger.info(f"Retrieved meetings for participants emails: {emails}")
                return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
        except psycopg2.Error as error:
            logger.error(f"Error fetching meetings by participants emails: {error.pgerror}")
            traceback.print_exc()
            return []
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
            return []

    def update(self, meeting: MeetingDTO):
        update_query = """
        UPDATE meetings
        SET tenant_id = %s, participants_emails = %s, participants_hash = %s, link = %s, subject = %s, location = %s, start_time = %s, end_time = %s
        WHERE google_calendar_id = %s;
        """
        meeting_data = meeting.to_tuple()
        meeting_data = meeting_data[:3] + (json.dumps(meeting_data[3]),) + meeting_data[4:]

        meeting_data = meeting_data[2:] + (meeting_data[1],)  # move uuid to the end
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, meeting_data)
                self.conn.commit()
                logger.info(f"Updated meeting with uuid: {meeting.uuid}")
        except psycopg2.Error as error:
            # self.conn.rollback()
            logger.error(f"Error updating meeting: {error.pgerror}")
            traceback.print_exc()
            raise Exception(f"Error updating meeting, because: {error.pgerror}")

    def save_meeting(self, meeting: MeetingDTO):
        self.create_table_if_not_exists()
        if self.exists(meeting.google_calendar_id):
            if self.exists_without_changes(meeting):
                logger.info(
                    f"Meeting with google_calendar_id {meeting.google_calendar_id} "
                    f"already exists - and no changes were made. Skipping..."
                )
                return
            self.update(meeting)
            logger.info(f"Meeting with uuid {meeting.uuid} already exists. Updated meeting.")
        else:
            self.insert_meeting(meeting)

    def delete(self, uuid: str):
        delete_query = "DELETE FROM meetings WHERE uuid = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_query, (uuid,))
                self.conn.commit()
                logger.info(f"Deleted meeting with uuid: {uuid}")
        except psycopg2.Error as error:
            # self.conn.rollback()
            logger.error(f"Error deleting meeting: {error.pgerror}")
            traceback.print_exc()
            raise Exception(f"Error deleting meeting, because: {error.pgerror}")


def hash_participants(participants_emails: list[str]) -> str:
    emails_string = json.dumps(participants_emails, sort_keys=True)
    return hashlib.sha256(emails_string.encode("utf-8")).hexdigest()
