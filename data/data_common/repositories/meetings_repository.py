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
            goals JSONB,
            agenda JSONB,
            classification VARCHAR
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f"Error: {error}")
            traceback.print_exc()

    def insert_meeting(self, meeting: MeetingDTO) -> Optional[str]:
        logger.debug(f"Meeting to insert: {meeting}")
        if self.exists(meeting.google_calendar_id):
            logger.info(f"Meeting with google_calendar_id {meeting.google_calendar_id} already exists")
            return None
        insert_query = """
        INSERT INTO meetings (uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, classification)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """

        meeting_data = (
            meeting.uuid,
            meeting.google_calendar_id,
            meeting.tenant_id,
            json.dumps(meeting.participants_emails),
            meeting.participants_hash,
            meeting.link,
            meeting.subject,
            meeting.location,
            meeting.start_time,
            meeting.end_time,
            meeting.classification.value,  # Convert enum to string for DB
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
            logger.error(f"Error inserting meeting: {error.pgerror}")
            traceback.print_exc()
            raise Exception(f"Error inserting meeting, because: {error}")

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

    def save_meeting_goals(self, uuid: str, goals: List[str]):
        if not goals:
            logger.error(f"Invalid goals data: {goals}, skip saving goals")
            return None
        goals_json = json.dumps(goals)
        update_query = """
        UPDATE meetings
        SET goals = %s
        WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (goals_json, uuid))
                self.conn.commit()
                logger.info(f"Updated goals in database for meeting uuid {uuid}")
        except psycopg2.Error as error:
            raise Exception(f"Error updating goals, because: {error.pgerror}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return

    def exists(self, google_calendar_id: str) -> bool:
        exists_query = "SELECT 1 FROM meetings WHERE google_calendar_id = %s AND classification != 'deleted';"
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
        agenda = meeting.agenda
        if agenda:
            agenda = [
                agenda_item.to_dict() if isinstance(agenda_item, AgendaItem) else agenda_item
                for agenda_item in agenda
            ]
        exists_query = """
        SELECT 1 FROM meetings
        WHERE google_calendar_id = %s AND participants_hash = %s AND start_time = %s AND link = %s AND agenda = %s
        AND classification = %s AND classification != 'deleted';
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
                        json.dumps(agenda),
                        meeting.classification.value,
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

    def get_meeting_data(self, uuid: str) -> Optional[MeetingDTO]:
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE uuid = %s AND classification != 'deleted';
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
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE google_calendar_id = %s AND classification != 'deleted';
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (google_calendar_id,))
                row = cursor.fetchone()
                if row:
                    logger.debug(f"Got meeting data {row[0]} from database")
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
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE tenant_id = %s AND classification != 'deleted';
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
        if not emails:
            return []
        query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE participants_emails ?| array[%s] AND classification != 'deleted';
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

    def get_meetings_without_goals_by_email(self, email: str) -> list[MeetingDTO]:
        query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE (participants_emails @> %s::jsonb AND (goals IS NULL OR goals = '[]' or agenda = 'null')) AND classification = 'external' AND classification != 'deleted';
        """
        email_json = json.dumps([{"email": email}])
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (email_json,))
                meetings = cursor.fetchall()
                logger.info(f"Retrieved {len(meetings)} meetings for email: {email} with no agenda")
                return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
        except psycopg2.Error as error:
            logger.error(f"Error fetching meetings by email with no agenda: {error.pgerror}")
            traceback.print_exc()
            return []
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
            return []

    def get_meetings_without_goals_by_company_domain(self, domain: str) -> list[MeetingDTO]:
        query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE
            (EXISTS (
                SELECT 1
                FROM jsonb_array_elements(participants_emails) AS participants
                WHERE participants->>'email' ILIKE %s
            )
            AND (goals IS NULL OR goals = '[]' or agenda = 'null')) AND classification = 'external' AND classification != 'deleted';
        """
        email_pattern = f"%@{domain}"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (email_pattern,))
                meetings = cursor.fetchall()
                logger.info(
                    f"Retrieved {len(meetings)} meetings with emails from domain: {domain} and no agenda"
                )
                return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
        except psycopg2.Error as error:
            logger.error(f"Error fetching meetings by domain with no agenda: {error.pgerror}")
            traceback.print_exc()
            return []
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
            return []

    def get_meetings_with_goals_without_agenda_by_email(self, email):
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE (participants_emails @> %s::jsonb AND (agenda IS NULL OR agenda = '[]' OR agenda != 'null')) AND (goals IS NOT NULL OR goals != '[]' OR goals = 'null') AND classification = 'external' AND classification != 'deleted';
        """
        email_json = json.dumps([{"email": email}])
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email_json,))
                meetings = cursor.fetchall()
                logger.info(f"Got {len(meetings)} meetings without agenda for email: {email}")
                return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
        except psycopg2.Error as error:
            logger.error(f"Error fetching meetings without agenda by email: {error.pgerror}")
            traceback.print_exc()
            return []
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
            return []

    def get_meeting_goals(self, uuid: str) -> Optional[List[str]]:
        select_query = "SELECT goals FROM meetings WHERE uuid = %s AND classification != 'deleted';"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (uuid,))
                row = cursor.fetchone()
                if row:
                    logger.info(f"Got meeting goals {row[0]} from database")
                    return row[0]
                else:
                    logger.error(f"Could not find meeting goals for {uuid}")
        except Exception as error:
            logger.error("Error fetching goals by uuid:", error)
            traceback.print_exception(error)
        return None

    def get_all_external_meetings_without_agenda(self):
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location,
         start_time, end_time, agenda, classification
        FROM meetings
        WHERE (agenda IS NULL OR agenda = '[]' or agenda = 'null') AND classification = 'external' AND classification != 'deleted'
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query)
                meetings = cursor.fetchall()
                logger.info(f"Got {len(meetings)} external meetings without agenda from database")
                for meeting in meetings:
                    logger.debug(f"Meeting: {meeting}")
                return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
        except Exception as error:
            logger.error("Error fetching meetings without agenda:", error)
            traceback.print_exception(error)
            return []

    def get_all_meetings_without_classification(self):
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE classification IS NULL AND classification != 'deleted';
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query)
                meetings = cursor.fetchall()
                logger.info(f"Got {len(meetings)} meetings without classification from database")
                return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
        except Exception as error:
            logger.error("Error fetching meetings without classification:", error)
            traceback.print_exception(error)
            return []

    def get_all_meetings(self):
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings WHERE classification != 'deleted';
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query)
                meetings = cursor.fetchall()
                logger.info(f"Got {len(meetings)} meetings from database")
                return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
        except Exception as error:
            logger.error("Error fetching all meetings:", error)
            traceback.print_exception(error)
            return []

    def update_tenant_id(self, new_tenant_id, old_tenant_id):
        update_query = "UPDATE meetings SET tenant_id = %s WHERE tenant_id = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, (new_tenant_id, old_tenant_id))
                self.conn.commit()
                return True
        except psycopg2.Error as error:
            logger.error(f"Error updating tenant_id: {error.pgerror}")
            traceback.print_exc()
            return False

    def update(self, meeting: MeetingDTO):
        update_query = """
        UPDATE meetings
        SET tenant_id = %s, participants_emails = %s, participants_hash = %s, link = %s, subject = %s, location = %s,
        start_time = %s, end_time = %s, agenda = %s, classification = %s
        WHERE google_calendar_id = %s AND classification != 'deleted';
        """

        agenda = meeting.agenda
        agenda_dicts = [agenda_item.to_dict() for agenda_item in agenda] if agenda else None

        meeting_data = (
            meeting.tenant_id,
            json.dumps(meeting.participants_emails),
            meeting.participants_hash,
            meeting.link,
            meeting.subject,
            meeting.location,
            meeting.start_time,
            meeting.end_time,
            json.dumps(agenda_dicts),
            meeting.classification.value,
            meeting.google_calendar_id,
        )
        logger.debug(f"About to update meeting data: {meeting_data}")
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(update_query, meeting_data)
                self.conn.commit()
                logger.info(f"Updated meeting with uuid: {meeting.uuid}")
        except psycopg2.Error as error:
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
        delete_query = "UPDATE meetings SET classification = 'deleted' WHERE uuid = %s;"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(delete_query, (uuid,))
                self.conn.commit()
                logger.info(f"Deleted meeting with uuid: {uuid}")
        except psycopg2.Error as error:
            logger.error(f"Error deleting meeting: {error.pgerror}")
            traceback.print_exc()
            raise Exception(f"Error deleting meeting, because: {error.pgerror}")


def hash_participants(participants_emails: list[str]) -> str:
    emails_string = json.dumps(participants_emails, sort_keys=True)
    return hashlib.sha256(emails_string.encode("utf-8")).hexdigest()
