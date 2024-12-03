import traceback
from datetime import timedelta, datetime, timezone
from typing import Optional, List
import psycopg2
import json
import hashlib

from common.genie_logger import GenieLogger

logger = GenieLogger()
from data.data_common.utils.postgres_connector import db_connection
from data.data_common.data_transfer_objects.meeting_dto import MeetingDTO, AgendaItem, MeetingClassification


class MeetingsRepository:
    def __init__(self):
        self.create_table_if_not_exists()

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
            classification VARCHAR,
            reminder_sent BOOLEAN DEFAULT FALSE,
            reminder_schedule TIMESTAMPTZ DEFAULT NULL
        );
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                logger.error(f"Error: {error}")
                traceback.print_exc()

    def insert_meeting(self, meeting: MeetingDTO) -> Optional[str]:
        logger.debug(f"Meeting to insert: {meeting}")
        if self.exists(meeting.google_calendar_id, meeting.tenant_id):
            logger.info(f"Meeting with google_calendar_id {meeting.google_calendar_id} already exists")
            return None
        insert_query = """
        INSERT INTO meetings (uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link,
         subject, location, start_time, end_time, classification, reminder_schedule, fake)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            meeting.calculate_reminder_schedule(meeting.start_time) if meeting.classification == MeetingClassification.EXTERNAL else None,
            meeting.fake,
        )
        logger.info(f"About to insert meeting data: {meeting_data}")
        with db_connection() as conn:

            try:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, meeting_data)
                    conn.commit()
                    meeting_id = cursor.fetchone()[0]
                    logger.info(f"Inserted meeting to database. Meeting id: {meeting_id}")
                    return meeting_id
            except psycopg2.Error as error:
                logger.error(f"Error inserting meeting: {error.pgerror}")
                traceback.print_exc()
                raise Exception(f"Error inserting meeting, because: {error}")

    def get_meeting_data(self, uuid: str) -> Optional[MeetingDTO]:
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE uuid = %s AND classification != %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid, MeetingClassification.DELETED.value))
                    row = cursor.fetchone()
                    if row:
                        logger.info(f"Got meeting data {row[0]} from database")
                        return MeetingDTO.from_tuple(row)
                    else:
                        logger.warning(f"Meeting not found for {uuid}")
            except Exception as error:
                logger.error("Error fetching meeting data by uuid:", error)
                traceback.print_exc()
            return None

    def get_meeting_by_google_calendar_id(self, google_calendar_id: str, tenant_id: str) -> Optional[MeetingDTO]:
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE google_calendar_id = %s AND tenant_id = %s AND classification != %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (google_calendar_id, tenant_id, MeetingClassification.DELETED.value))
                    row = cursor.fetchone()
                    if row:
                        logger.debug(f"Got meeting data {row[0]} from database")
                        return MeetingDTO.from_tuple(row)
                    else:
                        logger.warning(f"Meeting not found for {google_calendar_id}")
            except Exception as error:
                logger.error("Error fetching meeting data by google_calendar_id:", error)
                traceback.print_exception(error)
                traceback.print_exc()
            return None

    def get_all_meetings_by_tenant_id(self, tenant_id: str) -> list[MeetingDTO]:
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE tenant_id = %s AND classification != %s;
        """
        with db_connection() as conn:
            try:
                self.create_table_if_not_exists()
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (tenant_id, MeetingClassification.DELETED.value))
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

    def get_all_meetings_by_tenant_id_in_datetime(self, tenant_id: str, selected_datetime: datetime) -> list[MeetingDTO]:
        start_range = (selected_datetime - timedelta(weeks=2)).isoformat()
        end_range = (selected_datetime + timedelta(weeks=2)).isoformat()

        # SQL query to filter within the date range
        select_query = """
            SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
            FROM meetings
            WHERE tenant_id = %s 
              AND classification != %s
              AND to_timestamp(start_time, 'YYYY-MM-DD"T"HH24:MI:SS') AT TIME ZONE 'UTC' BETWEEN %s AND %s;
            """
        with db_connection() as conn:
            try:
                self.create_table_if_not_exists()
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (tenant_id, MeetingClassification.DELETED.value, start_range, end_range))
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

    def get_all_future_meetings_for_tenant(self, tenant_id):
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE tenant_id = %s
        AND (
            CASE
                WHEN start_time ~ 'T' THEN TO_TIMESTAMP(SPLIT_PART(start_time, '+', 1), 'YYYY-MM-DD"T"HH24:MI:SS')
                ELSE TO_TIMESTAMP(start_time, 'YYYY-MM-DD')
            END > %s
        )
        AND classification != %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        select_query, (tenant_id, datetime.now(timezone.utc), MeetingClassification.DELETED.value)
                    )
                    meetings = cursor.fetchall()
                    logger.info(f"Got {len(meetings)} future meetings for tenant {tenant_id}")
                    return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
            except Exception as error:
                logger.error("Error fetching future meetings for tenant:", exc_info=True)
                return []

    def get_meetings_without_goals_by_email(self, email: str) -> list[MeetingDTO]:
        query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE (participants_emails @> %s::jsonb AND (goals IS NULL OR goals = '[]' or agenda = 'null')) AND classification = %s;
        """
        email_json = json.dumps([{"email": email}])
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (email_json, MeetingClassification.EXTERNAL.value))
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
        WHERE EXISTS (
                SELECT 1
                FROM jsonb_array_elements(participants_emails) AS participants
                WHERE participants->>'email' ILIKE %s
            )
            AND (goals IS NULL OR goals = '[]' or agenda = 'null') AND classification = %s;
        """
        email_pattern = f"%@{domain}"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, (email_pattern, MeetingClassification.EXTERNAL.value))
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

    def get_meeting_goals(self, uuid: str) -> Optional[List[str]]:
        select_query = "SELECT goals FROM meetings WHERE uuid = %s AND classification != %s;"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid, MeetingClassification.DELETED.value))
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
        WHERE (agenda IS NULL OR agenda = '[]' or agenda = 'null') AND classification = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (MeetingClassification.EXTERNAL.value,))
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
        WHERE classification IS NULL or classification = 'null';
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
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
        FROM meetings WHERE classification != %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (MeetingClassification.DELETED.value,))
                    meetings = cursor.fetchall()
                    logger.info(f"Got {len(meetings)} meetings from database")
                    return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
            except Exception as error:
                logger.error("Error fetching all meetings:", error)
                traceback.print_exception(error)
                return []

    def delete(self, uuid: str):
        delete_query = "UPDATE meetings SET classification = %s WHERE uuid = %s;"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(delete_query, (MeetingClassification.DELETED.value, uuid))
                    conn.commit()
                    logger.info(f"Deleted meeting with uuid: {uuid}")
            except psycopg2.Error as error:
                logger.error(f"Error deleting meeting: {error.pgerror}")
                traceback.print_exc()
                raise Exception(f"Error deleting meeting, because: {error.pgerror}")

    def update(self, meeting: MeetingDTO):
        update_query = """
        UPDATE meetings
        SET participants_emails = %s, participants_hash = %s, link = %s, subject = %s, location = %s,
        start_time = %s, end_time = %s, agenda = %s, classification = %s, reminder_schedule = %s
        WHERE google_calendar_id = %s AND tenant_id = %s;
        """

        agenda = meeting.agenda
        agenda_dicts = [agenda_item.to_dict() for agenda_item in agenda] if agenda else None

        reminder_schedule = meeting.calculate_reminder_schedule(meeting.start_time) if meeting.classification == MeetingClassification.EXTERNAL else None

        logger.info(f"Reminder schedule: {reminder_schedule}")

        meeting_data = (
            json.dumps(meeting.participants_emails),
            meeting.participants_hash,
            meeting.link,
            meeting.subject,
            meeting.location,
            meeting.start_time,
            meeting.end_time,
            json.dumps(agenda_dicts),
            meeting.classification.value,
            reminder_schedule,
            meeting.google_calendar_id,
            meeting.tenant_id,
        )
        logger.debug(f"About to update meeting data: {meeting_data}")
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, meeting_data)
                    conn.commit()
                    logger.info(f"Updated meeting with uuid: {meeting.uuid}")
            except psycopg2.Error as error:
                logger.error(f"Error updating meeting: {error.pgerror}")
                traceback.print_exc()
                raise Exception(f"Error updating meeting, because: {error.pgerror}")

    def save_meeting(self, meeting: MeetingDTO):
        if self.exists(meeting.google_calendar_id, meeting.tenant_id):
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
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (json.dumps(agenda_dicts), uuid))
                    conn.commit()
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
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (goals_json, uuid))
                    conn.commit()
                    logger.info(f"Updated goals in database for meeting uuid {uuid}")
            except psycopg2.Error as error:
                raise Exception(f"Error updating goals, because: {error.pgerror}")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return

    def exists(self, google_calendar_id: str, tenant_id=None) -> bool:
        exists_query = f"SELECT 1 FROM meetings WHERE google_calendar_id = %s{"AND tenant_id = %s" if tenant_id else ''};"
        args = (google_calendar_id,) if not tenant_id else (google_calendar_id, tenant_id)
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    logger.info(f"About to execute check if uuid exists: {google_calendar_id}")
                    cursor.execute(exists_query, args)
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
        AND classification != %s AND reminder_schedule = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
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
                            MeetingClassification.DELETED.value,
                            meeting.calculate_reminder_schedule(meeting.start_time) if meeting.classification == MeetingClassification.EXTERNAL else None,
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

    def get_all_meetings_by_tenant_id_that_should_be_imported(
        self, number_of_imported_meetings: int, tenant_id: str
    ) -> list[MeetingDTO]:
        ten_hours_ago = datetime.now(timezone.utc) - timedelta(hours=10)
        cutoff_time = ten_hours_ago.isoformat()

        select_query = f"""
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE classification NOT IN (%s)
        AND fake = FALSE
        AND start_time > %s
        AND tenant_id = %s
        ORDER BY start_time ASC
        {"LIMIT %s" if number_of_imported_meetings > 0 else ""};
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    if number_of_imported_meetings > 0:
                        cursor.execute(
                            select_query,
                            (MeetingClassification.DELETED.value, cutoff_time, tenant_id, number_of_imported_meetings),
                        )
                    else:
                        cursor.execute(select_query, (MeetingClassification.DELETED.value, cutoff_time, tenant_id))

                    meetings = cursor.fetchall()
                    logger.info(f"Got {len(meetings)} meetings from database")
                    return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
            except Exception as error:
                logger.error("Error fetching all meetings:", exc_info=True)
                return []

    def hard_delete(self, google_calendar_id: str):
        delete_query = "DELETE FROM meetings WHERE google_calendar_id = %s;"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(delete_query, (google_calendar_id,))
                    conn.commit()
                    logger.info(f"Hard deleted meeting with google_calendar_id: {google_calendar_id}")
            except psycopg2.Error as error:
                logger.error(f"Error hard deleting meeting: {error.pgerror}")
                traceback.print_exc()
                raise Exception(f"Error hard deleting meeting, because: {error.pgerror}")

    def update_tenant_id(self, new_tenant_id, old_tenant_id):
        update_query = "UPDATE meetings SET tenant_id = %s WHERE tenant_id = %s;"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (new_tenant_id, old_tenant_id))
                    conn.commit()
                    return True
            except psycopg2.Error as error:
                logger.error(f"Error updating tenant_id: {error.pgerror}")
                traceback.print_exc()
                return False

    def get_meetings_with_missing_classification(self) -> list[MeetingDTO]:
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE classification IS NULL or classification = 'null';
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query)
                    meetings = cursor.fetchall()
                    logger.info(f"Got {len(meetings)} meetings with missing classification from database")
                    return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
            except Exception as error:
                logger.error("Error fetching meetings with missing classification:", error)
                traceback.print_exception(error)
                return []

    def get_all_future_external_meetings_for_tenant(self, tenant_id):
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE tenant_id = %s
        AND (
        CASE
            WHEN start_time ~ 'T' THEN TO_TIMESTAMP(SPLIT_PART(start_time, '+', 1), 'YYYY-MM-DD"T"HH24:MI:SS')
            ELSE TO_TIMESTAMP(start_time, 'YYYY-MM-DD')
        END > %s)
        AND classification = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        select_query,
                        (tenant_id, datetime.now(timezone.utc).isoformat(), MeetingClassification.EXTERNAL.value),
                    )
                    meetings = cursor.fetchall()
                    logger.info(f"Got {len(meetings)} future meetings for tenant {tenant_id}")
                    return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
            except Exception as error:
                logger.error("Error fetching future meetings for tenant:", error)
                traceback.print_exception(error)
                return []

    def get_meetings_with_goals_without_agenda_by_email(self, email):
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE (participants_emails @> %s::jsonb AND (agenda IS NULL OR agenda = '[]' OR agenda != 'null')) AND (goals IS NOT NULL OR goals != '[]' OR goals = 'null') AND classification = 'external';
        """
        email_json = json.dumps([{"email": email}])
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
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

    def has_sent_meeting_reminder(self, meeting_uuid: str) -> bool:
        select_query = "SELECT reminder_sent FROM meetings WHERE uuid = %s;"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (meeting_uuid,))
                    reminder_sent = cursor.fetchone()
                    logger.info(f"Got reminder_sent for meeting {meeting_uuid}")
                    if reminder_sent:
                        return reminder_sent[0]
                    return False
            except psycopg2.Error as error:
                logger.error(f"Error fetching reminder_sent for meeting: {error.pgerror}")
                traceback.print_exc()
                return False
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                traceback.print_exc()
                return False

    def get_meetings_by_participants_emails(self, emails: list[str]) -> list[MeetingDTO]:
        if not emails:
            return []
        query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, subject, location, start_time, end_time, agenda, classification
        FROM meetings
        WHERE participants_emails ?| array[%s] AND classification != 'deleted';
        """
        formatted_emails = ",".join(emails)
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
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

    def get_meetings_to_send_reminders(self):
        """
        This will return all external meetings that are scheduled to start in the next 30 minutes,
        and have not yet had a reminder sent, for tenants with reminder_subscription = TRUE.
        """
        select_query = """
            SELECT m.uuid, m.google_calendar_id, m.tenant_id, m.participants_emails, m.participants_hash, m.link, 
                   m.subject, m.location, m.start_time, m.end_time, m.agenda, m.classification, m.reminder_schedule
            FROM meetings m
            INNER JOIN tenants t ON m.tenant_id = t.tenant_id
            WHERE (m.reminder_schedule AT TIME ZONE 'UTC') BETWEEN (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '25 minutes') 
                  AND (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' + INTERVAL '5 minutes')
              AND m.classification = %s 
              AND m.reminder_sent IS NULL
              AND t.reminder_subscription = TRUE;
        """
        current_utc_time = datetime.now(timezone.utc)
        logger.info(f"Fetching meetings to send reminders for at current UTC time: {current_utc_time}")

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (MeetingClassification.EXTERNAL.value,))
                    meetings = cursor.fetchall()

                    # Log detailed info on each meeting for debugging purposes
                    if meetings:
                        logger.info(f"Got {len(meetings)} meetings to send reminders for")
                        for meeting in meetings:
                            meeting_uuid = meeting[0]
                            reminder_schedule = meeting[-1]  # Assuming last field is `reminder_schedule`
                            start_time = meeting[8]  # Assuming `start_time` is at index 8

                            logger.debug(f"Meeting ID: {meeting_uuid}, Start Time: {start_time}, Reminder Schedule: {reminder_schedule}")

                        return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
                    else:
                        logger.info("No meetings found to send reminders for the current time range.")
                        return []

            except psycopg2.Error as db_error:
                logger.error(f"Database error fetching meetings to send reminders for: {db_error.pgerror}")
                logger.debug(traceback.format_exc())  # Logs the traceback for detailed error analysis
                return []
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                logger.debug(traceback.format_exc())  # Logs the traceback for general exceptions
                return []

    def get_next_meeting(self):
        select_query = """
        SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, 
               subject, location, start_time,
               end_time, agenda, classification, (start_time::timestamp AT TIME ZONE 'UTC') as start_time_utc, 
               (reminder_schedule::timestamp AT TIME ZONE 'UTC') as reminder_schedule_utc
        FROM meetings
        WHERE (start_time::timestamp AT TIME ZONE 'UTC') > (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
          AND classification = %s
        ORDER BY start_time
        LIMIT 1;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (MeetingClassification.EXTERNAL.value,))
                    meeting = cursor.fetchone()

                    if meeting:
                        logger.info(f"Got next meeting: {meeting[0]}")

                        # Parse meeting data correctly
                        meeting_dto = MeetingDTO.from_tuple(meeting[:-2])  # All fields except parsed `start_time_utc` and `reminder_schedule_utc`
                        start_time_utc = meeting[-2]  # Parsed start_time in UTC
                        reminder_schedule_utc = meeting[-1]  # Parsed reminder_schedule in UTC

                        return meeting_dto, start_time_utc, reminder_schedule_utc
                    else:
                        logger.info("No upcoming meeting found")
                        return None, None, None
            except psycopg2.Error as db_error:
                logger.error(f"Database error fetching next meeting: {db_error.pgerror}")
                logger.debug(traceback.format_exc())



    def get_all_meetings_without_reminders(self):
        select_query = """
            SELECT uuid, google_calendar_id, tenant_id, participants_emails, participants_hash, link, 
                   subject, location, start_time, end_time, agenda, classification
            FROM meetings
            WHERE reminder_schedule IS NULL AND classification = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (MeetingClassification.EXTERNAL.value,))
                    meetings = cursor.fetchall()
                    logger.info(f"Got {len(meetings)} meetings without reminders")
                    return [MeetingDTO.from_tuple(meeting) for meeting in meetings]
            except psycopg2.Error as db_error:
                logger.error(f"Database error fetching meetings without reminders: {db_error.pgerror}")
                logger.debug(traceback.format_exc())
                return []
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                logger.debug(traceback.format_exc())
                return []


    def update_senders_meeting_reminder(self, meeting_uuid):
        update_query = "UPDATE meetings SET reminder_sent = CURRENT_TIMESTAMP WHERE uuid = %s;"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(update_query, (meeting_uuid,))
                    conn.commit()
                    logger.info(f"Updated reminder_sent for meeting {meeting_uuid}")
            except psycopg2.Error as error:
                logger.error(f"Error updating reminder_sent for meeting: {error.pgerror}")
                traceback.print_exc()
                return False
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                traceback.print_exc()
                return False


def hash_participants(participants_emails: list[str] | list[dict]) -> str:
    emails_string = json.dumps(participants_emails, sort_keys=True)
    return hashlib.sha256(emails_string.encode("utf-8")).hexdigest()
