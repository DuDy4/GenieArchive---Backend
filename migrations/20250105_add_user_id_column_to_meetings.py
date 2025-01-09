from marshal import loads

from common.genie_logger import logger
from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Step 1: Create meetings_temp table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS meetings_temp (
                    id SERIAL PRIMARY KEY,
                    uuid VARCHAR UNIQUE NOT NULL,
                    google_calendar_id VARCHAR,
                    user_id VARCHAR,
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
                    reminder_sent TIMESTAMPTZ DEFAULT NULL,
                    reminder_schedule TIMESTAMPTZ DEFAULT NULL,
                    fake BOOLEAN DEFAULT FALSE

                );
            """)

            # Step 2: Copy rows to meetings_temp with user_id mapping from users table
            cursor.execute("""
                INSERT INTO meetings_temp (
                    uuid, google_calendar_id, user_id, tenant_id, 
                    participants_emails, participants_hash, link, subject, 
                    location, start_time, end_time, goals, agenda, 
                    classification, reminder_sent, reminder_schedule
                )
                SELECT 
                    m.uuid, 
                    m.google_calendar_id, 
                    u.user_id, 
                    m.tenant_id, 
                    m.participants_emails, 
                    m.participants_hash, 
                    m.link, 
                    m.subject, 
                    m.location, 
                    m.start_time, 
                    m.end_time, 
                    m.goals, 
                    m.agenda, 
                    m.classification, 
                    m.reminder_sent,
                    m.reminder_schedule
                FROM 
                    meetings m
                JOIN 
                    users u ON m.tenant_id = u.tenant_id;
            """)

            # Step 3: Validate row counts and ensure no NULL user_id or tenant_id
            cursor.execute("SELECT COUNT(*) FROM meetings;")
            meetings_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM meetings_temp;")
            meetings_temp_count = cursor.fetchone()[0]

            cursor.execute("""SELECT m.uuid
                FROM meetings m
                LEFT JOIN meetings_temp mt ON m.uuid = mt.uuid
                WHERE mt.uuid IS NULL;""")
            missing_uuids = cursor.fetchall()
            logger.info(f"Problematic meetings: {missing_uuids}")

            cursor.execute("""
                SELECT COUNT(*) 
                FROM meetings_temp 
                WHERE user_id IS NULL OR tenant_id IS NULL;
            """)
            null_check = cursor.fetchone()[0]

            if meetings_count != meetings_temp_count or null_check > 0:
                raise Exception("Validation failed: Row counts differ or NULL values exist in user_id/tenant_id.")

            # Step 4: Drop the original meetings table
            cursor.execute("DROP TABLE IF EXISTS meetings;")

            # Step 5: Recreate the meetings table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS meetings (
                    id SERIAL PRIMARY KEY,
                    uuid VARCHAR UNIQUE NOT NULL,
                    google_calendar_id VARCHAR,
                    user_id VARCHAR,
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
                    reminder_sent TIMESTAMPTZ DEFAULT NULL,
                    reminder_schedule TIMESTAMPTZ DEFAULT NULL,
                    fake BOOLEAN DEFAULT FALSE
                );
            """)

            # Step 6: Copy rows back from meetings_temp to meetings
            cursor.execute("""
                INSERT INTO meetings (
                    uuid, google_calendar_id, user_id, tenant_id, 
                    participants_emails, participants_hash, link, subject, 
                    location, start_time, end_time, goals, agenda, 
                    classification, reminder_sent, reminder_schedule
                )
                SELECT 
                    uuid, google_calendar_id, user_id, tenant_id, 
                    participants_emails, participants_hash, link, subject, 
                    location, start_time, end_time, goals, agenda, 
                    classification, reminder_sent, reminder_schedule
                FROM 
                    meetings_temp;
            """)

            # Step 7: Drop meetings_temp table
            cursor.execute("DROP TABLE IF EXISTS meetings_temp;")

            # Commit the transaction
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Downgrade logic (optional and depends on the project)
            pass