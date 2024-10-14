import time
import traceback
from data.data_common.utils.postgres_connector import get_db_connection
from common.genie_logger import GenieLogger
import psycopg2
from psycopg2 import sql, OperationalError

logger = GenieLogger()

conn = get_db_connection()

alter_command = """
-- Daily Badges
INSERT INTO badges (badge_id, name, description, criteria, icon_url, created_at, last_updated) VALUES
(uuid_generate_v4(), 'Genie Hustler', 'View the details of 3 meetings in one day.',
 '{"type": "VIEW_MEETING", "count": 3, "frequency": "daily"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp-bw.png', NOW(), NOW()),

(uuid_generate_v4(), 'Profile Explorer', 'View 5 profiles in one day.',
 '{"type": "VIEW_PROFILE", "count": 5, "frequency": "daily"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp-bw.png', NOW(), NOW()),

(uuid_generate_v4(), 'Genie Marathoner', 'View 12 meetings in one day.',
 '{"type": "VIEW_MEETING", "count": 12, "frequency": "daily"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp-bw.png', NOW(), NOW());

-- Weekly Badges
INSERT INTO badges (badge_id, name, description, criteria, icon_url, created_at, last_updated) VALUES
(uuid_generate_v4(), 'Genie Sprinter', 'View 20 meetings in one week.',
 '{"type": "VIEW_MEETING", "count": 20, "frequency": "weekly"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp-bw.png', NOW(), NOW()),

(uuid_generate_v4(), 'Profile Whisperer', 'View 25 profiles in one week.',
 '{"type": "VIEW_PROFILE", "count": 25, "frequency": "weekly"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp-bw.png', NOW(), NOW()),

(uuid_generate_v4(), 'Daily Devotee', 'Log in 3 times per day for a week.',
 '{"type": "LOGIN_USER", "count": 3, "frequency": "weekly"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp-bw.png', NOW(), NOW());

-- All-Time Badges (Meeting Views)
INSERT INTO badges (badge_id, name, description, criteria, icon_url, created_at, last_updated) VALUES
(uuid_generate_v4(), 'Meeting Newcomer', 'View first meeting.',
 '{"type": "VIEW_MEETING", "count": 1, "frequency": "alltime"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', NOW(), NOW()),

(uuid_generate_v4(), 'Meeting Explorer', 'View 20 meetings.',
 '{"type": "VIEW_MEETING", "count": 20, "frequency": "alltime"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', NOW(), NOW()),

(uuid_generate_v4(), 'Meeting Magician', 'View 50 meetings.',
 '{"type": "VIEW_MEETING", "count": 50, "frequency": "alltime"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', NOW(), NOW()),

(uuid_generate_v4(), 'Meeting Genie Master', 'View 100 meetings.',
 '{"type": "VIEW_MEETING", "count": 100, "frequency": "alltime"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', NOW(), NOW());

-- All-Time Badges (Profile Views)
INSERT INTO badges (badge_id, name, description, criteria, icon_url, created_at, last_updated) VALUES
(uuid_generate_v4(), 'Profile Seeker', 'View first profile.',
 '{"type": "VIEW_PROFILE", "count": 1, "frequency": "alltime"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', NOW(), NOW()),

(uuid_generate_v4(), 'Profile Explorer', 'View 20 profiles.',
 '{"type": "VIEW_PROFILE", "count": 20, "frequency": "alltime"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', NOW(), NOW()),

(uuid_generate_v4(), 'Profile Wizard', 'View 50 profiles.',
 '{"type": "VIEW_PROFILE", "count": 50, "frequency": "alltime"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', NOW(), NOW()),

(uuid_generate_v4(), 'Profile Genie Master', 'View 100 profiles.',
 '{"type": "VIEW_PROFILE", "count": 100, "frequency": "alltime"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', NOW(), NOW());

-- All-Time Badges (User Logins)
INSERT INTO badges (badge_id, name, description, criteria, icon_url, created_at, last_updated) VALUES
(uuid_generate_v4(), 'Habit Builder', 'Log in 5 times.',
 '{"type": "LOGIN_USER", "count": 5, "frequency": "alltime"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', NOW(), NOW()),

(uuid_generate_v4(), 'Consistency Seeker', 'Log in 15 times.',
 '{"type": "LOGIN_USER", "count": 15, "frequency": "alltime"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', NOW(), NOW()),

(uuid_generate_v4(), 'Daily Grinder', 'Log in 50 times.',
 '{"type": "LOGIN_USER", "count": 50, "frequency": "alltime"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', NOW(), NOW()),

(uuid_generate_v4(), 'Routine Genie', 'Log in 100 times.',
 '{"type": "LOGIN_USER", "count": 100, "frequency": "alltime"}',
 'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', NOW(), NOW());
"""

max_retries = 5
retry_count = 0
backoff_time = 2  # Starting backoff time in seconds

while retry_count < max_retries:
    try:
        logger.debug(f"Attempt {retry_count + 1}: Executing command: {alter_command}")
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("SET statement_timeout TO 60000;"))  # Increase timeout to 60 seconds
            cursor.execute(alter_command)
        conn.commit()
        logger.debug("Command executed and committed successfully.")
        break
    except OperationalError as e:
        logger.error(f"Operational error: {e}. Retrying after {backoff_time} seconds...")
        conn.rollback()
        retry_count += 1
        time.sleep(backoff_time)
        backoff_time *= 2  # Exponential backoff
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        conn.rollback()
        break

    finally:
        logger.debug("Closing the database connection.")
        if conn:
            conn.close()
