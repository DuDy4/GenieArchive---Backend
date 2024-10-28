import time
import traceback
from data.data_common.utils.postgres_connector import get_db_connection
from common.genie_logger import GenieLogger
import psycopg2
from psycopg2 import sql, OperationalError

logger = GenieLogger()

conn = get_db_connection()

alter_command = """
ALTER TABLE meetings
ADD COLUMN IF NOT EXISTS reminder_sent BOOLEAN DEFAULT FALSE;
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
