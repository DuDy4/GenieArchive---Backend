import traceback
from data.data_common.utils.postgres_connector import get_db_connection
from common.genie_logger import GenieLogger
import psycopg2

logger = GenieLogger()

conn = get_db_connection()

alter_command = """
ALTER TABLE companies
ADD COLUMN address VARCHAR,
ADD COLUMN logo VARCHAR,
ADD COLUMN founded_year INT,
ADD COLUMN industry VARCHAR,
ADD COLUMN social_links JSONB,
ADD COLUMN annual_revenue VARCHAR,
ADD COLUMN total_funding VARCHAR,
ADD COLUMN funding_rounds JSONB;

ALTER TABLE meetings
ADD COLUMN agenda JSONB;
"""

try:
    logger.debug(f"About to execute command: {alter_command}")
    with conn.cursor() as cursor:
        cursor.execute(alter_command)
    logger.debug("Command executed successfully. About to commit command.")
    conn.commit()
    logger.debug("Command committed successfully.")

except psycopg2.Error as e:
    logger.error(f"Error executing SQL command: {e}")
    conn.rollback()

except Exception as e:
    logger.error(f"Unexpected error: {e}")
    traceback.print_exc()
    conn.rollback()

finally:
    logger.debug("Closing the database connection.")
    conn.close()
