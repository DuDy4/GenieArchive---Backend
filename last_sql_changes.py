import traceback
from data.data_common.utils.postgres_connector import get_db_connection
from common.genie_logger import GenieLogger
import psycopg2
from psycopg2 import sql

logger = GenieLogger()

conn = get_db_connection()

alter_command = """
ALTER TABLE meetings
ADD COLUMN classification VARCHAR;
"""

try:
    logger.debug(f"About to execute command: {alter_command}")
    conn.autocommit = False  # Disable autocommit mode for better control
    with conn.cursor() as cursor:
        # Increase the statement timeout to 30 seconds (30000 milliseconds)
        cursor.execute(sql.SQL("SET statement_timeout TO 30000;"))
        cursor.execute(alter_command)
    logger.debug("Command executed successfully. About to commit command.")
    conn.commit()
    logger.debug("Command committed successfully.")

except psycopg2.OperationalError as e:
    logger.error(f"Operational error with the database: {e}")
    conn.rollback()

except psycopg2.Error as e:
    logger.error(f"Error executing SQL command: {e}")
    conn.rollback()

except Exception as e:
    logger.error(f"Unexpected error: {e}")
    traceback.print_exc()
    conn.rollback()

finally:
    logger.debug("Closing the database connection.")
    if conn:
        conn.close()
