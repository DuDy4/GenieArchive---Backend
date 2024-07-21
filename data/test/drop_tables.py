from loguru import logger
import sys
import os
import psycopg2
from psycopg2 import sql

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.dependencies.dependencies import get_db_connection


def drop_table(table_name: str):
    """
    Drop the table with the given name from the PostgreSQL database.

    :param table_name: Name of the table to be dropped.
    """
    try:
        # Get the database connection
        conn = get_db_connection()
        cur = conn.cursor()

        # Drop the table
        drop_query = sql.SQL("DROP TABLE IF EXISTS {}").format(
            sql.Identifier(table_name)
        )
        cur.execute(drop_query)
        conn.commit()

        logger.info(f"Table '{table_name}' dropped successfully.")

    except Exception as e:
        logger.error(f"Error occurred while dropping the table '{table_name}': {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


drop_table("profiles")
