import psycopg2
import traceback
from psycopg2 import sql
from dotenv import load_dotenv
import os
from contextlib import contextmanager

from common.utils import env_utils
from common.genie_logger import GenieLogger

logger = GenieLogger()
load_dotenv()

DEV_MODE = env_utils.get("DEV_MODE", "")

# Retrieve the environment variables
db_user = env_utils.get(DEV_MODE + "DB_USER")
host = env_utils.get(DEV_MODE + "DB_HOST")
database = env_utils.get(DEV_MODE + "DB_NAME")
password = env_utils.get(DEV_MODE + "DB_PASSWORD")
port = int(env_utils.get(DEV_MODE + "DB_PORT"))


def create_database_if_not_exists():
    try:
        # Connect to the PostgreSQL server without specifying the database
        conn = psycopg2.connect(user=db_user, host=host, password=password, port=port, database="postgres")
        conn.autocommit = True  # Enable autocommit for creating the database
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [database])
            exists = cursor.fetchone()
            if not exists:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
                logger.info(f"Database '{database}' created successfully")
        conn.close()
    except Exception as error:
        logger.error("Error creating database:", error)
        traceback.print_exc()


def get_db_connection():
    try:
        create_database_if_not_exists()
        conn = psycopg2.connect(user=db_user, host=host, database=database, password=password, port=port)
        return conn
    except Exception as error:
        logger.error("Could not connect to PostgreSQL:", error)
        traceback.print_exc()
        return None


@contextmanager
def db_connection():
    create_database_if_not_exists()
    conn = get_db_connection()
    try:
        if conn:
            yield conn
    finally:
        if conn:
            conn.close()
            logger.info("Connection closed")
