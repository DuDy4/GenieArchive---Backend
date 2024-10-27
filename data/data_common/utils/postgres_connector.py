import psycopg2
import traceback
from psycopg2 import sql, pool
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

# Create a connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=25,  # Adjust this based on your expected concurrency
    user=db_user,
    password=password,
    host=host,
    port=port,
    database=database,
)


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
        # Get a connection from the pool
        conn = connection_pool.getconn()
        if conn.closed:
            logger.error("Connection is closed")
            return get_db_connection()
        return conn
    except psycopg2.DatabaseError as e:
        logger.error(f"Error getting connection from pool: {e}")
        traceback.print_exc()
        return None


def release_db_connection(conn):
    """Safely release the connection back to the pool."""
    if conn:
        connection_pool.putconn(conn)
        logger.debug("Connection released back to pool")


@contextmanager
def db_connection():
    create_database_if_not_exists()
    conn = get_db_connection()
    try:
        # Check if the connection is still open before using it
        if conn and not conn.closed:
            yield conn
        else:
            logger.error("Connection is already closed")
            raise psycopg2.InterfaceError("Connection is closed")
    except psycopg2.Error as error:
        logger.error(f"Database error: {error}")
        traceback.print_exc()
        yield None
    finally:
        if conn and not conn.closed:
            release_db_connection(conn)


def check_db_connection():
    with db_connection() as conn:
        if conn:
            logger.info("Connection is active")

            return True
        else:
            logger.error("Connection is not active")
            return False
