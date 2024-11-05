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
        # Connect to the PostgresSQL server without specifying the database
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
    if conn and not conn.closed:
        connection_pool.putconn(conn)
        logger.debug("Connection released back to pool")
    else:
        logger.warning("Attempted to release an already closed connection")


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
        elif conn and conn.closed:
            # Log the closed connection and remove it from the pool
            logger.warning("Connection was closed before releasing; removing from pool")
            connection_pool.putconn(conn, close=True)  # Remove and close the invalid connection


def check_db_connection():
    max_connections = connection_pool.maxconn
    closed_connections = 0

    # Acquire and check each connection in the pool
    for _ in range(max_connections):
        conn = connection_pool.getconn()
        try:
            if conn.closed:
                closed_connections += 1
        finally:
            # Always release the connection back to the pool
            connection_pool.putconn(conn)
    logger.info(f"Checked {max_connections} connections; {closed_connections} are closed")
    # Check if more than half of the connections are closed
    if closed_connections >= max_connections // 2:
        logger.warning(f"More than half of the connections are closed ({closed_connections}/{max_connections}). Consider corrective action.")
        return False
    else:
        logger.info("Connections are active.")
        return True
