import psycopg2
from dotenv import load_dotenv
import os
from contextlib import contextmanager


# Load environment variables from a .env file
load_dotenv()

# Retrieve the environment variables
db_user = os.getenv("DB_USER")
host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")
password = os.getenv("DB_PASSWORD")
port = int(os.getenv("DB_PORT"))


def get_db_connection():
    try:
        conn = psycopg2.connect(
            user=db_user, host=host, database=database, password=password, port=port
        )
        print("Connected to PostgreSQL")
        return conn
    except Exception as error:
        print("Could not connect to PostgreSQL:", error)
        return None


@contextmanager
def db_connection():
    conn = get_db_connection()
    try:
        if conn:
            yield conn
    finally:
        if conn:
            conn.close()
            print("Connection closed")


# Example usage of the context manager
if __name__ == "__main__":
    with db_connection() as conn:
        if conn:
            # Example query
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                db_version = cursor.fetchone()
                print(f"PostgreSQL database version: {db_version}")
