import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

# Load environment variables from a .env file
load_dotenv()

# Retrieve the environment variables
db_user = os.getenv("DB_USER")
host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")
password = os.getenv("DB_PASSWORD")
port = int(os.getenv("DB_PORT"))


# Initialize the connection
def get_client():
    try:
        conn = psycopg2.connect(
            user=db_user, host=host, database=database, password=password, port=port
        )
        print("Connected to PostgreSQL")
        return conn
    except Exception as error:
        print("Could not connect to PostgreSQL", error)
        return None


# Connect to the database
conn = get_client()

# Ensure the connection is closed properly
if conn:
    conn.close()
