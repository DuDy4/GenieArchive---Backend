import traceback
import psycopg2
from datetime import datetime
from data.data_common.data_transfer_objects.file_upload_dto import FileUploadDTO
from common.genie_logger import GenieLogger

logger = GenieLogger()


class FileUploadRepository:
    def __init__(self, conn):
        self.conn = conn
        self.create_table_if_not_exists()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS file_uploads (
            id SERIAL PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            file_name VARCHAR,
            file_hash VARCHAR UNIQUE,
            upload_time TIMESTAMP,
            email VARCHAR,
            tenant_id VARCHAR
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
        except Exception as error:
            logger.error("Error creating table:", error)

    def insert(self, file_upload: FileUploadDTO) -> str | None:
        """
        :param file_upload: FileUploadDTO object with file upload data to insert into the database
        :return: the uuid of the newly created file upload in the database
        """
        insert_query = """
        INSERT INTO file_uploads (uuid, file_name, file_hash, upload_time, email, tenant_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        file_data = file_upload.to_tuple()

        logger.info(f"About to insert file upload data: {file_data}")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, file_data)
                self.conn.commit()
                file_id = cursor.fetchone()[0]
                logger.info(f"Inserted file upload to database. File id: {file_id}")
                return str(file_upload.uuid)
        except psycopg2.Error as error:
            logger.error(f"Error inserting file upload: {error.pgerror}")
            traceback.print_exc()
            raise Exception(f"Error inserting file upload, because: {error.pgerror}")

    def exists(self, file_hash: str) -> bool:
        logger.info(f"About to check if file with hash {file_hash} exists")
        exists_query = """SELECT uuid FROM file_uploads WHERE file_hash = %s;"""

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(exists_query, (file_hash,))
                result = cursor.fetchone()
                return result[0] if result else None
        except psycopg2.Error as error:
            logger.error(f"Error checking existence of file: {error}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def get_files_by_email(self, email: str) -> list[FileUploadDTO] | None:
        select_query = """
        SELECT * FROM file_uploads WHERE email = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email,))
                files = cursor.fetchall()
                if files:
                    logger.info(f"Got files uploaded by email {email}")
                    return [FileUploadDTO.from_tuple(file[1:]) for file in files]
                logger.info(f"No files found for email {email}")
                return None
        except psycopg2.Error as error:
            logger.error(f"Error getting files by email: {error}")
            traceback.print_exc()
            return None
