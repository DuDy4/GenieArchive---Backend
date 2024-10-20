import traceback
import psycopg2
from datetime import datetime
from data.data_common.data_transfer_objects.file_upload_dto import FileUploadDTO, FileStatusEnum
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
            upload_time_epoch BIGINT,
            upload_timestamp TIMESTAMP,
            email VARCHAR,
            tenant_id VARCHAR,
            status VARCHAR DEFAULT 'UPLOADED' NOT NULL,
            categories JSONB DEFAULT '[]'::JSONB NOT NULL
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
        INSERT INTO file_uploads (uuid, file_name, file_hash, upload_time_epoch, upload_timestamp, email, tenant_id, status, categories)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        file_data = (
            str(file_upload.uuid),
            file_upload.file_name,
            file_upload.file_hash,
            file_upload.upload_time_epoch,
            file_upload.upload_timestamp,
            file_upload.email,
            file_upload.tenant_id,
            file_upload.status.value,
            file_upload.categories,
        )

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
        SELECT uuid, file_name, file_hash, upload_time_epoch, upload_timestamp, email, tenant_id, status
        FROM file_uploads
        WHERE email = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (email,))
                files = cursor.fetchall()
                if files:
                    logger.info(f"Got files uploaded by email {email}")
                    return [
                        FileUploadDTO(
                            uuid=file[0],
                            file_name=file[1],
                            file_hash=file[2],
                            upload_time_epoch=file[3],
                            upload_timestamp=file[4],
                            email=file[5],
                            tenant_id=file[6],
                            status=file[7],
                        )
                        for file in files
                    ]
                logger.info(f"No files found for email {email}")
                return None
        except psycopg2.Error as error:
            logger.error(f"Error getting files by email: {error}")
            traceback.print_exc()
            return None
        
    def update_file_categories(self, uuid: str, categories: list[str]) -> bool:
        query = """
        UPDATE file_uploads
        SET categories = to_jsonb(%s)
        WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (categories, str(uuid)))
                self.conn.commit()
        except psycopg2.Error as error:
            logger.error(f"Error updating file categories: {error}")
            traceback.print_exc()
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False
        return True
    
    def get_all_files(self, tenant_id: str) -> list[FileUploadDTO] | None:
        select_query = """
        SELECT uuid, file_name, file_hash, upload_time_epoch, upload_timestamp, email, tenant_id, status, categories
        FROM file_uploads
        WHERE tenant_id = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(select_query, (tenant_id,))
                files = cursor.fetchall()
                if files:
                    logger.info(f"Got all files uploaded by tenant {tenant_id}")
                    return [
                        FileUploadDTO(
                            uuid=file[0],
                            file_name=file[1],
                            file_hash=file[2],
                            upload_time_epoch=file[3],
                            upload_timestamp=file[4],
                            email=file[5],
                            tenant_id=file[6],
                            status=file[7],
                            categories=file[8]
                        )
                        for file in files
                    ]
                logger.info(f"No files found for tenant {tenant_id}")
                return None
        except psycopg2.Error as error:
            logger.error(f"Error getting all files by tenant: {error}")
            traceback.print_exc()
            return None

    def get_file_count_and_last_upload_time(self, tenant_id: str) -> tuple[int, datetime | None]:
        """
        Get the count of files and the timestamp of the last upload for a specific tenant.
        :param tenant_id: ID of the tenant
        :return: (upload_count, last_upload_time) tuple
        """
        query = """
        SELECT COUNT(*), MAX(upload_timestamp)
        FROM file_uploads
        WHERE tenant_id = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (tenant_id,))
                result = cursor.fetchone()
                upload_count, last_upload_time = result if result else (0, None)
                logger.info(
                    f"Tenant {tenant_id} has {upload_count} files uploaded, last upload at {last_upload_time}"
                )
                return upload_count, last_upload_time
        except psycopg2.Error as error:
            logger.error(f"Error getting file count and last upload time for tenant {tenant_id}: {error}")
            traceback.print_exc()
            return 0, None

    def exists_metadata(self, file_upload_dto):
        query = """
        SELECT * FROM file_uploads WHERE file_name = %s AND email = %s AND tenant_id = %s AND upload_time_epoch = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        file_upload_dto.file_name,
                        file_upload_dto.email,
                        file_upload_dto.tenant_id,
                        file_upload_dto.upload_time_epoch,
                    ),
                )
                result = cursor.fetchone()
                return result is not None
        except psycopg2.Error as error:
            logger.error(f"Error checking existence of file metadata: {error}")
            traceback.print_exc()
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def update_file_hash(self, file_upload_dto):
        query = """
        UPDATE file_uploads
        SET file_hash = %s
        WHERE file_name = %s AND email = %s AND tenant_id = %s AND upload_time_epoch = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        file_upload_dto.file_hash,
                        file_upload_dto.file_name,
                        file_upload_dto.email,
                        file_upload_dto.tenant_id,
                        file_upload_dto.upload_time_epoch,
                    ),
                )
                self.conn.commit()
        except psycopg2.Error as error:
            logger.error(f"Error updating file metadata: {error}")
            traceback.print_exc()
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False
        return True

    def is_last_file_added(self, tenant_id: str, current_upload_time_epoch: int) -> bool:
        """
        Checks if the current file with the provided `current_upload_time_epoch` is the latest one for a given tenant.

        :param tenant_id: ID of the tenant
        :param current_upload_time_epoch: The epoch time of the current file upload to be checked
        :return: True if it is the latest upload, otherwise False
        """
        query = """
        SELECT MAX(upload_time_epoch) FROM file_uploads WHERE tenant_id = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (tenant_id,))
                latest_upload_time_epoch = cursor.fetchone()[0]

                # Log the comparison of current vs latest upload times
                logger.info(
                    f"Tenant {tenant_id} - Current Upload Time Epoch: {current_upload_time_epoch}, "
                    f"Latest Upload Time Epoch: {latest_upload_time_epoch}"
                )

                # Check if the current upload time is the same as the latest upload time
                is_latest = latest_upload_time_epoch and (
                    current_upload_time_epoch == latest_upload_time_epoch
                )
                logger.info(f"Is current upload the last file: {is_latest}")
                return is_latest
        except psycopg2.Error as error:
            logger.error(f"Error checking last file added for tenant {tenant_id}: {error}")
            traceback.print_exc()
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False
        
    def update_file_status(self, uuid: str, status: FileStatusEnum) -> bool:
        query = """
        UPDATE file_uploads
        SET status = %s
        WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (status.value, str(uuid)))
                self.conn.commit()
        except psycopg2.Error as error:
            logger.error(f"Error updating file status: {error}")
            traceback.print_exc()
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False
        return True

    def delete(self, uuid):
        query = """
        DELETE FROM file_uploads WHERE uuid = %s;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (str(uuid),))
                self.conn.commit()
        except psycopg2.Error as error:
            logger.error(f"Error deleting file: {error}")
            traceback.print_exc()
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False
        return True
