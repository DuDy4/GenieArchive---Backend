from common.genie_logger import logger
from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Step 1: Create file_uploads_temp table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_uploads_temp (
                    id SERIAL PRIMARY KEY,
                    uuid VARCHAR UNIQUE NOT NULL,
                    file_name VARCHAR,
                    file_hash VARCHAR UNIQUE,
                    upload_time_epoch BIGINT,
                    upload_timestamp TIMESTAMP,
                    email VARCHAR,
                    user_id VARCHAR NOT NULL,
                    tenant_id VARCHAR,
                    status VARCHAR DEFAULT 'UPLOADED' NOT NULL,
                    categories JSONB DEFAULT '[]'::JSONB NOT NULL
                );
            """)

            # Step 2: Copy rows to file_uploads_temp with user_id mapping from users table
            cursor.execute("""
                INSERT INTO file_uploads_temp (
                    id, uuid, file_name, file_hash, upload_time_epoch,
                    upload_timestamp, email, user_id, tenant_id, status, categories
                )
                SELECT
                    f.id,
                    f.uuid,
                    f.file_name,
                    f.file_hash,
                    f.upload_time_epoch,
                    f.upload_timestamp,
                    f.email,
                    u.user_id,
                    f.tenant_id,
                    f.status,
                    f.categories
                FROM 
                    file_uploads f
                LEFT JOIN 
                    users u ON f.tenant_id = u.tenant_id;
            """)

            # Step 3: Validate row counts and ensure no NULL user_id
            cursor.execute("SELECT COUNT(*) FROM file_uploads;")
            original_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM file_uploads_temp;")
            temp_count = cursor.fetchone()[0]

            cursor.execute("""
                SELECT COUNT(*) 
                FROM file_uploads_temp 
                WHERE user_id IS NULL;
            """)
            null_user_id_count = cursor.fetchone()[0]

            if original_count != temp_count or null_user_id_count > 0:
                raise Exception(
                    f"Validation failed: Row counts differ ({original_count} != {temp_count}) "
                    f"or NULL values exist in user_id."
                )

            # Step 4: Drop the original file_uploads table
            cursor.execute("DROP TABLE IF EXISTS file_uploads;")

            # Step 5: Recreate the file_uploads table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_uploads (
                    id SERIAL PRIMARY KEY,
                    uuid VARCHAR UNIQUE NOT NULL,
                    file_name VARCHAR,
                    file_hash VARCHAR UNIQUE,
                    upload_time_epoch BIGINT,
                    upload_timestamp TIMESTAMP,
                    email VARCHAR,
                    user_id VARCHAR NOT NULL,
                    tenant_id VARCHAR,
                    status VARCHAR DEFAULT 'UPLOADED' NOT NULL,
                    categories JSONB DEFAULT '[]'::JSONB NOT NULL
                );
            """)

            # Step 6: Copy rows back from file_uploads_temp to file_uploads
            cursor.execute("""
                INSERT INTO file_uploads (
                    id, uuid, file_name, file_hash, upload_time_epoch,
                    upload_timestamp, email, user_id, tenant_id, status, categories
                )
                SELECT
                    id, uuid, file_name, file_hash, upload_time_epoch,
                    upload_timestamp, email, user_id, tenant_id, status, categories
                FROM 
                    file_uploads_temp;
            """)

            # Step 7: Drop file_uploads_temp table
            cursor.execute("DROP TABLE IF EXISTS file_uploads_temp;")

            # Commit the transaction
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Downgrade logic to restore the original file_uploads table
            cursor.execute("""
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
            """)
            # Add logic to restore data if needed
            conn.commit()
