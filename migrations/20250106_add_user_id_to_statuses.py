from common.genie_logger import logger
from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Step 1: Create statuses_temp table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS statuses_temp (
                    id SERIAL PRIMARY KEY,
                    ctx_id VARCHAR,
                    object_id VARCHAR NOT NULL,
                    object_type VARCHAR,
                    user_id VARCHAR NOT NULL,
                    tenant_id VARCHAR NOT NULL,
                    event_topic VARCHAR,
                    previous_event_topic VARCHAR,
                    current_event_start_time TIMESTAMPTZ,
                    status VARCHAR,
                    error_message TEXT
                );
            """)

            # Step 2: Copy rows to statuses_temp with user_id mapping from users table
            cursor.execute("""
                INSERT INTO statuses_temp (
                    id, ctx_id, object_id, object_type, user_id, tenant_id,
                    event_topic, previous_event_topic, current_event_start_time, status, error_message
                )
                SELECT
                    s.id,
                    s.ctx_id,
                    s.object_id,
                    s.object_type,
                    u.user_id,
                    s.tenant_id,
                    s.event_topic,
                    s.previous_event_topic,
                    s.current_event_start_time,
                    s.status,
                    s.error_message
                FROM 
                    statuses s
                JOIN 
                    users u ON s.tenant_id = u.tenant_id;
            """)

            # Step 3: Validate row counts and ensure no NULL user_id or tenant_id
            cursor.execute("SELECT COUNT(*) FROM statuses;")
            statuses_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM statuses_temp;")
            statuses_temp_count = cursor.fetchone()[0]

            cursor.execute("""
                SELECT s.id
                FROM statuses s
                LEFT JOIN statuses_temp st ON s.id = st.id
                WHERE st.id IS NULL;
            """)
            missing_ids = cursor.fetchall()
            logger.info(f"Problematic statuses: {missing_ids}")

            cursor.execute("""
                SELECT COUNT(*)
                FROM statuses_temp
                WHERE user_id IS NULL OR tenant_id IS NULL;
            """)
            null_check = cursor.fetchone()[0]

            if statuses_count != statuses_temp_count or null_check > 0:
                raise Exception("Validation failed: Row counts differ or NULL values exist in user_id/tenant_id.")

            # Step 4: Drop the original statuses table
            cursor.execute("DROP TABLE IF EXISTS statuses;")

            # Step 5: Recreate the statuses table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS statuses (
                    id SERIAL PRIMARY KEY,
                    ctx_id VARCHAR,
                    object_id VARCHAR NOT NULL,
                    object_type VARCHAR,
                    user_id VARCHAR NOT NULL,
                    tenant_id VARCHAR,
                    event_topic VARCHAR,
                    previous_event_topic VARCHAR,
                    current_event_start_time TIMESTAMPTZ,
                    status VARCHAR,
                    error_message TEXT
                );
            """)

            # Step 6: Copy rows back from statuses_temp to statuses
            cursor.execute("""
                INSERT INTO statuses (
                    ctx_id, object_id, object_type, user_id, tenant_id,
                    event_topic, previous_event_topic, current_event_start_time, status, error_message
                )
                SELECT
                    ctx_id, object_id, object_type, user_id, tenant_id,
                    event_topic, previous_event_topic, current_event_start_time, status, error_message
                FROM 
                    statuses_temp;
            """)

            # Step 7: Drop statuses_temp table
            cursor.execute("DROP TABLE IF EXISTS statuses_temp;")

            # Commit the transaction
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Downgrade logic to restore the original statuses table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS statuses (
                    id SERIAL PRIMARY KEY,
                    ctx_id VARCHAR,
                    object_id VARCHAR NOT NULL,
                    object_type VARCHAR,
                    tenant_id VARCHAR NOT NULL,
                    event_topic VARCHAR,
                    previous_event_topic VARCHAR,
                    current_event_start_time TIMESTAMPTZ,
                    status VARCHAR,
                    error_message TEXT
                );
            """)
            # Add logic to restore data if needed
            conn.commit()
