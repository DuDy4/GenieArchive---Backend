from common.genie_logger import logger
from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Step 1: Create ownerships_temp table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ownerships_temp (
                    id SERIAL PRIMARY KEY,
                    person_uuid VARCHAR NOT NULL,
                    user_id VARCHAR,
                    tenant_id VARCHAR
                );
            """)

            # Step 2: Copy rows to ownerships_temp with user_id mapping from users table
            cursor.execute("""
                INSERT INTO ownerships_temp (
                    id, person_uuid, user_id, tenant_id
                )
                SELECT 
                    o.id,
                    o.person_uuid,
                    u.user_id,
                    o.tenant_id
                FROM 
                    ownerships o
                LEFT JOIN 
                    users u ON o.tenant_id = u.tenant_id;
            """)

            # Step 3: Validate row counts and ensure no NULL user_id or tenant_id
            cursor.execute("SELECT COUNT(*) FROM ownerships;")
            ownerships_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM ownerships_temp;")
            ownerships_temp_count = cursor.fetchone()[0]

            cursor.execute("""
                SELECT o.id
                FROM ownerships o
                LEFT JOIN ownerships_temp ot ON o.person_uuid = ot.person_uuid
                WHERE ot.person_uuid IS NULL;
            """)
            missing_ids = cursor.fetchall()
            logger.info(f"Problematic ownerships: {missing_ids}")

            cursor.execute("""
                SELECT person_uuid, tenant_id
                FROM ownerships_temp
                WHERE user_id IS NULL OR tenant_id IS NULL;
            """)
            null_check = cursor.fetchall()
            logger.info(f"Problematic ownerships: {null_check}")

            if ownerships_count != ownerships_temp_count or len(null_check) > 0:
                raise Exception("Validation failed: Row counts differ or NULL values exist in user_id/tenant_id.")

            # Step 4: Drop the original ownerships table
            cursor.execute("DROP TABLE IF EXISTS ownerships;")

            # Step 5: Recreate the ownerships table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ownerships (
                    id SERIAL PRIMARY KEY,
                    person_uuid VARCHAR NOT NULL,
                    user_id VARCHAR,
                    tenant_id VARCHAR
                );
            """)

            # Step 6: Copy rows back from ownerships_temp to ownerships
            cursor.execute("""
                INSERT INTO ownerships (
                    person_uuid, user_id, tenant_id
                )
                SELECT
                    person_uuid, user_id, tenant_id
                FROM 
                    ownerships_temp;
            """)

            # Step 7: Drop ownerships_temp table
            cursor.execute("DROP TABLE IF EXISTS ownerships_temp;")

            # Commit the transaction
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Downgrade logic to restore the original ownerships table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ownerships (
                    id SERIAL PRIMARY KEY,
                    person_uuid VARCHAR NOT NULL,
                    tenant_id VARCHAR
                );
            """)
            # Add logic to restore data if needed
            conn.commit()
