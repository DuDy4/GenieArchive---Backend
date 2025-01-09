from common.genie_logger import logger
from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Step 1: Create user_profiles_temp table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_profiles_temp (
                    id SERIAL PRIMARY KEY,
                    uuid VARCHAR UNIQUE NOT NULL,
                    profile_uuid VARCHAR NOT NULL,
                    user_id VARCHAR,
                    tenant_id VARCHAR NOT NULL,
                    connections JSONB DEFAULT '[]',
                    get_to_know JSONB DEFAULT '{}',
                    sales_criteria JSONB DEFAULT '[]',
                    action_items JSONB DEFAULT '[]'
                );
            """)

            # Step 2: Copy rows to user_profiles_temp with user_id mapping from users table
            cursor.execute("""
                INSERT INTO user_profiles_temp (
                    uuid, profile_uuid, user_id, tenant_id,
                    connections, get_to_know, sales_criteria, action_items
                )
                SELECT
                    tp.uuid,
                    tp.profile_uuid,
                    u.user_id,
                    tp.tenant_id,
                    tp.connections,
                    tp.get_to_know,
                    tp.sales_criteria,
                    tp.action_items
                FROM 
                    tenant_profiles tp
                JOIN 
                    users u ON tp.tenant_id = u.tenant_id;
            """)

            # Step 3: Validate row counts and ensure no NULL user_id or tenant_id
            cursor.execute("SELECT COUNT(*) FROM tenant_profiles;")
            tenant_profiles_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM user_profiles_temp;")
            user_profiles_temp_count = cursor.fetchone()[0]

            cursor.execute("""
                SELECT tp.uuid
                FROM tenant_profiles tp
                LEFT JOIN user_profiles_temp upt ON tp.uuid = upt.uuid
                WHERE upt.uuid IS NULL;
            """)
            missing_uuids = cursor.fetchall()
            logger.info(f"Problematic tenant_profiles: {missing_uuids}")

            cursor.execute("""
                SELECT COUNT(*)
                FROM user_profiles_temp
                WHERE user_id IS NULL OR tenant_id IS NULL;
            """)
            null_check = cursor.fetchone()[0]



            if tenant_profiles_count != user_profiles_temp_count or null_check > 0:
                raise Exception("Validation failed: Row counts differ or NULL values exist in user_id/tenant_id.")
            #
            # # Step 4: Drop the original tenant_profiles table
            # cursor.execute("DROP TABLE IF EXISTS tenant_profiles;")

            # Step 5: Recreate the user_profiles table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_profiles (
                    id SERIAL PRIMARY KEY,
                    uuid VARCHAR UNIQUE NOT NULL,
                    profile_uuid VARCHAR NOT NULL,
                    user_id VARCHAR,
                    tenant_id VARCHAR NOT NULL,
                    connections JSONB DEFAULT '[]',
                    get_to_know JSONB DEFAULT '{}',
                    sales_criteria JSONB DEFAULT '[]',
                    action_items JSONB DEFAULT '[]'
                );
            """)

            # Step 6: Copy rows back from user_profiles_temp to user_profiles
            cursor.execute("""
                INSERT INTO user_profiles (
                    uuid, profile_uuid, user_id, tenant_id,
                    connections, get_to_know, sales_criteria, action_items
                )
                SELECT
                    uuid, profile_uuid, user_id, tenant_id,
                    connections, get_to_know, sales_criteria, action_items
                FROM 
                    user_profiles_temp;
            """)

            # Step 7: Drop user_profiles_temp table
            cursor.execute("DROP TABLE IF EXISTS user_profiles_temp;")

            # Commit the transaction
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Downgrade logic to restore tenant_profiles (optional)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tenant_profiles (
                    id SERIAL PRIMARY KEY,
                    uuid VARCHAR UNIQUE NOT NULL,
                    profile_uuid VARCHAR NOT NULL,
                    tenant_id VARCHAR NOT NULL,
                    connections JSONB DEFAULT '[]',
                    get_to_know JSONB DEFAULT '{}',
                    sales_criteria JSONB DEFAULT '[]',
                    action_items JSONB DEFAULT '[]'
                );
            """)
            # Add logic to restore data if needed
            conn.commit()
