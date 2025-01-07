from common.genie_logger import logger
from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Step 1: Create stats_temp table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stats_temp (
                    id SERIAL PRIMARY KEY,
                    uuid VARCHAR UNIQUE NOT NULL,
                    action VARCHAR,
                    entity VARCHAR,
                    entity_id VARCHAR,
                    timestamp TIMESTAMP,
                    email VARCHAR,
                    tenant_id VARCHAR,
                    user_id VARCHAR NOT NULL
                );
            """)

            # Step 2: Copy rows to stats_temp with user_id mapping from users table
            cursor.execute("""
                INSERT INTO stats_temp (
                    id, uuid, action, entity, entity_id, timestamp,
                    email, tenant_id, user_id
                )
                SELECT
                    s.id,
                    s.uuid,
                    s.action,
                    s.entity,
                    s.entity_id,
                    s.timestamp,
                    s.email,
                    s.tenant_id,
                    u.user_id
                FROM 
                    stats s
                LEFT JOIN 
                    users u ON s.tenant_id = u.tenant_id;
            """)

            # Step 3: Validate row counts and ensure no NULL user_id
            cursor.execute("SELECT COUNT(*) FROM stats;")
            original_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM stats_temp;")
            temp_count = cursor.fetchone()[0]

            cursor.execute("""
                SELECT COUNT(*) 
                FROM stats_temp 
                WHERE user_id IS NULL;
            """)
            null_user_id_count = cursor.fetchone()[0]

            if original_count != temp_count or null_user_id_count > 0:
                raise Exception(
                    f"Validation failed: Row counts differ ({original_count} != {temp_count}) "
                    f"or NULL values exist in user_id."
                )

            # Step 4: Drop the original stats table
            cursor.execute("DROP TABLE IF EXISTS stats;")

            # Step 5: Recreate the stats table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    id SERIAL PRIMARY KEY,
                    uuid VARCHAR UNIQUE NOT NULL,
                    action VARCHAR,
                    entity VARCHAR,
                    entity_id VARCHAR,
                    timestamp TIMESTAMP,
                    email VARCHAR,
                    tenant_id VARCHAR,
                    user_id VARCHAR NOT NULL
                );
            """)

            # Step 6: Copy rows back from stats_temp to stats
            cursor.execute("""
                INSERT INTO stats (
                    id, uuid, action, entity, entity_id, timestamp,
                    email, tenant_id, user_id
                )
                SELECT
                    id, uuid, action, entity, entity_id, timestamp,
                    email, tenant_id, user_id
                FROM 
                    stats_temp;
            """)

            # Step 7: Reset serial sequence for `id`
            cursor.execute("""
                SELECT setval(
                    pg_get_serial_sequence('stats', 'id'),
                    (SELECT MAX(id) FROM stats),
                    true
                );
            """)

            # Step 8: Drop stats_temp table
            cursor.execute("DROP TABLE IF EXISTS stats_temp;")

            # Commit the transaction
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Downgrade logic to restore the original stats table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    id SERIAL PRIMARY KEY,
                    uuid VARCHAR UNIQUE NOT NULL,
                    action VARCHAR,
                    entity VARCHAR,
                    entity_id VARCHAR,
                    timestamp TIMESTAMP,
                    email VARCHAR,
                    tenant_id VARCHAR
                );
            """)
            # Add logic to restore data if needed
            conn.commit()
