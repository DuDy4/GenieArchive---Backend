from common.genie_logger import logger
from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Step 1: Create the `users` table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    uuid VARCHAR UNIQUE NOT NULL,
                    user_id VARCHAR UNIQUE NOT NULL,
                    user_name VARCHAR,
                    email VARCHAR,
                    tenant_id VARCHAR,
                    reminder_subscription BOOLEAN DEFAULT TRUE
                );
            """)

            # Step 2: Insert data from `tenants` into `users` if not already exists
            cursor.execute("""
                INSERT INTO users (uuid, user_id, user_name, email, tenant_id, reminder_subscription)
                SELECT
                    t.uuid,
                    t.user_id,
                    t.user_name,
                    t.email,
                    t.tenant_id,
                    t.reminder_subscription
                FROM tenants t
                WHERE NOT EXISTS (
                    SELECT 1 FROM users u WHERE u.user_id = t.user_id
                );
            """)

            # Commit the transaction
            conn.commit()
            logger.info("Migration completed: users table created and populated with data from tenants.")

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Downgrade logic: Optionally drop the `users` table if needed
            cursor.execute("DROP TABLE IF EXISTS users;")
            conn.commit()
            logger.info("Downgrade completed: users table dropped.")
