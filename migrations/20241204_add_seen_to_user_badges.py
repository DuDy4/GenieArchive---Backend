from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_name = 'user_badges' AND column_name = 'seen'
                        ) THEN
                            ALTER TABLE user_badges ADD COLUMN seen BOOLEAN DEFAULT FALSE;
                        END IF;
                    END $$;
            """)
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE user_badges DROP COLUMN seen;
            """)
            conn.commit()
