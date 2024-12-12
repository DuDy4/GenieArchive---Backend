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
                        WHERE table_name = 'user_badges'
                        AND column_name = 'last_earned_at'
                    ) THEN
                        ALTER TABLE user_badges
                        ADD COLUMN last_earned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                    END IF;
                END $$;
            """)
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE user_badges
                DROP COLUMN IF EXISTS last_earned_at; 
            """)
            conn.commit()
