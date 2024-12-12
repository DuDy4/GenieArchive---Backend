from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'user_badges'
                        AND column_name = 'earned_at'
                    ) THEN
                        ALTER TABLE user_badges
                        RENAME COLUMN earned_at TO first_earned_at;
                    END IF;
                END $$;
            """)
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'user_badges'
                        AND column_name = 'first_earned_at'
                    ) THEN
                        ALTER TABLE user_badges
                        RENAME COLUMN first_earned_at TO earned_at;
                    END IF;
                END $$;
            """)
            conn.commit()
