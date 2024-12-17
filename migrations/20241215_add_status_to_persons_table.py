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
                        WHERE table_name = 'persons'
                        AND column_name = 'status'
                    ) THEN
                        ALTER TABLE persons
                        ADD COLUMN status TEXT;
                    END IF;
                END $$;
            """)
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE persons
                DROP COLUMN IF EXISTS status; 
            """)
            conn.commit()
