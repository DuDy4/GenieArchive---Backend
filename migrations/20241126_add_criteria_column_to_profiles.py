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
                            WHERE table_name = 'profiles' AND column_name = 'sales_criteria'
                        ) THEN
                            ALTER TABLE profiles ADD COLUMN sales_criteria JSONB;
                        END IF;
                    END $$;
            """)
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE tenant_profiles DROP COLUMN sales_criteria;
            """)
            conn.commit()
