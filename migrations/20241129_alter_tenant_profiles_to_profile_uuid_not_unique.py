from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = 'tenant_profiles_profile_uuid_key'
                    ) THEN
                        ALTER TABLE tenant_profiles
                        DROP CONSTRAINT tenant_profiles_profile_uuid_key;
                    END IF;
                END $$;
            """)
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE tenant_profiles
                ADD CONSTRAINT tenant_profiles_profile_uuid_key UNIQUE (profile_uuid);
            """)
            conn.commit()
