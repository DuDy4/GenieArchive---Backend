from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS artifacts (
                id SERIAL PRIMARY KEY,
                uuid UUID,
                artifact_type VARCHAR NOT NULL,
                source VARCHAR NOT NULL,
                profile_uuid VARCHAR NOT NULL,
                artifact_url VARCHAR NOT NULL,
                text TEXT,
                description TEXT,
                summary TEXT,
                published_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata JSONB
            );
            """)
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                DROP TABLE artifacts;
            """)
            conn.commit()
