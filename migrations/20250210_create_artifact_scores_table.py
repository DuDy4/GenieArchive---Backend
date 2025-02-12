from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS artifact_scores (
                id SERIAL PRIMARY KEY,
                uuid UUID,
                artifact_uuid VARCHAR NOT NULL,
                param VARCHAR,
                score INTEGER,
                clues_scores JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_artifact_param UNIQUE (artifact_uuid, param)
            );
            """)
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                DROP TABLE artifact_scores;
            """)
            conn.commit()
