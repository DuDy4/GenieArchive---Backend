from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Check if the column already exists
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name='artifacts' AND column_name='description';
            """)
            column_exists = cursor.fetchone()

            if not column_exists:
                cursor.execute("""
                    ALTER TABLE artifacts ADD COLUMN description TEXT;
                """)
                conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Drop the column only if it exists
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name='artifacts' AND column_name='description';
            """)
            column_exists = cursor.fetchone()

            if column_exists:
                cursor.execute("""
                    ALTER TABLE artifacts DROP COLUMN description;
                """)
                conn.commit()
