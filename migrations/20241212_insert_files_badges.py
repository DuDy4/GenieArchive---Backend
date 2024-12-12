from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Insert the first badge
            cursor.execute("""
                INSERT INTO badges (badge_id, name, description, criteria, icon_url, created_at, last_updated)
                VALUES (
                    uuid_generate_v4(), 
                    'Whats up DOC', 
                    'Uploaded your first file.',
                    '{"type": "UPLOAD_FILE", "count": 1, "frequency": "alltime"}',
                    'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', 
                    NOW(), NOW()
                );
            """)
            # Insert the second badge
            cursor.execute("""
                INSERT INTO badges (badge_id, name, description, criteria, icon_url, created_at, last_updated)
                VALUES (
                    uuid_generate_v4(), 
                    'Categorically Speaking', 
                    'Uploaded 2 file categories.',
                    '{"type": "UPLOAD_FILE_CATEGORY", "count": 2, "frequency": "alltime"}',
                    'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', 
                    NOW(), NOW()
                );
            """)
            # Insert the third badge
            cursor.execute("""
                INSERT INTO badges (badge_id, name, description, criteria, icon_url, created_at, last_updated)
                VALUES (
                    uuid_generate_v4(), 
                    'Lord of the Files', 
                    'Uploaded 4 file categories.',
                    '{"type": "UPLOAD_FILE_CATEGORY", "count": 4, "frequency": "alltime"}',
                    'https://frontedresources.blob.core.windows.net/images/badge-lamp.png', 
                    NOW(), NOW()
                );
            """)
            conn.commit()

def downgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM badges 
                WHERE name IN ('Whats up DOC', 'Categorically Speaking', 'Lord of the Files');
            """)
            conn.commit()
