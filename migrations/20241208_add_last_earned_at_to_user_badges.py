from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE user_badges
                ADD COLUMN last_earned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """)
            conn.commit()

# def downgrade():
#     with db_connection() as conn:
#         with conn.cursor() as cursor:
#             cursor.execute("""
#                 ALTER TABLE tenant_profiles DROP COLUMN sales_criteria;
#             """)
#             conn.commit()
