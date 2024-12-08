from data.data_common.utils.postgres_connector import db_connection

def upgrade():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                ALTER TABLE user_badges
                RENAME COLUMN earned_at to first_earned_at;
            """)
            conn.commit()
#
# def downgrade():
#     with db_connection() as conn:
#         with conn.cursor() as cursor:
#             cursor.execute("""
#                 ALTER TABLE user_badges DROP COLUMN seen;
#             """)
#             conn.commit()
