import os
from data.data_common.utils.postgres_connector import db_connection
from common.genie_logger import GenieLogger


logger = GenieLogger()

MIGRATIONS_DIR = "migrations"

def get_applied_migrations():
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    id SERIAL PRIMARY KEY,
                    migration_name VARCHAR UNIQUE NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            cursor.execute("SELECT migration_name FROM schema_migrations;")
            return {row[0] for row in cursor.fetchall()}

def mark_migration_as_applied(migration_name):
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO schema_migrations (migration_name) VALUES (%s);", (migration_name,))
            conn.commit()

def apply_migration(migration_file):
    migration_name = os.path.basename(migration_file)
    try:
        module = __import__(f"migrations.{migration_name[:-3]}", fromlist=["upgrade"])
        with db_connection() as conn:
            with conn.cursor() as cursor:
                module.upgrade()
                cursor.execute("INSERT INTO schema_migrations (migration_name) VALUES (%s);", (migration_name,))
                conn.commit()
    except Exception as e:
        logger.error(f"Failed to apply migration {migration_name}: {e}")
        raise

def run_migrations():
    logger.info("Running migrations")
    applied_migrations = get_applied_migrations()
    all_migrations = sorted(os.listdir(MIGRATIONS_DIR))

    for migration in all_migrations:
        if migration not in applied_migrations and migration.endswith(".py"):
            print(f"Applying migration: {migration}")
            apply_migration(os.path.join(MIGRATIONS_DIR, migration))
            print(f"Migration {migration} applied successfully.")

if __name__ == "__main__":
    run_migrations()
