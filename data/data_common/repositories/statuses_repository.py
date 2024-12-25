from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection
from data.data_common.data_transfer_objects.status_dto import StatusDTO

logger = GenieLogger()


class StatusesRepository:
    def __init__(self):
        self.create_table_if_not_exists()


    def create_table_if_not_exists(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS tenant_profiles (
                id SERIAL PRIMARY KEY,
                person_uuid VARCHAR NOT NULL,
                tenant_id VARCHAR NOT NULL,
                status VARCHAR,
                current_event VARCHAR,
                current_event_start_time TIMESTAMP,
            );
            """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
            except Exception as error:
                logger.error(f"Error creating table: {error}")
                traceback.print_exc()

    def insert_status(self, status_dto: StatusDTO):
        query = f"""
            INSERT INTO tenant_profiles (person_uuid, tenant_id, status, current_event, current_event_start_time)
            VALUES (%s, %s, %s, %s, %s);
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, status_dto.to_tuple())
                    conn.commit()
            except psycopg2.Error as error:
                raise Exception(f"Error inserting profile, because: {error.pgerror}")


    def get_status(self, person_uuid: str, tenant_id: str):
        query = f"""
            SELECT status,  FROM tenant_profiles WHERE person_uuid = '{person_uuid}' AND tenant_id = '{tenant_id}';
        """
        with db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchone()
