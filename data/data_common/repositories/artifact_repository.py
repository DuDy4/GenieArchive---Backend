import traceback
import psycopg2
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection
from typing import List, Optional
from data.data_common.data_transfer_objects.artifact_dto import (
    ArtifactScoreDTO,
)

logger = GenieLogger()


class ArtifactRepository:
    def __init__(self):
        self.create_tables_if_not_exists()

    def create_tables_if_not_exists(self):
        artifact_query = """
            CREATE TABLE IF NOT EXISTS artifact (
                id SERIAL PRIMARY KEY,
                uuid UUID,
                artifact_type VARCHAR NOT NULL,
                profile_uuid VARCHAR NOT NULL,
                source VARCHAR NOT NULL,
                artifact_url VARCHAR NOT NULL,
                metadata JSONB,
                published_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(artifact_query)
                    conn.commit()
            except Exception as error:
                logger.error(f"Error creating tables: {error}")
                traceback.print_exc()

    def get_user_artifacts(self, profile_uuid: str) -> List[ArtifactScoreDTO]:
        select_query = """
        SELECT uuid, artifact_type, profile_uuid, source, artifact_url, metadata, published_date
        FROM artifact
        WHERE profile_uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (profile_uuid,))
                    return [ArtifactScoreDTO.from_tuple(row) for row in cursor.fetchall()]
            except psycopg2.Error as error:
                logger.error(f"Error getting artifacts: {error.pgerror}")
                traceback.print_exc()
                return []
            

    def insert_artifact(self, artifact: ArtifactScoreDTO) -> Optional[str]:
        insert_query = """
        INSERT INTO artifact (uuid, artifact_type, profile_uuid, source, artifact_url, metadata, published_date, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, current_timestamp)
        RETURNING uuid;
        """
        artifact_data = artifact.to_tuple()

        logger.info(f"About to insert artifact data: {artifact_data}")

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, artifact_data)
                    conn.commit()
                    uuid = cursor.fetchone()[0]
                    logger.info(f"Inserted artifact into database. Artifact ID: {uuid}")
                    return uuid
            except psycopg2.Error as error:
                logger.error(f"Error inserting artifact: {error.pgerror}")
                traceback.print_exc()
                return None

    def exists(self, uuid: str):
        select_query = """
        SELECT uuid FROM artifact_scores WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid,))
                    return cursor.fetchone() is not None
            except psycopg2.Error as error:
                logger.error(f"Error checking if deal exists: {error.pgerror}")
                traceback.print_exc()
                return False

