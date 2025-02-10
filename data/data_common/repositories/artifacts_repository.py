import traceback
import psycopg2
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection
from typing import List, Optional
from data.data_common.data_transfer_objects.artifact_dto import (
    ArtifactDTO, ArtifactScoreDTO, ArtifactType, ArtifactSource
)

logger = GenieLogger()


class ArtifactsRepository:
    def __init__(self):
        self.create_tables_if_not_exists()

    def create_tables_if_not_exists(self):
        artifact_query = """
            CREATE TABLE IF NOT EXISTS artifacts (
                id SERIAL PRIMARY KEY,
                uuid UUID,
                artifact_type VARCHAR NOT NULL,
                source VARCHAR NOT NULL,
                profile_uuid VARCHAR NOT NULL,
                artifact_url VARCHAR NOT NULL,
                text TEXT,
                summary TEXT,
                published_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata JSONB
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

    def get_artifact(self, uuid: str) -> Optional[ArtifactDTO]:
        select_query = """
        SELECT uuid, artifact_type, source, profile_uuid, artifact_url, text, summary, published_date, created_at, metadata
        FROM artifacts
        WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid,))
                    return ArtifactDTO.from_tuple(cursor.fetchone())
            except psycopg2.Error as error:
                logger.error(f"Error getting artifact: {error.pgerror}")
                traceback.print_exc()
                return None

    def get_user_artifacts(self, profile_uuid: str, artifact_type: ArtifactType = None) -> List[ArtifactScoreDTO]:
        select_query = """
        SELECT uuid, artifact_type, source, profile_uuid, artifact_url, text, summary, published_date, created_at, metadata
        FROM artifacts
        WHERE profile_uuid = %s
        """
        if artifact_type:
            select_query += "AND artifact_type = %s"
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    if artifact_type:
                        cursor.execute(select_query, (profile_uuid, artifact_type.value))
                    else:
                        cursor.execute(select_query, (profile_uuid,))
                    return [ArtifactDTO.from_tuple(row) for row in cursor.fetchall()]
            except psycopg2.Error as error:
                logger.error(f"Error getting artifacts: {error.pgerror}")
                traceback.print_exc()
                return []
            
    def save_artifact(self, artifact: ArtifactDTO) -> Optional[str]:
        if self.exists(artifact.uuid):
            return None
        return self._insert_artifact(artifact)
            

    def _insert_artifact(self, artifact: ArtifactDTO) -> Optional[str]:
        insert_query = """
        INSERT INTO artifacts (uuid, artifact_type, source, profile_uuid, artifact_url, text, summary, published_date, created_at, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            
    def get_unique_users(self) -> List[str]:
        select_query = """
        SELECT DISTINCT profile_uuid FROM artifacts;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query)
                    return [row[0] for row in cursor.fetchall()]
            except psycopg2.Error as error:
                logger.error(f"Error getting unique users: {error.pgerror}")
                traceback.print_exc()
                return

    def exists(self, uuid: str):
        select_query = """
        SELECT uuid FROM artifacts WHERE uuid = %s;
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

