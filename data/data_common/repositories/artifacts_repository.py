import traceback
import psycopg2
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection
from typing import List, Optional, Any
from data.data_common.data_transfer_objects.artifact_dto import (
    ArtifactDTO, ArtifactScoreDTO, ArtifactType, ArtifactSource
)
from data.data_common.data_transfer_objects.work_history_dto import WorkHistoryArtifactDTO

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
                artifact_url VARCHAR,
                text TEXT,
                description TEXT,
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
        SELECT uuid, artifact_type, source, profile_uuid, artifact_url, text, description, summary, published_date, created_at, metadata
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

    def get_user_artifacts(self, profile_uuid: str, artifact_type: ArtifactType = None) -> list[ArtifactDTO] | list[
        Any]:
        select_query = """
        SELECT uuid, artifact_type, source, profile_uuid, artifact_url, text, description, summary, published_date, created_at, metadata
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
                    rows = cursor.fetchall()
                    logger.info(f"Retrieved {len(rows)} artifacts for profile {profile_uuid}")
                    return [ArtifactDTO.from_tuple(row) for row in rows]
            except psycopg2.Error as error:
                logger.error(f"Error getting artifacts: {error.pgerror}")
                traceback.print_exc()
                return []
            
    def save_artifact(self, artifact: ArtifactDTO) -> Optional[str]:
        if self.exists(artifact.uuid):
            return None
        return self._insert_artifact(artifact)
            

    def _insert_artifact(self, artifact: ArtifactDTO) -> Optional[str]:
        insert_query = f"""
        INSERT INTO artifacts (uuid, artifact_type, source, profile_uuid, artifact_url, text, description, summary, published_date, {"created_at, " if artifact.created_at else ''} metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, {'%s,' if artifact.created_at else ''} %s)
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
            
    def get_unique_users(self) -> List[dict]:
        select_query = """
        SELECT DISTINCT p.uuid, p.name FROM artifacts a
	        join persons p on p.uuid = a.profile_uuid;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query)
                    return [{"uuid": row[0], "name": row[1]} for row in cursor.fetchall()]
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

    def exists_work_history_element(self, work_history_element: WorkHistoryArtifactDTO):
        select_query = """
        SELECT uuid FROM artifacts 
        WHERE profile_uuid = %s 
          AND artifact_type = %s 
          AND COALESCE(metadata->'company'->>'name', '') = %s 
          AND COALESCE(metadata->'title'->>'name', '') = %s 
          AND COALESCE(metadata->>'start_date', '') = %s 
          AND COALESCE(metadata->>'end_date' IS NULL OR metadata->>'end_date' = %s);
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (
                        work_history_element.profile_uuid,
                        ArtifactType.WORK_EXPERIENCE.value,
                        work_history_element.company_name,
                        work_history_element.title,
                        work_history_element.start_date,
                        work_history_element.end_date if work_history_element.end_date else 'null',
                    ))
                    result = cursor.fetchone()
                    return result[0] if result is not None else None
            except psycopg2.Error as error:
                logger.error(f"Error checking if work history element exists: {error.pgerror}")
                traceback.print_exc()
                return False

    def check_existing_artifact(self, artifact: ArtifactDTO) -> Optional[str]:
        select_query = """
        SELECT uuid FROM artifacts WHERE artifact_url = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (artifact.artifact_url,))
                    return cursor.fetchone()
            except psycopg2.Error as error:
                logger.error(f"Error checking if artifact exists: {error.pgerror}")
                traceback.print_exc()
                return None

