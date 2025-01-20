import traceback
import psycopg2
from common.genie_logger import GenieLogger
from data.data_common.utils.postgres_connector import db_connection
from typing import List, Optional
from data.data_common.data_transfer_objects.artifact_dto import (
    ArtifactScoreDTO,
)

logger = GenieLogger()


class ArtifactScoresRepository:
    def __init__(self):
        self.create_tables_if_not_exists()

    def create_tables_if_not_exists(self):
        artifact_score_query = """
            CREATE TABLE IF NOT EXISTS artifact_scores (
                id SERIAL PRIMARY KEY,
                uuid UUID,
                artifact_uuid VARCHAR NOT NULL,
                param VARCHAR,
                param_score INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(artifact_score_query)
                    conn.commit()
            except Exception as error:
                logger.error(f"Error creating tables: {error}")
                traceback.print_exc()


    def insert_artifact_score(self, artifact_score: ArtifactScoreDTO) -> Optional[str]:
        insert_query = """
        INSERT INTO artifact_scores (uuid, artifact_uuid, param, param_score, created_at)
        VALUES (%s, %s, %s, %s, current_timestamp)
        RETURNING uuid;
        """
        artifact_score_data = artifact_score.to_tuple()

        logger.info(f"About to insert artifact score data: {artifact_score_data}")

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(insert_query, artifact_score_data)
                    conn.commit()
                    uuid = cursor.fetchone()[0]
                    logger.info(f"Inserted artifact score into database. Artifact Score ID: {uuid}")
                    return uuid
            except psycopg2.Error as error:
                logger.error(f"Error inserting artifact score: {error.pgerror}")
                traceback.print_exc()
                return None

    def get_artifact_score(self, uuid: str) -> Optional[ArtifactScoreDTO]:
        select_query = """
        SELECT uuid, artifact_uuid, param, param_score, created_at FROM artifact_scores WHERE uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (uuid,))
                    result = cursor.fetchone()
                    if result:
                        return ArtifactScoreDTO.from_tuple(result)
                    return None
            except psycopg2.Error as error:
                logger.error(f"Error getting artifact score: {error.pgerror}")
                traceback.print_exc()
                return None
            
    def get_artifact_scores_by_artifact_uuid(self, artifact_uuid: str) -> List[ArtifactScoreDTO]:
        select_query = """
        SELECT uuid, artifact_uuid, param, param_score, created_at FROM artifact_scores WHERE artifact_uuid = %s;
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (artifact_uuid,))
                    results = cursor.fetchall()
                    return [ArtifactScoreDTO.from_tuple(result) for result in results]
            except psycopg2.Error as error:
                logger.error(f"Error getting artifact scores: {error.pgerror}")
                traceback.print_exc()
                return []

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

