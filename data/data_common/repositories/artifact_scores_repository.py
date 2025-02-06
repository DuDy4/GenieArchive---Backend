import traceback
import psycopg2
from psycopg2.extras import execute_values
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
                score INTEGER,
                clues_scores JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_artifact_param UNIQUE (artifact_uuid, param)
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

    def upsert_artifact_scores(self, artifact_scores: List[ArtifactScoreDTO]):
        """
        Inserts or updates artifact scores in batch using execute_values dynamically.
        
        :param conn: Active psycopg2 connection
        :param artifact_scores: List of ArtifactScoreDTO objects
        """
        if not artifact_scores:
            return  # No data to insert, avoid running an empty query

        # Hardcoded column names (these must match your table schema)
        columns = ["uuid", "artifact_uuid", "param", "score", "clues_scores", "created_at"]

        # Convert DTO objects to tuples dynamically based on row count
        data = [artifact.to_tuple() for artifact in artifact_scores]

        # Generate the SQL query using execute_values
        query = f"""
        INSERT INTO artifact_scores ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (artifact_uuid, param)
        DO UPDATE SET score = EXCLUDED.score, clues_scores = EXCLUDED.clues_scores, created_at = CURRENT_TIMESTAMP;
        """

        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    execute_values(cursor, query, data)
                    conn.commit()
                    logger.info(f"Inserted artifact scores into database. Artifact Score IDs: {artifact_scores}")
            except psycopg2.Error as error:
                logger.error(f"Error inserting artifact score: {error.pgerror}")
                traceback.print_exc()
                return None

            
    def get_artifact_scores_by_artifact_uuid(self, artifact_uuid: str) -> List[ArtifactScoreDTO]:
        select_query = """
        SELECT uuid, artifact_uuid, param, score, clues_scores, created_at FROM artifact_scores WHERE artifact_uuid = %s;
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

    def get_all_artifact_scores_by_profile_uuid(self, profile_uuid: str) -> List[ArtifactScoreDTO]:
        select_query = """
        SELECT uuid, artifact_uuid, param, score, clues_scores, created_at FROM artifact_scores
        WHERE artifact_uuid IN (SELECT uuid FROM artifacts WHERE profile_uuid = %s);
        """
        with db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(select_query, (profile_uuid,))
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

