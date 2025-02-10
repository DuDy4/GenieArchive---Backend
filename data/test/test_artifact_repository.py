import pytest
import json
import psycopg2
from datetime import datetime
from uuid import uuid4
from data.data_common.utils.postgres_connector import db_connection
from data.data_common.data_transfer_objects.artifact_dto import ArtifactDTO, ArtifactType, ArtifactSource
from data.data_common.data_transfer_objects.artifact_dto import ArtifactScoreDTO
from data.data_common.repositories.artifacts_repository import ArtifactsRepository
from data.data_common.repositories.artifact_scores_repository import ArtifactScoresRepository

@pytest.fixture(scope="module")
def test_db():
    """Setup a test database connection"""
    with db_connection() as conn:
        yield conn
        conn.rollback()  # Ensure rollback after tests

@pytest.fixture(scope="module")
def artifact_repo():
    """Initialize the ArtifactsRepository"""
    return ArtifactsRepository()

@pytest.fixture(scope="module")
def artifact_scores_repo():
    """Initialize the ArtifactScoresRepository"""
    return ArtifactScoresRepository()

@pytest.fixture
def test_artifact():
    """Create a test artifact"""
    return ArtifactDTO(
        uuid=str(uuid4()),
        artifact_type=ArtifactType.POST,
        source=ArtifactSource.LINKEDIN,
        profile_uuid=str(uuid4()),
        artifact_url="https://linkedin.com/test-post",
        text="Test post content",
        summary="Test summary",
        published_date=datetime.now(),
        created_at=datetime.now(),
        metadata={"tag": "test"}
    )

@pytest.fixture
def test_artifact_score(test_artifact):
    """Create a test artifact score"""
    return ArtifactScoreDTO(
        uuid=str(uuid4()),
        artifact_uuid=test_artifact.uuid,
        param="relevance",
        score=90,
        clues_scores={"key": "value"},
        created_at=datetime.now()
    )

def test_create_artifact(artifact_repo, test_artifact):
    """Test inserting an artifact into the repository"""
    artifact_id = artifact_repo.save_artifact(test_artifact)
    assert artifact_id is not None, "Artifact should be saved successfully"

    retrieved_artifact = artifact_repo.get_artifact(test_artifact.uuid)
    assert retrieved_artifact is not None, "Artifact should be retrievable"
    assert retrieved_artifact.uuid == test_artifact.uuid, "UUID should match"
    assert retrieved_artifact.text == test_artifact.text, "Text should match"

def test_get_user_artifacts(artifact_repo, test_artifact):
    """Test retrieving artifacts by profile UUID"""
    test_create_artifact(artifact_repo, test_artifact)
    artifacts = artifact_repo.get_user_artifacts(test_artifact.profile_uuid)
    assert isinstance(artifacts, list), "Should return a list"
    assert len(artifacts) > 0, "At least one artifact should be found"

def test_check_artifact_existence(artifact_repo, test_artifact):
    """Test checking artifact existence"""
    test_create_artifact(artifact_repo, test_artifact)
    assert artifact_repo.exists(test_artifact.uuid), "Artifact should exist"

def test_insert_artifact_score(artifact_scores_repo, test_artifact_score):
    """Test inserting artifact scores"""
    artifact_scores_repo.upsert_artifact_scores([test_artifact_score])

    retrieved_scores = artifact_scores_repo.get_artifact_scores_by_artifact_uuid(test_artifact_score.artifact_uuid)
    assert len(retrieved_scores) > 0, "Artifact scores should be retrievable"
    assert retrieved_scores[0].score == test_artifact_score.score, "Score should match"

def test_get_artifact_scores_by_profile(artifact_scores_repo, test_artifact):
    """Test retrieving artifact scores by profile UUID"""
    scores = artifact_scores_repo.get_all_artifact_scores_by_profile_uuid(test_artifact.profile_uuid)
    assert isinstance(scores, list), "Should return a list"
    assert len(scores) >= 0, "Should return scores (or empty list if none exist)"
