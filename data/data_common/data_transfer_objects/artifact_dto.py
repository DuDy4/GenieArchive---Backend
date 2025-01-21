from data.data_common.data_transfer_objects.profile_dto import SalesCriteria
from pydantic import BaseModel, EmailStr
from uuid import UUID
from enum import Enum
from datetime import datetime
from typing import Tuple, Dict, Any


class ArtifactType(Enum):
    POST = "post"
    OTHER = "other"


class ArtifactSource(Enum):
    LINKEDIN = "linkedin"
    OTHER = "other"



class ArtifactDTO(BaseModel):
    uuid: UUID
    artifact_type: ArtifactType
    source: ArtifactSource
    profile_uuid: str
    artifact_url: str
    text: str
    summary: str
    published_date: datetime
    created_at: datetime
    metadata: Dict[str, Any]

    def to_tuple(self) -> Tuple:
        return (
            str(self.uuid),
            self.artifact_type.value,
            self.source.value,
            self.profile_uuid,
            self.artifact_url,
            self.text,
            self.summary,
            self.published_date,
            self.created_at,
            self.metadata
        )
    
    @classmethod
    def from_tuple(cls, data: Tuple) -> 'ArtifactDTO':
        return cls(
            uuid=data[0],
            artifact_type=ArtifactType(data[1]) if data[1] else ArtifactType.OTHER,
            source=ArtifactSource(data[2]) if data[2] else ArtifactSource.OTHER,
            profile_uuid=data[3],
            artifact_url=data[4],
            text=data[5],
            summary=data[6],
            published_date=data[7],
            created_at=data[8],
            metadata=data[9]
        )
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ArtifactDTO':
        return cls(
            uuid=data['uuid'],
            artifact_type=ArtifactType(data['artifact_type']) if data['artifact_type'] else ArtifactType.OTHER,
            source=ArtifactSource(data['source']) if data['source'] else ArtifactSource.OTHER,
            profile_uuid=data['profile_uuid'],
            artifact_url=data['artifact_url'],
            text=data['text'],
            summary=data['summary'],
            published_date=data['published_date'],
            created_at=data['created_at'],
            metadata=data['metadata']
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'uuid': self.uuid,
            'artifact_type': self.artifact_type,
            'source': self.source,
            'profile_uuid': self.profile_uuid,
            'artifact_url': self.artifact_url,
            'text': self.text,
            'summary': self.summary,
            'published_date': self.published_date,
            'created_at': self.created_at,
            'metadata': self.metadata
        }


class ArtifactScoreDTO(BaseModel):
    uuid: UUID
    artifact_uuid: str
    param: str
    score: int
    clues_scores: Dict[str, int]
    created_at: datetime

    def to_tuple(self) -> Tuple:
        return (
            str(self.uuid),
            self.artifact_uuid,
            self.param,
            self.score,
            self.clues_scores,
            self.created_at
        )
    
    @classmethod
    def from_tuple(cls, data: Tuple) -> 'ArtifactScoreDTO':
        return cls(
            uuid=data[0],
            artifact_uuid=data[1],
            param=data[2],
            score=data[3],
            clues_scores=data[4],
            created_at=data[5]
        )
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ArtifactScoreDTO':
        return cls(
            uuid=data['uuid'],
            artifact_uuid=data['artifact_uuid'],
            param=data['param'],
            score=data['score'],
            clues_scores=data['clues_scores'],
            created_at=data['created_at']
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'uuid': self.uuid,
            'artifact_uuid': self.artifact_uuid,
            'param': self.param,
            'score': self.score,
            'clues_scores': self.clues_scores,
            'created_at': self.created_at
        }