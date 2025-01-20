from data.data_common.data_transfer_objects.profile_dto import SalesCriteria
from pydantic import BaseModel, EmailStr
from uuid import UUID
from enum import Enum
from datetime import datetime
from typing import Tuple, Dict, Any


class ArtifactType(Enum):
    POST = "post"


class ArtifactSource(Enum):
    LINKEDIN = "linkedin"



class ArtifactDTO(BaseModel):
    uuid: UUID
    name: str
    artifact_type: ArtifactType
    profile_id: str
    published_date: datetime
    creation_date: datetime
    source: ArtifactSource
    artifact_url: str
    metadata: Dict[str, Any]

    def to_tuple(self) -> Tuple:
        return (
            str(self.uuid),
            self.name,
            self.artifact_type.value,
            self.profile_id,
            self.published_date,
            self.creation_date,
            self.source.value,
            self.artifact_url,
            self.metadata
        )
    
    @classmethod
    def from_tuple(cls, data: Tuple) -> 'ArtifactDTO':
        return cls(
            uuid=data[0],
            name=data[1],
            artifact_type=ArtifactType(data[2]),
            profile_id=data[3],
            published_date=data[4],
            creation_date=data[5],
            source=ArtifactSource(data[6]),
            artifact_url=data[7],
            metadata=data[8]
        )
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ArtifactDTO':
        return cls(
            uuid=data['uuid'],
            name=data['name'],
            artifact_type=ArtifactType(data['artifact_type']),
            profile_id=data['profile_id'],
            published_date=data['published_date'],
            creation_date=data['creation_date'],
            source=ArtifactSource(data['source']),
            artifact_url=data['artifact_url'],
            metadata=data['metadata']
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'uuid': self.uuid,
            'name': self.name,
            'artifact_type': self.artifact_type.value,
            'profile_id': self.profile_id,
            'published_date': self.published_date,
            'creation_date': self.creation_date,
            'source': self.source.value,
            'artifact_url': self.artifact_url,
            'metadata': self.metadata
        }


class ArtifactScoreDTO(BaseModel):
    uuid: UUID
    artifact_uuid: str
    param_score: str
    score: int
    clues_scores: Dict[str, int]
    created_at: datetime

    def to_tuple(self) -> Tuple:
        return (
            str(self.uuid),
            self.artifact_uuid,
            self.param_score,
            self.score,
            self.clues_scores     
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
            'param': self.param_score,
            'score': self.score,
            'clues_scores': self.clues_scores,
            'created_at': self.created_at
        }