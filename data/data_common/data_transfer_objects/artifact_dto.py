import json
from data.data_common.data_transfer_objects.news_data_dto import SocialMediaPost
from data.data_common.data_transfer_objects.profile_dto import SalesCriteria
from pydantic import BaseModel, EmailStr, HttpUrl
from uuid import UUID
from enum import Enum
from datetime import datetime, date
from typing import Tuple, Dict, Any, Optional
from data.data_common.utils.str_utils import get_uuid4


class ArtifactType(Enum):
    POST = "post"
    WORK_EXPERIENCE = "work_experience"
    OTHER = "other"


class ArtifactSource(Enum):
    LINKEDIN = "linkedin"
    OTHER = "other"



class ArtifactDTO(BaseModel):
    uuid: str
    artifact_type: ArtifactType
    source: ArtifactSource
    profile_uuid: str
    artifact_url: HttpUrl | None = None
    text: str
    description: str | None = None
    summary: str | None = None
    published_date: Optional[date | datetime]
    created_at: datetime = datetime.now()
    metadata: Dict[str, Any]

    def to_tuple(self) -> Tuple:
        return (
            self.uuid,
            self.artifact_type.value,
            self.source.value,
            self.profile_uuid,
            str(self.artifact_url),
            self.text,
            self.description,
            self.summary,
            self.published_date,
            self.created_at,
            json.dumps(self.metadata)
        )
    
    @classmethod
    def from_tuple(cls, data: Tuple) -> 'ArtifactDTO':
        return cls(
            uuid=str(data[0]),
            artifact_type=ArtifactType(data[1]) if data[1] else ArtifactType.OTHER,
            source=ArtifactSource(data[2]) if data[2] else ArtifactSource.OTHER,
            profile_uuid=data[3],
            artifact_url=data[4],
            text=data[5],
            description=data[6],
            summary=data[7],
            published_date=data[8],
            created_at=data[9],
            metadata=data[10]
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
            description=data.get('description'),
            summary=data['summary'],
            published_date=data['published_date'],
            created_at=data['created_at'],
            metadata=data['metadata']
        )
    
    @classmethod
    def from_social_media_post(cls, post: SocialMediaPost, profile_uuid) -> 'ArtifactDTO':
        return cls(
            uuid=get_uuid4(),
            artifact_type=ArtifactType.POST,
            source=ArtifactSource.LINKEDIN if post.media == "linkedin" else ArtifactSource.OTHER,
            profile_uuid=profile_uuid,
            artifact_url=post.link,
            text=post.text,
            description=None,
            summary=post.summary if post.summary else post.text[:100],
            published_date=post.date,
            created_at=datetime.now(),
            metadata= post.to_dict()
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'uuid': self.uuid,
            'artifact_type': self.artifact_type.value,
            'source': self.source.value,
            'profile_uuid': self.profile_uuid,
            'artifact_url': str(self.artifact_url),
            'text': self.text,
            'description': self.description,
            'summary': self.summary,
            'published_date': str(self.published_date),
            'created_at': str(self.created_at),
            'metadata': self.metadata
        }


class ArtifactScoreDTO(BaseModel):
    uuid: str
    artifact_uuid: str
    param: str
    score: int | float
    clues_scores: Dict[str, Any]
    created_at: datetime

    def to_tuple(self) -> Tuple:
        return (
            self.uuid,
            self.artifact_uuid,
            self.param,
            self.score,
            json.dumps(self.clues_scores),
            self.created_at
        )
    
    @classmethod
    def from_tuple(cls, data: Tuple) -> 'ArtifactScoreDTO':
        return cls(
            uuid=str(data[0]),
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
    
    @classmethod
    def from_evaluation_dict(cls, artifact_uuid, data: Dict[str, Any]) -> 'ArtifactScoreDTO':
        return cls(
            uuid=get_uuid4(),
            artifact_uuid=artifact_uuid,
            param=data['param'],
            score=data['score'],
            clues_scores=data,
            created_at=datetime.now()
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
    

