from typing import Optional, List
from pydantic import BaseModel, Field, HttpUrl
from datetime import datetime

from common.utils.str_utils import get_uuid4
from data.data_common.data_transfer_objects.artifact_dto import ArtifactDTO, ArtifactType, ArtifactSource
from common.genie_logger import GenieLogger

logger = GenieLogger()


class WorkHistoryEntry(BaseModel):
    title: str = Field(..., description="Job title")
    role: Optional[str] = None  # Sales, Engineering, etc.
    levels: List[str] = Field(default=[], description="Job level (Manager, Director, VP, etc.)")
    company_name: str = Field(..., description="Company name")
    industry: Optional[str] = None
    company_size: Optional[str] = None  # 1-10, 11-50, etc.
    start_date: Optional[str] = None  # YYYY-MM format
    end_date: Optional[str] = None  # YYYY-MM format or None if current
    summary: Optional[str] = None  # Job description
    metadata: dict | None = None

class WorkHistoryArtifact(ArtifactDTO):
    title: str = Field(..., description="Job title")
    role: Optional[str] = None  # Sales, Engineering, etc.
    artifact_url: str | None = None
    published_date: str | None = None
    created_at: str | None = None
    levels: List[str] = Field(default=[], description="Job level (Manager, Director, VP, etc.)")
    company_name: str = Field(..., description="Company name")
    industry: Optional[str] = None
    company_size: Optional[str] = None  # 1-10, 11-50, etc.
    start_date: Optional[str] = None  # YYYY-MM format
    end_date: Optional[str] = None  # YYYY-MM format or None if current

    @classmethod
    def from_work_history_entry(cls, entry: WorkHistoryEntry, profile_uuid: str) -> 'WorkHistoryArtifact':
        return WorkHistoryArtifact(
            uuid=get_uuid4(),
            artifact_type=ArtifactType.POST,
            source=ArtifactSource.WORK_EXPERIENCE,
            profile_uuid=profile_uuid,
            artifact_url=None,
            text=entry.summary if entry.summary else entry.title,
            summary=entry.summary if entry.summary else "",
            metadata= entry.metadata,
            title=entry.title,
            role=entry.role,
            levels=entry.levels,
            company_name=entry.company_name,
            industry=entry.industry,
            company_size=entry.company_size,
            start_date=entry.start_date,
            end_date=entry.end_date
        )

    def from_dict(self, data: dict):
        return WorkHistoryArtifact(
            uuid=data.get("uuid"),
            artifact_type=data.get("artifact_type"),
            source=data.get("source"),
            profile_uuid=data.get("profile_uuid"),
            artifact_url=data.get("artifact_url"),
            text=data.get("text"),
            summary=data.get("summary"),
            published_date=data.get("published_date"),
            created_at=data.get("created_at"),
            metadata=data.get("metadata"),
            title=data.get("title"),
            role=data.get("role"),
            levels=data.get("levels"),
            company_name=data.get("company_name"),
            industry=data.get("industry"),
            company_size=data.get("company_size"),
            start_date=data.get("start_date"),
            end_date=data.get("end_date")
        )

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "artifact_type": self.artifact_type,
            "source": self.source,
            "profile_uuid": self.profile_uuid,
            "artifact_url": self.artifact_url,
            "text": self.text,
            "summary": self.summary,
            "published_date": self.published_date,
            "created_at": self.created_at,
            "metadata": self.metadata,
            "title": self.title,
            "role": self.role,
            "levels": self.levels,
            "company_name": self.company_name,
            "industry": self.industry,
            "company_size": self.company_size,
            "start_date": self.start_date,
            "end_date": self.end_date
        }

    @classmethod
    def from_pdl_element(cls, pdl_element: dict, profile_uuid: str) -> 'WorkHistoryArtifact':
        title_data = pdl_element.get("title", {})
        company_data = pdl_element.get("company", {})

        return WorkHistoryArtifact(
            uuid=get_uuid4(),
            artifact_type=ArtifactType.WORK_EXPERIENCE,
            source=ArtifactSource.WORK_EXPERIENCE,
            profile_uuid=profile_uuid,
            text=pdl_element.get("summary", title_data.get("name", "")),
            summary=pdl_element.get("summary", ""),
            metadata=pdl_element,
            title=title_data.get("name", ""),
            role=title_data.get("role"),
            levels=title_data.get("levels", []),
            company_name=company_data.get("name", ""),
            industry=company_data.get("industry"),
            company_size=company_data.get("size"),
            start_date=pdl_element.get("start_date"),
            end_date=pdl_element.get("end_date")
        )

    @classmethod
    def from_apollo_element(cls, apollo_element: dict, profile_uuid: str) -> 'WorkHistoryArtifact':
        return WorkHistoryArtifact(
            uuid=get_uuid4(),
            artifact_type=ArtifactType.WORK_EXPERIENCE,
            source=ArtifactSource.WORK_EXPERIENCE,
            profile_uuid=profile_uuid,
            text=apollo_element.get("description") or apollo_element.get("title", " "),
            summary=apollo_element.get("description", " ") or apollo_element.get("title", " "),
            metadata=apollo_element,
            title=apollo_element.get("title", ""),
            role=None,  # Apollo data doesn't include specific roles
            levels=[],  # Apollo data doesn't include job levels
            company_name=apollo_element.get("organization_name", ""),
            industry=None,  # Apollo data doesn't provide industry info
            company_size=None,  # Apollo data doesn't provide company size
            start_date=apollo_element.get("start_date", ""),
            end_date=apollo_element.get("end_date")
        )
