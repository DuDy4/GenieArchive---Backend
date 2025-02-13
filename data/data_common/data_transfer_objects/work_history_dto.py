import json
from typing import Optional, List, Tuple
from pydantic import BaseModel, Field, HttpUrl
from datetime import datetime

from common.utils.str_utils import get_uuid4
from data.data_common.data_transfer_objects.artifact_dto import ArtifactDTO, ArtifactType, ArtifactSource
from common.genie_logger import GenieLogger

logger = GenieLogger()


class WorkHistoryArtifactDTO(ArtifactDTO):
    title: str = Field(..., description="Job title")
    role: Optional[str] = None  # Sales, Engineering, etc.
    published_date: str | None = None
    levels: List[str] = Field(default=[], description="Job level (Manager, Director, VP, etc.)")
    company_name: str = Field(..., description="Company name")
    industry: Optional[str] = None
    company_size: Optional[str] = None  # 1-10, 11-50, etc.
    start_date: Optional[str] = None  # YYYY-MM format
    end_date: Optional[str] = None  # YYYY-MM format or None if current
    text: str | None = None
    description: str | None = None

    @classmethod
    def from_dict(cls, data: dict):
        return WorkHistoryArtifactDTO(
            uuid=data.get("uuid"),
            artifact_type=ArtifactType(data.get("artifact_type")),
            source=ArtifactSource(data.get("source")),
            profile_uuid=data.get("profile_uuid"),
            artifact_url=HttpUrl(data.get("artifact_url")),
            text=data.get("text"),
            description=data.get("description"),
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
            "artifact_type": self.artifact_type.value,
            "source": self.source.value,
            "profile_uuid": self.profile_uuid,
            "artifact_url": str(self.artifact_url),
            "text": self.text,
            "description": self.description,
            "published_date": str(self.published_date) if self.published_date else None,
            "created_at": str(self.created_at) if self.created_at else None,
            "metadata": self.metadata,
            "title": self.title,
            "role": self.role,
            "levels": self.levels,
            "company_name": self.company_name,
            "industry": self.industry,
            "company_size": self.company_size,
            "start_date": str(self.start_date) if self.start_date else None,
            "end_date": str(self.end_date) if self.end_date else None
        }

    # def to_tuple(self) -> Tuple:
    #     return (
    #         self.uuid,
    #         self.artifact_type.value,
    #         self.source.value,
    #         self.profile_uuid,
    #         str(self.artifact_url),
    #         self.text,
    #         self.description,
    #         self.published_date,
    #         self.created_at,
    #         json.dumps(self.metadata),
    #         self.title,
    #         self.role,
    #         self.levels,
    #         self.company_name,
    #         self.industry,
    #         self.company_size,
    #         self.start_date,
    #         self.end_date
    #     )
    #
    # @classmethod
    # def from_tuple(cls, data: Tuple) -> 'WorkHistoryArtifactDTO':
    #     return cls(
    #         uuid=str(data[0]),
    #         artifact_type=ArtifactType(data[1]) if data[1] else ArtifactType.OTHER,
    #         source=ArtifactSource(data[2]) if data[2] else ArtifactSource.OTHER,
    #         profile_uuid=data[3],
    #         artifact_url=data[4],
    #         text=data[5],
    #         description=data[6],
    #         published_date=data[7],
    #         created_at=data[8],
    #         metadata=json.loads(data[9]),
    #         title=data[10],
    #         role=data[11],
    #         levels=data[12],
    #         company_name=data[13],
    #         industry=data[14],
    #         company_size=data[15],
    #         start_date=data[16],
    #         end_date=data[17]
    #     )


    @classmethod
    def from_pdl_element(cls, pdl_element: dict, profile_uuid: str, linkedin_url: str) -> 'WorkHistoryArtifactDTO':
        title_data = pdl_element.get("title", {})
        company_data = pdl_element.get("company", {})

        return WorkHistoryArtifactDTO(
            uuid=get_uuid4(),
            artifact_type=ArtifactType.WORK_EXPERIENCE,
            source=ArtifactSource.LINKEDIN,
            artifact_url="https://www." + linkedin_url,
            profile_uuid=profile_uuid,
            text="",
            description=pdl_element.get("summary", ""),
            metadata=pdl_element,
            title=title_data.get("name"),
            role=title_data.get("role"),
            levels=title_data.get("levels", []),
            company_name=company_data.get("name", ""),
            industry=company_data.get("industry"),
            company_size=company_data.get("size"),
            start_date=pdl_element.get("start_date"),
            end_date=pdl_element.get("end_date")
        )

    @classmethod
    def from_apollo_element(cls, apollo_element: dict, profile_uuid: str, linkedin_url: str) -> 'WorkHistoryArtifactDTO':
        return WorkHistoryArtifactDTO(
            uuid=get_uuid4(),
            artifact_type=ArtifactType.WORK_EXPERIENCE,
            source=ArtifactSource.LINKEDIN,
            artifact_url="https://www." + linkedin_url,
            profile_uuid=profile_uuid,
            text="",
            description=apollo_element.get("description", ""),
            metadata=apollo_element,
            title=apollo_element.get("title", ""),
            company_name=apollo_element.get("organization_name", ""),
            start_date=apollo_element.get("start_date", ""),
            end_date=apollo_element.get("end_date")
        )


class WorkHistoryDescriptionDTO(WorkHistoryArtifactDTO):

    @classmethod
    def from_work_history_artifact(cls, work_history: WorkHistoryArtifactDTO) -> 'WorkHistoryDescriptionDTO':
        return WorkHistoryDescriptionDTO(
            uuid=work_history.uuid,
            artifact_type=work_history.artifact_type,
            source=work_history.source,
            profile_uuid=work_history.profile_uuid,
            artifact_url=work_history.artifact_url,
            text=work_history.text,
            description=work_history.description,
            published_date=work_history.published_date,
            created_at=work_history.created_at,
            metadata=work_history.metadata,
            title=work_history.title,
            role=work_history.role,
            levels=work_history.levels,
            company_name=work_history.company_name,
            industry=work_history.industry,
            company_size=work_history.company_size,
            start_date=work_history.start_date,
            end_date=work_history.end_date
        )

    @classmethod
    def from_pdl_element(cls, pdl_element: dict, profile_uuid: str) -> 'WorkHistoryDescriptionDTO':

        work_history = WorkHistoryArtifactDTO.from_pdl_element(pdl_element, profile_uuid)
        work_history.text = pdl_element.get("summary", "")
        return WorkHistoryDescriptionDTO.from_work_history_artifact(work_history)

    @classmethod
    def from_apollo_element(cls, apollo_element: dict, profile_uuid: str) -> 'WorkHistoryDescriptionDTO':

        work_history = WorkHistoryArtifactDTO.from_apollo_element(apollo_element, profile_uuid)
        work_history.text = apollo_element.get("description", "")
        return WorkHistoryDescriptionDTO.from_work_history_artifact(work_history)

