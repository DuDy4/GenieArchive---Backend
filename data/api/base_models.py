from pydantic import BaseModel
from typing import List, Optional, Dict
from data.data_common.data_transfer_objects.profile_dto import (
    ProfileDTO,
    Connection,
    Phrase,
    Strength,
    NewsData,
)


class UserResponse(BaseModel):
    tenantId: str
    name: str
    email: str


class MiniProfileResponse(BaseModel):
    uuid: str
    name: str


class StrengthsListResponse(BaseModel):
    strengths: List[Strength]


class GoodToKnowResponse(BaseModel):
    news: Optional[List[NewsData]] = None
    hobbies: Optional[List[dict]] = None
    connections: Optional[List[Connection]] = None


class GetToKnowResponse(BaseModel):
    title: str
    avoid: Optional[List[Phrase]] = None
    best_practices: Optional[List[Phrase]] = None
    phrases_to_use: Optional[List[Phrase]] = None


class WorkExperienceResponse(BaseModel):
    experience: Optional[List[dict]] = None


class AttendeeInfo(BaseModel):
    picture: str
    name: str
    company: str
    position: str
    social_media_links: List[dict]


class ProfileResponse(BaseModel):
    profile: ProfileDTO


class ProfilesListResponse(BaseModel):
    profiles: List[ProfileResponse]


class MeetingResponse(BaseModel):
    uuid: str
    google_calendar_id: str
    tenant_id: str
    participants_emails: List[str]
    link: str
    subject: str
    start_time: str
    end_time: str


class MeetingsListResponse(BaseModel):
    meetings: List[MeetingResponse]
