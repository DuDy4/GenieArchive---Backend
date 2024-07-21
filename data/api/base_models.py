from pydantic import BaseModel
from typing import List, Optional, Dict


class UserResponse(BaseModel):
    tenantId: str


class MiniProfileResponse(BaseModel):
    uuid: str
    name: str


class Strength(BaseModel):
    strength_name: str
    score: int
    reasoning: str


class StrengthsListResponse(BaseModel):
    strengths: List[Strength]


class GoodToKnowResponse(BaseModel):
    news: Optional[List[dict]] = None
    hobbies: Optional[List[dict]] = None
    connections: Optional[List[dict]] = None


class GetToKnowResponse(BaseModel):
    title: str
    avoid: Optional[List[str]] = None
    best_practices: Optional[List[str]] = None
    phrases_to_use: Optional[List[str]] = None


class WorkExperienceResponse(BaseModel):
    experience: Optional[List[dict]] = None


class AttendeeInfo(BaseModel):
    picture: str
    name: str
    company: str
    position: str
    social_media_links: List[dict]


class NewsItem(BaseModel):
    news_url: str
    news_icon: str
    news_title: str


class ProfileResponse(BaseModel):
    uuid: str
    name: str
    company: str
    position: str
    challenges: List[Dict]
    strengths: List[Strength]
    hobbies: List[str]
    connections: List[str]
    news: List[NewsItem]
    get_to_know: GetToKnowResponse
    summary: str
    picture_url: str


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
