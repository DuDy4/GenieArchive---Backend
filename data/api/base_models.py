from pydantic import BaseModel, Field, HttpUrl, field_validator
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

    @staticmethod
    def from_profile_dto(profile: ProfileDTO):
        return MiniProfileResponse(uuid=str(profile.uuid), name=str(profile.name))


class MiniProfilesListResponse(BaseModel):
    profiles: List[MiniProfileResponse]

    @staticmethod
    def from_profiles_list(profiles: List[ProfileDTO]):
        return MiniProfilesListResponse(
            profiles=[
                MiniProfileResponse.from_profile_dto(profile) for profile in profiles
            ]
        )


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


class SocialMediaLinks(BaseModel):
    url: HttpUrl | str
    platform: str


class AttendeeInfo(BaseModel):
    picture: HttpUrl | str | None
    name: str
    company: str
    position: str
    social_media_links: List[SocialMediaLinks]


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
