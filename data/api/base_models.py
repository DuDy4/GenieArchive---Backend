from pydantic import BaseModel, Field, HttpUrl, field_validator
from typing import List, Optional, Dict, Union
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


class Hobby(BaseModel):
    hobby_name: str
    icon_url: HttpUrl


class GoodToKnowResponse(BaseModel):
    news: List[NewsData]
    hobbies: List[Hobby]
    connections: List[Connection]


class GetToKnowResponse(BaseModel):
    avoid: Optional[List[Phrase]] = None
    best_practices: Optional[List[Phrase]] = None
    phrases_to_use: Optional[List[Phrase]] = None


class WorkPlace(BaseModel):
    company: str
    position: str
    start_date: str
    end_date: str | None

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            company=data["company"],
            position=data["position"],
            start_date=data["start_date"],
            end_date=data["end_date"],
        )


class WorkExperienceResponse(BaseModel):
    experience: List[WorkPlace] = []

    @classmethod
    def from_list_of_dict(cls, data: List[Dict]):
        return cls(experience=[WorkPlace.from_dict(work_place) for work_place in data])


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
