from pydantic import BaseModel, Field, HttpUrl, field_validator
from typing import List, Optional, Dict, Union
from data.data_common.data_transfer_objects.profile_dto import (
    ProfileDTO,
    Connection,
    Phrase,
    Strength,
    NewsData,
)
from data.data_common.data_transfer_objects.company_dto import CompanyDTO


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

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            hobby_name=data["hobby_name"],
            icon_url=data["icon_url"],
        )


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


class Challenge(BaseModel):
    challenge_name: str
    reasoning: str
    score: int = Field(..., ge=0, le=100)


class CompanyResponse(BaseModel):
    uuid: str
    name: str
    domain: str
    size: Optional[str]
    description: Optional[str]
    overview: Optional[str]
    challenges: Optional[List[Challenge]]
    technologies: Optional[List[str]]

    @classmethod
    def from_company_dto(cls, company: CompanyDTO):
        return cls(
            uuid=company.uuid,
            name=company.name,
            domain=company.domain,
            size=company.size,
            description=company.description,
            overview=company.overview,
            challenges=company.challenges,
            technologies=company.technologies,
        )


class ParticipantEmail(BaseModel):
    email_address: str
    responseStatus: str
    organizer: Optional[bool]
    self: Optional[bool]

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            email_address=data["email"],
            responseStatus=data["responseStatus"],
            organizer=data.get("organizer", False),
            self=data.get("self", False),
        )


class MeetingResponse(BaseModel):
    uuid: str
    participants_emails: List[ParticipantEmail]
    link: str
    subject: str
    start_time: str
    end_time: str
    companies: List[CompanyResponse]

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            uuid=data["uuid"],
            participants_emails=[
                ParticipantEmail.from_dict(email)
                for email in data["participants_emails"]
            ],
            link=data["link"],
            subject=data["subject"],
            start_time=data["start_time"],
            end_time=data["end_time"],
            companies=data["companies"],
        )


class MeetingsListResponse(BaseModel):
    meetings: List[MeetingResponse]
