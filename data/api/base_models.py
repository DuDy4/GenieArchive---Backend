from datetime import datetime

from pydantic import BaseModel, Field, HttpUrl
from typing import List, Optional, Dict

from data.data_common.data_transfer_objects.meeting_dto import AgendaItem, MeetingDTO, MeetingClassification
from data.data_common.data_transfer_objects.profile_dto import (
    ProfileDTO,
    Connection,
    Phrase,
    SalesCriteria,
    Strength,
)
from data.data_common.data_transfer_objects.profile_category_dto import ProfileCategory
from data.data_common.data_transfer_objects.person_dto import PersonDTO
from data.data_common.data_transfer_objects.company_dto import (
    CompanyDTO,
    SocialMediaLinks,
    SocialMediaLinksList,
    FundingEvent,
)
from data.data_common.data_transfer_objects.news_data_dto import NewsData, SocialMediaPost
from data.data_common.utils.str_utils import titleize_name
from data.data_common.repositories.profiles_repository import DEFAULT_PROFILE_PICTURE
from common.genie_logger import GenieLogger

logger = GenieLogger()


class UserResponse(BaseModel):
    tenantId: str
    name: str
    email: str


class TicketData(BaseModel):
    subject: str
    description: str
    name: str
    email: str
    priority: str


class TranslateRequest(BaseModel):
    text: str


class MiniPersonResponse(BaseModel):
    uuid: str
    email: str
    name: Optional[str] = None

    @staticmethod
    def from_person_dto(person: PersonDTO):
        if not person:
            logger.error("Person is None")
            return None
        return MiniPersonResponse(
            uuid=str(person.uuid),
            name=titleize_name(str(person.name)),
            email=person.email,
        )

    @staticmethod
    def from_dict(data: Dict):
        return MiniPersonResponse(
            uuid=data.get("uuid", ""),
            name=data.get("name", ""),
            email=data.get("email", None),
        )

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "name": self.name,
            "email": self.email,
        }


class InternalMiniPersonResponse(BaseModel):
    uuid: str
    email: str
    name: Optional[str] = None
    profile_picture: Optional[HttpUrl] = None

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "name": self.name,
            "email": self.email,
            "profile_picture": self.profile_picture,
        }

    @staticmethod
    def from_dict(data: Dict):
        return InternalMiniPersonResponse(
            uuid=data.get("uuid", ""),
            name=data.get("name", ""),
            email=data.get("email", None),
            profile_picture=data.get("profile_picture", None),
        )

    @staticmethod
    def from_person_dto(person: PersonDTO, profile_picture: Optional[HttpUrl | str] = None):
        if not person:
            logger.error("Person is None")
            return None
        return InternalMiniPersonResponse(
            uuid=str(person.uuid),
            name=titleize_name(str(person.name)),
            email=person.email,
            profile_picture=HttpUrl(profile_picture)
            if profile_picture and isinstance(profile_picture, str)
            else None,
        )


class MiniProfileResponse(BaseModel):
    uuid: str
    name: str
    email: Optional[str] = None
    profile_picture: Optional[str] = None

    @staticmethod
    def from_profile_dto(profile: ProfileDTO, person: Optional[PersonDTO] = None):
        if not profile:
            logger.error("Profile is None")
            return None
        if not person:
            logger.error("Person is None")
            return MiniProfileResponse(uuid=str(profile.uuid), name=titleize_name(str(profile.name)))
        return MiniProfileResponse(
            uuid=str(profile.uuid),
            name=titleize_name(str(profile.name)),
            email=person.email if str(person.uuid) == str(profile.uuid) else None,
            profile_picture=str(profile.picture_url) if profile.picture_url else DEFAULT_PROFILE_PICTURE,
        )

    @staticmethod
    def from_profile_dto_and_email(profile: ProfileDTO, email: str):
        if not profile:
            logger.error("Profile is None")
            return None
        return MiniProfileResponse(
            uuid=str(profile.uuid),
            name=titleize_name(str(profile.name)),
            email=email,
            profile_picture=str(profile.picture_url) if profile.picture_url else None,
        )

    @staticmethod
    def from_dict(data: Dict):
        return MiniProfileResponse(
            uuid=data["uuid"],
            name=data["name"],
            email=data.get("email", None),
            profile_picture=data.get("profile_picture", None),
        )


class MiniProfilesAndPersonsListResponse(BaseModel):
    profiles: List[MiniProfileResponse]
    persons: Optional[List[MiniPersonResponse]] = None

    @staticmethod
    def from_profiles_list(profiles: List[ProfileDTO], persons: Optional[List[PersonDTO]] = None):
        return MiniProfilesAndPersonsListResponse(
            profiles=[MiniProfileResponse.from_profile_dto(profile) for profile in profiles],
            persons=[MiniPersonResponse.from_person_dto(person) for person in persons] if persons else None,
        )


class StrengthsListResponse(BaseModel):
    strengths: List[Strength]
    profile_category: ProfileCategory
    sales_criteria: Optional[list[SalesCriteria]]


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
    news: Optional[List[NewsData | SocialMediaPost]] = []
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


class AttendeeInfo(BaseModel):
    picture: HttpUrl | str | None
    name: str
    company: str
    position: str
    social_media_links: Optional[List[SocialMediaLinks]] = []
    work_history_summary: Optional[str] = None


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
    challenges: Optional[List[Challenge]] = []
    technologies: Optional[List[str]]
    social_links: Optional[List[SocialMediaLinks]]
    news: Optional[List[NewsData]]

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
            social_links=SocialMediaLinksList.from_list(company.social_links).to_list(),
            news=company.news if company.news else None,
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
            responseStatus=data["responseStatus"] if "responseStatus" in data else "needsAction",
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
            participants_emails=[ParticipantEmail.from_dict(email) for email in data["participants_emails"]],
            link=data["link"],
            subject=data["subject"],
            start_time=data["start_time"],
            end_time=data["end_time"],
            companies=data["companies"],
        )


class MeetingsListResponse(BaseModel):
    meetings: List[MeetingResponse]


class MeetingCompany(BaseModel):
    name: str
    overview: str
    size: str
    industry: str
    country: str
    annual_revenue: Optional[str]
    total_funding: Optional[str]
    last_raised_at: Optional[str]
    main_costumers: Optional[str]
    main_competitors: Optional[str]
    technologies: List[str]
    challenges: List[Challenge]
    news: List[NewsData]

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            name=data.get("name", ""),
            overview=data.get("overview", ""),
            size=data.get("size", ""),
            industry=data.get("industry", ""),
            country=data.get("country", ""),
            annual_revenue=data.get("annual_revenue", ""),
            total_funding=data.get("total_funding", ""),
            last_raised_at=data.get("last_raised_at", ""),
            main_costumers=data.get("main_costumers", ""),
            main_competitors=data.get("main_competitors", ""),
            technologies=data.get("technologies", []),
            challenges=data.get("challenges", []),
            news=data.get("news", []),
        )

    @classmethod
    def from_company_dto(cls, company: CompanyDTO):
        return cls(
            name=company.name,
            overview=company.overview,
            size=company.size,
            industry=company.industry,
            country=company.country,
            annual_revenue=company.annual_revenue,
            total_funding=company.total_funding,
            last_raised_at=company.last_raised_at,
            main_costumers=company.main_costumers,
            main_competitors=company.main_competitors,
            technologies=company.technologies,
            challenges=company.challenges,
            news=company.news,
        )


class MidMeetingCompany(BaseModel):
    name: str
    description: Optional[str] | None = None
    logo: Optional[str] | None = None
    overview: Optional[str] | None = None
    size: Optional[str] | None = None
    industry: Optional[str] | None = None
    address: Optional[str] | None = None
    country: Optional[str] | None = None
    annual_revenue: Optional[str] | None = None
    total_funding: Optional[str] | None = None
    funding_rounds: Optional[List[FundingEvent]] | None = []
    technologies: Optional[List[str]] = []
    challenges: Optional[List[Challenge]] = []
    social_links: Optional[List[SocialMediaLinks]] | None = []
    news: List[NewsData] = []

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            name=data.get("name", ""),
            description=data.get("description", ""),
            logo=data.get("logo", ""),
            overview=data.get("overview", ""),
            size=data.get("size", ""),
            industry=data.get("industry", ""),
            address=data.get("address", ""),
            country=data.get("country", ""),
            annual_revenue=data.get("annual_revenue", ""),
            total_funding=data.get("total_funding", ""),
            funding_rounds=data.get("funding_rounds", ""),
            technologies=data.get("technologies", []),
            challenges=data.get("challenges", []),
            social_links=SocialMediaLinksList.from_list(data.get("social_links", [])).to_list(),
            news=data.get("news", []),
        )

    def to_dict(self):
        return {
            "name": self.name,
            "description": self.description,
            "logo": self.logo,
            "overview": self.overview,
            "size": self.size,
            "industry": self.industry,
            "address": self.address,
            "country": self.country,
            "annual_revenue": self.annual_revenue,
            "total_funding": self.total_funding,
            "funding_rounds": self.funding_rounds,
            "technologies": self.technologies,
            "challenges": self.challenges,
            "social_links": self.social_links,
            "news": self.news,
        }

    @classmethod
    def from_company_dto(cls, company: CompanyDTO):
        return cls(
            name=company.name if company.name else "",
            description=company.description,
            logo=company.logo,
            overview=company.overview,
            size=company.size,
            industry=company.industry,
            address=company.address,
            country=company.country,
            annual_revenue=company.annual_revenue,
            total_funding=company.total_funding,
            funding_rounds=company.funding_rounds,
            technologies=company.technologies,
            challenges=company.challenges,
            social_links=SocialMediaLinksList.from_list(company.social_links).to_list()
            if company.social_links
            else None,
            news=company.news,
        )


class MiniMeetingCompany(BaseModel):
    name: str
    description: str
    overview: str
    size: Optional[str] | None
    technologies: List[str]
    challenges: List[Challenge]
    news: List[NewsData]

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            name=data.get("name", ""),
            description=data.get("description", ""),
            overview=data.get("overview", ""),
            size=data.get("size", ""),
            technologies=data.get("technologies", []),
            challenges=data.get("challenges", []),
            news=data.get("news", []),
        )

    def to_dict(self):
        return {
            "name": self.name,
            "description": self.description,
            "overview": self.overview,
            "size": self.size,
            "technologies": self.technologies,
            "challenges": self.challenges,
            "news": self.news,
        }

    @classmethod
    def from_company_dto(cls, company: CompanyDTO):
        return cls(
            name=company.name,
            description=company.description,
            overview=company.overview,
            size=company.size,
            technologies=company.technologies,
            challenges=company.challenges,
            news=company.news,
        )


class Guideline(BaseModel):
    text: str
    duration: str

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            text=data.get("text", ""),
            time=data.get("duration", ""),
        )


class GuideLines(BaseModel):
    total_duration: str
    guidelines: List[Guideline]

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            time=data.get("time", ""),
            guidelines=[Guideline.from_dict(guideline) for guideline in data.get("guidelines", [])],
        )


class MiniMeeting(BaseModel):
    subject: str
    video_link: str
    duration: str
    agenda: Optional[List[AgendaItem]] | None
    classification: MeetingClassification

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            subject=data.get("subject", ""),
            video_link=data.get("video_link", ""),
            duration=data.get("duration", ""),
            agenda=[AgendaItem.from_dict(agenda) for agenda in data.get("agenda", [])],
            classification=data.get("classification", MeetingClassification.EXTERNAL),
        )

    def to_dict(self):
        return {
            "subject": self.subject,
            "video_link": self.video_link,
            "duration": self.duration,
            "agenda": [agenda.to_dict() for agenda in self.agenda],
            "classification": self.classification,
        }

    @classmethod
    def from_meeting_dto(cls, meeting: MeetingDTO):
        return cls(
            subject=meeting.subject,
            video_link=meeting.link,
            duration=str(
                datetime.fromisoformat(meeting.end_time) - datetime.fromisoformat(meeting.start_time)
            ),
            agenda=meeting.agenda,
            classification=meeting.classification
            if meeting.classification
            else MeetingClassification.EXTERNAL,
        )


class MiniMeetingOverviewResponseOld(BaseModel):
    meeting: MiniMeeting
    company: MidMeetingCompany
    participants: List[MiniProfileResponse]

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            meeting=MiniMeeting.from_dict(data.get("meeting", {})),
            company=MidMeetingCompany.from_dict(data.get("company", {})),
            participants=[
                MiniProfileResponse.from_dict(participant) for participant in data.get("participants", [])
            ],
        )

    def to_dict(self):
        return {
            "meeting": self.meeting.to_dict(),
            "company": self.company.to_dict(),
            "participants": [participant.to_dict() for participant in self.participants],
        }


class MiniMeetingOverviewResponse(BaseModel):
    meeting: MiniMeeting
    company: MidMeetingCompany
    participants: MiniProfilesAndPersonsListResponse

    @classmethod
    def from_dict(cls, data: Dict):
        return cls(
            meeting=MiniMeeting.from_dict(data.get("meeting", {})),
            company=MidMeetingCompany.from_dict(data.get("company", {})),
            participants=MiniProfilesAndPersonsListResponse.from_profiles_list(
                data.get("participants", []), data.get("persons", [])
            ),
        )


class MeetingOverviewResponseOld(BaseModel):
    meeting: MiniMeeting
    company: MeetingCompany
    participants: List[MiniProfileResponse]


class MeetingOverviewResponse(BaseModel):
    meeting: MiniMeeting
    company: MeetingCompany
    participants: MiniProfilesAndPersonsListResponse


class InternalMeetingOverviewResponse(BaseModel):
    meeting: MiniMeeting
    participants: List[InternalMiniPersonResponse]


class PrivateMeetingOverviewResponse(BaseModel):
    meeting: MiniMeeting
