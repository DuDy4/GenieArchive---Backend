import json
from typing import List, Dict, Optional, Union, Tuple, Any
from pydantic import HttpUrl, field_validator, BaseModel
from datetime import date

from data.data_common.utils.str_utils import (
    titleize_values,
    to_custom_title_case,
    get_uuid4,
)

from data.data_common.data_transfer_objects.news_data_dto import NewsData

from common.genie_logger import GenieLogger

logger = GenieLogger()


class FundingEvent(BaseModel):
    date: date
    type: Optional[str]
    investors: Optional[List[str]]
    amount: str

    @field_validator("date", "amount")
    def not_empty(cls, value):
        if not value:
            raise ValueError("Field cannot be empty")
        return value

    @classmethod
    def from_json(cls, json_str: str) -> "FundingEvent":
        return cls.parse_raw(json_str)

    def to_json(self) -> str:
        return self.json()

    def to_tuple(self) -> Tuple[date, Optional[HttpUrl], Optional[str], Optional[List[str]], str]:
        return self.date, self.news_url, self.type, self.investors, self.amount

    @classmethod
    def from_tuple(
        cls, data: Tuple[date, Optional[HttpUrl], Optional[str], Optional[List[str]], str]
    ) -> "FundingEvent":
        return cls(date=data[0], news_url=data[1], type=data[2], investors=data[3], amount=data[4])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date.isoformat(),
            "type": self.type,
            "investors": self.investors,
            "amount": self.amount,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "FundingEvent":
        data = titleize_values(data)
        return cls(
            date=data.get("date"),
            type=data.get("type"),
            investors=data.get("investors"),
            amount=data.get("amount"),
        )

    @classmethod
    def from_apollo_object(cls, data: dict) -> "FundingEvent":
        return cls(
            date=data.get("date"),
            type=titleize_values(data.get("type")),
            investors=to_custom_title_case(
                data.get("investors").split(", ") if data.get("investors") else []
            ),
            amount=(data.get("amount") + " " + data.get("currency")) if data.get("amount") else "Unknown",
        )


class SocialMediaLinks(BaseModel):
    url: HttpUrl | str
    platform: str

    @field_validator("url", "platform")
    def not_empty(cls, value):
        if not value:
            raise ValueError("Field cannot be empty")
        return value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": str(self.url),
            "platform": self.platform,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "SocialMediaLinks":
        return cls(
            url=data.get("url"),
            platform=to_custom_title_case(data.get("platform")),
        )


class SocialMediaLinksList(BaseModel):
    links: List[SocialMediaLinks] = []

    def __init__(self, links: List[SocialMediaLinks], **kwargs):
        unique_links = {}
        for link in links:
            if link.platform.lower() not in unique_links:
                unique_links[link.platform.lower()] = link
        super().__init__(links=list(unique_links.values()), **kwargs)

    def append(self, link: SocialMediaLinks):
        if link.platform.lower() not in {l.platform.lower() for l in self.links}:
            self.links.append(link)

    def to_list(self) -> List[Dict[str, Any]]:
        return [link.to_dict() for link in self.links]

    @classmethod
    def from_list(cls, data: List[Dict[str, Any]] | List[SocialMediaLinks]) -> "SocialMediaLinksList":
        links = []
        for item in data:
            # Check if item is a dict and if the URL is non-empty
            if isinstance(item, dict):
                if item.get("url") and isinstance(item.get("url"), str) and item["url"].strip():
                    link = SocialMediaLinks.from_dict(item)
                    links.append(link)
            elif isinstance(item, SocialMediaLinks):
                # If the item is already a SocialMediaLinks object, check its URL
                if item.url and isinstance(item.url, str) and item.url.strip():
                    links.append(item)
            else:
                logger.error(f"Invalid item as social media link: {item}, type: {type(item)}")

        # Only valid links are passed to the constructor
        return cls(links=links)


class CompanyDTO:
    def __init__(
        self,
        uuid: str,
        name: str,
        domain: str,
        address: Optional[str],
        country: Optional[str],
        founded_year: Optional[int],
        size: Optional[str],
        industry: Optional[str],
        description: Optional[str],
        overview: Optional[str],
        challenges: Optional[Union[Dict, List[Dict]]],
        technologies: Optional[Union[Dict, List[Dict], List[str]]],
        logo: Optional[str] = "",
        employees: Optional[Union[Dict, List[Dict]]] = None,
        social_links: Optional[List[SocialMediaLinks]] = None,
        annual_revenue: Optional[str] = None,
        total_funding: Optional[str] = None,
        funding_rounds: Optional[List[FundingEvent]] = None,
        news: Optional[List[NewsData]] = [],
    ):
        self.uuid = uuid
        self.name = name
        self.domain = domain
        self.address = address
        self.country = country
        self.logo = logo
        self.founded_year = founded_year
        self.size = size
        self.industry = titleize_values(industry)
        self.description = titleize_values(description)
        self.overview = titleize_values(overview)
        self.challenges = challenges
        self.technologies = technologies
        self.employees = employees
        self.social_links = social_links
        self.annual_revenue = annual_revenue
        self.total_funding = total_funding
        self.funding_rounds = funding_rounds
        self.news = news

    def to_dict(self):
        return {
            "uuid": str(self.uuid),
            "name": to_custom_title_case(self.name),
            "domain": self.domain,
            "address": self.address,
            "country": titleize_values(self.country),
            "logo": self.logo,
            "founded_year": self.founded_year,
            "size": self.size,
            "industry": self.industry,
            "description": self.description,
            "overview": self.overview,
            "challenges": self.challenges,
            "technologies": self.technologies,
            "employees": self.employees,
            "social_links": [link.to_dict() for link in self.social_links if not isinstance(link, dict)]
            if self.social_links
            else None,
            "annual_revenue": self.annual_revenue,
            "total_funding": self.total_funding,
            "funding_rounds": [
                (round.to_dict() if not isinstance(round, dict) else round) for round in self.funding_rounds
            ]
            if self.funding_rounds
            else None,
            "news": [
                news_item.to_dict() if not isinstance(news_item, dict) else news_item
                for news_item in self.news
            ]
            if self.news
            else None,
        }

    @staticmethod
    def from_dict(data: dict):
        return CompanyDTO(
            uuid=data.get("uuid", ""),
            name=data.get("name", ""),
            domain=data.get("domain", ""),
            address=data.get("address", None),
            country=data.get("country", None),
            logo=data.get("logo", None),
            founded_year=data.get("founded_year", None),
            size=data.get("size", None),
            industry=data.get("industry", None),
            description=data.get("description", None),
            overview=data.get("overview", None),
            challenges=data.get("challenges", None),
            technologies=data.get("technologies", None),
            employees=data.get("employees", None),
            social_links=[SocialMediaLinks.from_dict(link) for link in data.get("social_links", [])]
            if data.get("social_links")
            else None,
            annual_revenue=data.get("annual_revenue", None),
            total_funding=data.get("total_funding", None),
            funding_rounds=[FundingEvent.from_dict(round) for round in data.get("funding_rounds", [])]
            if data.get("funding_rounds")
            else None,
            news=[NewsData.from_dict(item) for item in data.get("news", [])] if data.get("news") else None,
        )

    @staticmethod
    def from_apollo_object(data: dict) -> "CompanyDTO":
        funding_rounds = [
            FundingEvent.from_apollo_object(funding) for funding in data.get("funding_events", [])
        ]

        sorted_funding_rounds = sorted(funding_rounds, key=lambda x: x.date, reverse=True)

        social_links = []
        if data.get("linkedin_url"):
            social_links.append(SocialMediaLinks(platform="linkedin", url=data.get("linkedin_url")))
        if data.get("twitter_url"):
            social_links.append(SocialMediaLinks(platform="twitter", url=data.get("twitter_url")))
        if data.get("facebook_url"):
            social_links.append(SocialMediaLinks(platform="facebook", url=data.get("facebook_url")))
        if data.get("instagram_url"):
            social_links.append(SocialMediaLinks(platform="instagram", url=data.get("instagram_url")))
        if data.get("youtube_url"):
            social_links.append(SocialMediaLinks(platform="youtube", url=data.get("youtube_url")))

        technologies = data.get("current_technologies", [])
        technologies = [tech.get("name") for tech in technologies]

        return CompanyDTO(
            uuid=data.get("uuid", get_uuid4()),
            name=data.get("name", ""),
            domain=data.get("primary_domain", ""),
            address=data.get("raw_address", None),
            country=data.get("country", None),
            logo=data.get("logo_url", None),
            founded_year=data.get("founded_year", None),
            size=str(data.get("estimated_num_employees", None)),
            industry=titleize_values(data.get("industry", None)),
            description=titleize_values(data.get("seo_description") or data.get("short_description") or ""),
            overview=None,
            challenges=[],
            technologies=technologies,
            employees=None,
            social_links=social_links,
            annual_revenue=data.get("annual_revenue_printed", None),
            total_funding=data.get("total_funding_printed", None),
            funding_rounds=sorted_funding_rounds,
            news=None,
        )

    @staticmethod
    def from_hunter_object(data: dict) -> "CompanyDTO":
        employees = data.get("emails", [])
        processed_employees = [
            {
                "name": f"{email.get('first_name', '')} {email.get('last_name', '')}".strip(),
                "email": email.get("value"),
                "position": email.get("position"),
                "linkedin": email.get("linkedin"),
                "department": email.get("department"),
            }
            for email in employees
        ]

        social_links = []
        if data.get("linkedin"):
            social_links.append(SocialMediaLinks(platform="linkedin", url=data.get("linkedin")))
        if data.get("twitter"):
            social_links.append(SocialMediaLinks(platform="twitter", url=data.get("twitter")))
        if data.get("facebook"):
            social_links.append(SocialMediaLinks(platform="facebook", url=data.get("facebook")))
        if data.get("instagram"):
            social_links.append(SocialMediaLinks(platform="instagram", url=data.get("instagram")))
        if data.get("youtube"):
            social_links.append(SocialMediaLinks(platform="youtube", url=data.get("youtube")))

        return CompanyDTO(
            uuid=data.get("uuid", get_uuid4()),
            name=data.get("organization", ""),
            domain=data.get("domain", ""),
            address=f"{data.get('street', '')}, {data.get('city', '')}, {data.get('state', '')}, {data.get('postal_code', '')}".strip(
                ", "
            ),
            country=data.get("country", ""),
            logo=None,
            founded_year=None,
            size=data.get("headcount", ""),
            industry=data.get("industry", ""),
            description=data.get("description", ""),
            overview=None,
            challenges=None,
            technologies=data.get("technologies", []),
            employees=processed_employees,
            social_links=social_links if social_links else None,
            annual_revenue=None,
            total_funding=None,
            funding_rounds=None,
            news=None,
        )

    def to_tuple(self) -> tuple:
        return (
            self.uuid,
            self.name,
            self.domain,
            self.address,
            self.country,
            self.logo,
            self.founded_year,
            self.size,
            self.industry,
            self.description,
            self.overview,
            self.challenges,
            self.technologies,
            self.employees,
            self.social_links,
            self.annual_revenue,
            self.total_funding,
            self.funding_rounds,
            self.news,
        )

    @staticmethod
    def from_tuple(row: tuple) -> "CompanyDTO":
        return CompanyDTO(
            uuid=row[0],
            name=row[1],
            domain=row[2],
            address=row[3],
            country=row[4],
            logo=row[5],
            founded_year=row[6],
            size=row[7],
            industry=row[8],
            description=row[9],
            overview=row[10],
            challenges=row[11],
            technologies=row[12],
            employees=row[13],
            social_links=row[14],
            annual_revenue=row[15],
            total_funding=row[16],
            funding_rounds=row[17],
            news=row[18],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return CompanyDTO.from_dict(data)

    def __str__(self):
        return (
            f"CompanyDTO(uuid={self.uuid},\n name={self.name},\n domain={self.domain},\n address={self.address},\n "
            f"country={self.country},\n logo={self.logo},\n founded_year={self.founded_year},\n size={self.size},\n "
            f"industry={self.industry},\n description={self.description},\n overview={self.overview},\n challenges={self.challenges},\n "
            f"technologies={self.technologies},\n employees={self.employees},\n social_links={self.social_links},\n "
            f"annual_revenue={self.annual_revenue},\n total_funding={self.total_funding},\n funding_rounds={self.funding_rounds},\n "
            f"news={self.news})"
        )
