import json
from typing import List, Dict, Optional, Union, Tuple, Any


from pydantic import HttpUrl, field_validator, BaseModel
from datetime import date

from data.data_common.utils.str_utils import (
    titleize_values,
    to_custom_title_case,
    get_uuid4,
)
from common.genie_logger import GenieLogger

logger = GenieLogger()


class NewsData(BaseModel):
    date: Optional[date]
    link: HttpUrl
    media: str
    title: str
    summary: Optional[str]

    @field_validator("media", "title", "link")
    def not_empty(cls, value):
        # Convert HttpUrl to str if needed, otherwise ensure it's a string
        value_to_check = str(value)

        # Check if the value is empty or whitespace
        if not value_to_check.strip():
            raise ValueError("Field cannot be empty or whitespace")

        return value

    @classmethod
    def from_json(cls, json_str: str) -> "NewsData":
        return cls.parse_raw(json_str)

    def to_json(self) -> str:
        return self.json()

    def to_tuple(self) -> Tuple[Optional[date], HttpUrl, str, str, Optional[str]]:
        return self.date, self.link, self.media, self.title, self.summary

    @classmethod
    def from_tuple(cls, data: Tuple[Optional[date], HttpUrl, str, str, Optional[str]]) -> "NewsData":
        return cls(date=data[0], link=data[1], media=data[2], title=data[3], summary=data[4])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date.isoformat(),
            "link": str(self.link),
            "media": self.media,
            "title": self.title,
            "summary": self.summary,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "NewsData":
        if "date" in data:
            data["date"] = date.fromisoformat(data["date"])
        if "link" in data:
            data["link"] = HttpUrl(data["link"])
        return cls(**data)


class CompanyDTO:
    def __init__(
        self,
        uuid: str,
        name: str,
        domain: str,
        size: Optional[str],
        description: Optional[str],
        overview: Optional[str],
        challenges: Optional[Union[Dict, List[Dict]]],
        technologies: Optional[Union[Dict, List[Dict], List[str]]],
        employees: Optional[Union[Dict, List[Dict]]],
        news: Optional[List[NewsData]] = [],
    ):
        self.uuid = uuid
        self.name = name
        self.domain = domain
        self.size = size
        self.description = description
        self.overview = overview
        self.challenges = challenges
        self.technologies = technologies
        self.employees = employees
        self.news = news

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "name": to_custom_title_case(self.name),
            "domain": self.domain,
            "size": self.size,
            "description": self.description,
            "overview": self.overview,
            "challenges": self.challenges,
            "technologies": self.technologies,
            "employees": self.employees,
            "news": [news_item.to_dict() for news_item in self.news if not isinstance(news_item, dict)]
            if self.news
            else None,
        }

    @staticmethod
    def from_dict(data: dict):
        return CompanyDTO(
            uuid=data.get("uuid", ""),
            name=data.get("name", ""),
            domain=data.get("domain", ""),
            size=data.get("size", None),
            description=data.get("description", None),
            overview=data.get("overview", None),
            challenges=data.get("challenges", None),
            technologies=data.get("technologies", None),
            employees=data.get("employees", None),
            news=[NewsData.from_dict(item) for item in data.get("news", [])] if data.get("news") else None,
        )

    @staticmethod
    def from_hunter_object(data: dict) -> "CompanyDTO":
        employees = data.get("emails") or data.get("employees") or []
        processed_employees = [
            {
                "name": f"{email.get('first_name', '')} {email.get('last_name', '')}",
                "email": email.get("value"),
                "position": email.get("position"),
                "linkedin": email.get("linkedin"),
                "department": email.get("department"),
            }
            for email in employees
        ]

        return CompanyDTO(
            uuid=data.get("uuid", get_uuid4()),
            name=data.get("organization", ""),
            domain=data.get("domain", ""),
            size=data.get("headcount", ""),
            description=data.get("description", ""),
            overview=data.get("overview", ""),
            challenges=data.get("challenges", {}),
            technologies=data.get("technologies", []),
            employees=processed_employees,
            news=[NewsData.from_dict(item) for item in data.get("news", [])] if data.get("news") else None,
        )

    def to_tuple(self) -> tuple:
        return (
            self.uuid,
            self.name,
            self.domain,
            self.size,
            self.description,
            self.overview,
            self.challenges,
            self.technologies,
            self.employees,
            self.news,
        )

    @staticmethod
    def from_tuple(row: tuple) -> "CompanyDTO":
        return CompanyDTO(
            uuid=row[0],
            name=row[1],
            domain=row[2],
            size=row[3],
            description=row[4],
            overview=row[5],
            challenges=row[6],
            technologies=row[7],
            employees=row[8],
            news=row[9],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return CompanyDTO.from_dict(data)

    def __str__(self):
        return (
            f"CompanyDTO(uuid={self.uuid},\n name={self.name},\n domain={self.domain},\n size={self.size},\n "
            f"description={self.description},\n overview={self.overview},\n challenges={self.challenges},\n "
            f"technologies={self.technologies},\n employees={self.employees},\n news={self.news})"
        )
