import json
from pydantic import BaseModel, Field, HttpUrl, field_validator
from typing import List, Optional, Dict, Tuple
from uuid import UUID
from datetime import date

class Hobby(BaseModel):
    hobby_name: str
    icon_url: str

    @field_validator("hobby_name", "icon_url")
    def not_empty(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Hobby':
        return cls.parse_raw(json_str)
    
    def to_json(self) -> str:
        return self.json()
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Hobby':
        return cls.parse_obj(data)

    def to_tuple(self) -> Tuple[str, str]:
        return self.hobby_name, self.icon_url

    @classmethod
    def from_tuple(cls, data: Tuple[str, str]) -> "Hobby":
        return cls(hobby_name=data[0], icon_url=data[1])

    def to_dict(self) -> Dict[str, any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "Hobby":
        return cls(**data)


class Phrase(BaseModel):
    phrase_text: str
    reasoning: str
    confidence_score: int = Field(..., ge=0, le=100)

    @field_validator("phrase_text", "reasoning")
    def not_empty(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value

    def to_tuple(self) -> Tuple[str, str, int]:
        return self.phrase_text, self.reasoning, self.confidence_score

    @classmethod
    def from_tuple(cls, data: Tuple[str, str, int]) -> "Phrase":
        return cls(phrase_text=data[0], reasoning=data[1], confidence_score=data[2])

    def to_dict(self) -> Dict[str, any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "Phrase":
        return cls(**data)


class Strength(BaseModel):
    strength_name: str
    reasoning: str
    score: int = Field(..., ge=0, le=100)

    @field_validator("strength_name", "reasoning")
    def not_empty(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Strength':
        return cls.parse_raw(json_str)
    
    def to_json(self) -> str:
        return self.json()
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Strength':
        return cls.parse_obj(data)

    def to_tuple(self) -> Tuple[str, str, int]:
        return self.strength_name, self.reasoning, self.score

    @classmethod
    def from_tuple(cls, data: Tuple[str, str, int]) -> "Strength":
        return cls(strength_name=data[0], reasoning=data[1], score=data[2])

    def to_dict(self) -> Dict[str, any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "Strength":
        return cls(**data)


class NewsData(BaseModel):
    date: Optional[date] = None
    link: HttpUrl
    media: str
    title: str
    summary: Optional[str] = None

    @field_validator('media', 'title')
    def not_empty(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value
    
    @classmethod
    def from_json(cls, json_str: str) -> 'NewsData':
        return cls.parse_raw(json_str)
    

    def to_json(self) -> str:
        return self.json()
    
    @classmethod
    def from_dict(cls, data: dict) -> 'NewsData':
        return cls.parse_obj(data)

    def to_tuple(self) -> Tuple[date, HttpUrl, str, str, str]:
        return self.date, self.link, self.media, self.title, self.summary

    @classmethod
    def from_tuple(cls, data: Tuple[date, HttpUrl, str, str, str]) -> "NewsData":
        return cls(
            date=data[0], link=data[1], media=data[2], title=data[3], summary=data[4]
        )

    def to_dict(self) -> Dict[str, any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "NewsData":
        return cls(**data)


class Connection(BaseModel):
    name: str
    image_url: Optional[HttpUrl] = None
    linkedin_url: Optional[HttpUrl] = None

    @field_validator("name")
    def not_empty(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Connection':
        return cls.parse_raw(json_str)
    
    def to_json(self) -> str:
        return self.json()
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Connection':
        return cls.parse_obj(data)

    def to_tuple(self) -> Tuple[str, Optional[HttpUrl], Optional[HttpUrl]]:
        return self.name, self.image_url, self.linkedin_url

    @classmethod
    def from_tuple(
        cls, data: Tuple[str, Optional[HttpUrl], Optional[HttpUrl]]
    ) -> "Connection":
        return cls(name=data[0], image_url=data[1], linkedin_url=data[2])

    def to_dict(self) -> Dict[str, any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "Connection":
        return cls(**data)


class ProfileDTO(BaseModel):
    uuid: UUID
    name: str
    company: str
    position: str
    summary: Optional[str] = None
    picture_url: Optional[HttpUrl] = None
    get_to_know: Optional[Dict[str, List[Phrase]]] = Field(
        {"avoid": [], "best_practices": [], "phrases_to_use": []}
    )
    news: Optional[List[NewsData]] = []
    connections: Optional[List[Connection]] = []
    strengths: Optional[List[Strength]] = []
    hobbies: Optional[List[UUID]] = []

    @field_validator("name", "position")
    def not_empty(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value

    @field_validator("get_to_know")
    def validate_phrases(cls, value):
        for key in ["avoid", "best_practices", "phrases_to_use"]:
            if key not in value:
                raise ValueError(f"{key} must be present in get_to_know")
            if not isinstance(value[key], list):
                raise ValueError(f"{key} must be a list")
            for item in value[key]:
                if not isinstance(item, Phrase):
                    raise ValueError(f"All items in {key} must be of type Phrase")
        return value

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str) -> "ProfileDTO":
        return cls.parse_raw(json_str)

    def to_dict(self) -> dict:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: dict) -> "ProfileDTO":
        return cls.parse_obj(data)

    def to_tuple(
        self,
    ) -> Tuple[
        UUID,
        str,
        str,
        str,
        Optional[str],
        Optional[HttpUrl],
        Optional[Dict[str, List[Phrase]]],
        Optional[List[NewsData]],
        Optional[List[Connection]],
        Optional[List[Strength]],
        Optional[List[UUID]],
    ]:
        return (
            self.uuid,
            self.name,
            self.company,
            self.position,
            self.summary,
            self.picture_url,
            self.get_to_know,
            self.news,
            self.connections,
            self.strengths,
            self.hobbies,
        )

    @classmethod
    def from_tuple(
        cls,
        data: Tuple[
            UUID,
            str,
            str,
            Optional[str],
            Optional[HttpUrl],
            Optional[Dict[str, List[Phrase]]],
            Optional[List[NewsData]],
            Optional[List[Connection]],
            Optional[List[Strength]],
            Optional[List[UUID]],
        ],
    ) -> "ProfileDTO":
        return cls(
            uuid=data[0],
            name=data[1],
            position=data[2],
            summary=data[3],
            picture_url=data[4],
            get_to_know=data[5],
            news=data[6],
            connections=data[7],
            strengths=data[8],
            hobbies=data[9]
        )



# import json
# from typing import List, Dict
# from data.data_common.utils.str_utils import titleize_values, to_custom_title_case


# class ProfileDTO:
#     def __init__(
#         self,
#         uuid: str,
#         name: str,
#         company: str,
#         position: str,
#         challenges: List[Dict],
#         strengths: List[Dict],
#         hobbies: List[str],
#         connections: List[str],
#         news: List[str],
#         get_to_know: Dict[str, Dict | str],
#         summary: str,
#         picture_url: str,
#     ):
#         self.uuid = uuid
#         self.name = name
#         self.company = company
#         self.position = position
#         self.challenges = challenges
#         self.strengths = strengths
#         self.hobbies = hobbies
#         self.connections = connections
#         self.news = news
#         self.get_to_know = get_to_know
#         self.summary = summary
#         self.picture_url = picture_url

#     def to_dict(self):
#         return {
#             "uuid": self.uuid,
#             "name": to_custom_title_case(self.name),
#             "company": to_custom_title_case(self.company),
#             "position": to_custom_title_case(self.position),
#             "challenges": titleize_values(self.challenges),
#             "strengths": self.strengths,
#             "hobbies": titleize_values(self.hobbies),
#             "connections": to_custom_title_case(self.connections),
#             "news": titleize_values(self.news),
#             "get_to_know": self.get_to_know,
#             "summary": titleize_values(self.summary),
#             "picture_url": self.picture_url,
#         }

#     @staticmethod
#     def from_dict(data: dict):
#         return ProfileDTO(
#             uuid=data.get("uuid", ""),
#             name=data.get("name", ""),
#             company=data.get("company", ""),
#             position=data.get("position", ""),
#             challenges=data.get("challenges", []),
#             strengths=data.get("strengths", []),
#             hobbies=data.get("hobbies", []),
#             connections=data.get("connections", []),
#             news=data.get("news", []),
#             get_to_know=data.get("get_to_know", {}),
#             summary=data.get("summary", ""),
#             picture_url=data.get("picture_url", ""),
#         )

#     def to_tuple(self) -> tuple:
#         return (
#             self.uuid,
#             self.name,
#             self.company,
#             self.position,
#             self.challenges,
#             self.strengths,
#             self.hobbies,
#             self.connections,
#             self.news,
#             self.get_to_know,
#             self.summary,
#             self.picture_url,
#         )

#     @staticmethod
#     def from_tuple(row: tuple) -> "ProfileDTO":
#         return ProfileDTO(
#             uuid=row[0],
#             name=row[1],
#             company=row[2],
#             position=row[3],
#             challenges=row[4],
#             strengths=row[5],
#             hobbies=row[6],
#             connections=row[7],
#             news=row[8],
#             get_to_know=row[9],
#             summary=row[10],
#             picture_url=row[11],
#         )

#     def to_json(self):
#         return json.dumps(self.to_dict())

#     @staticmethod
#     def from_json(json_str: str):
#         data = json.loads(json_str)
#         return ProfileDTO.from_dict(data)

#     def __str__(self):
#         return (
#             f"ProfileDTO(uuid={self.uuid}, name={self.name}, company={self.company}, "
#             f"position={self.position}, challenges={self.challenges}, strengths={self.strengths}, "
#             f"hobbies={self.hobbies}, connections={self.connections}, news={self.news}, "
#             f"get_to_know={self.get_to_know}, summary={self.summary}, picture_url={self.picture_url})"
#         )

