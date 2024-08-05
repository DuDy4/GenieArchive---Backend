
from pydantic import BaseModel, Field, HttpUrl, validator
from typing import List, Optional, Dict, Tuple
from uuid import UUID
from datetime import date

class Phrase(BaseModel):
    phrase_text: str
    reasoning: str
    confidence_score: int = Field(..., ge=0, le=100)

    @validator('phrase_text', 'reasoning')
    def not_empty(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value

    def to_tuple(self) -> Tuple[str, str, int]:
        return self.phrase_text, self.reasoning, self.confidence_score

    @classmethod
    def from_tuple(cls, data: Tuple[str, str, int]) -> 'Phrase':
        return cls(phrase_text=data[0], reasoning=data[1], confidence_score=data[2])

class Strength(BaseModel):
    strength_name: str
    reasoning: str
    score: int = Field(..., ge=0, le=100)

    @validator('strength_name', 'reasoning')
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
    def from_tuple(cls, data: Tuple[str, str, int]) -> 'Strength':
        return cls(strength_name=data[0], reasoning=data[1], score=data[2])

class NewsData(BaseModel):
    date: Optional[date] = None
    link: HttpUrl
    media: str
    title: str
    summary: Optional[str] = None

    @validator('media', 'title')
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
    def from_tuple(cls, data: Tuple[date, HttpUrl, str, str, str]) -> 'NewsData':
        return cls(date=data[0], link=data[1], media=data[2], title=data[3], summary=data[4])

class Connection(BaseModel):
    name: str
    image_url: Optional[HttpUrl] = None
    linkedin_url: Optional[HttpUrl] = None

    @validator('name')
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
    def from_tuple(cls, data: Tuple[str, Optional[HttpUrl], Optional[HttpUrl]]) -> 'Connection':
        return cls(name=data[0], image_url=data[1], linkedin_url=data[2])

class ProfileDTO(BaseModel):
    uuid: UUID
    name: str
    position: str
    summary: Optional[str] = None
    picture_url: Optional[HttpUrl] = None
    get_to_know: Optional[Dict[str, List[Phrase]]] = Field({
        "avoid": [],
        "best_practices": [],
        "phrases_to_use": []
    })
    news: Optional[List[NewsData]] = []
    connections: Optional[List[Connection]] = []
    strengths: Optional[List[Strength]] = []
    hobbies: Optional[List[UUID]] = []

    @validator('name', 'position')
    def not_empty(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value

    @validator('get_to_know')
    def validate_phrases(cls, value):
        for key in ['avoid', 'best_practices', 'phrases_to_use']:
            if key not in value:
                raise ValueError(f"{key} must be present in get_to_know")
            if not isinstance(value[key], list):
                raise ValueError(f"{key} must be a list")
            for item in value[key]:
                if not isinstance(item, Phrase):
                    raise ValueError(f"All items in {key} must be of type Phrase")
        return value

    def to_json(self) -> str:
        return self.json()

    @classmethod
    def from_json(cls, json_str: str) -> 'ProfileDTO':
        return cls.parse_raw(json_str)

    def to_dict(self) -> dict:
        return self.dict()

    @classmethod
    def from_dict(cls, data: dict) -> 'ProfileDTO':
        return cls.parse_obj(data)

    def to_tuple(self) -> Tuple[UUID, str, str, Optional[str], Optional[HttpUrl], Optional[Dict[str, List[Phrase]]], Optional[List[NewsData]], Optional[List[Connection]], Optional[List[Strength]], Optional[List[UUID]]]:
        return (
            self.uuid,
            self.name,
            self.position,
            self.summary,
            self.picture_url,
            self.get_to_know,
            self.news,
            self.connections,
            self.strengths,
            self.hobbies
        )

    @classmethod
    def from_tuple(cls, data: Tuple[UUID, str, str, Optional[str], Optional[HttpUrl], Optional[Dict[str, List[Phrase]]], Optional[List[NewsData]], Optional[List[Connection]], Optional[List[Strength]], Optional[List[UUID]]]) -> 'ProfileDTO':
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

