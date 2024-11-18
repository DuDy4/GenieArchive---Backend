import json
from pydantic import BaseModel, Field, HttpUrl, field_validator
from typing import List, Optional, Dict, Tuple
from uuid import UUID
from data.data_common.utils.str_utils import to_custom_title_case


class Hobby(BaseModel):
    hobby_name: str
    icon_url: str

    @field_validator("hobby_name", "icon_url")
    def not_empty(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value

    @classmethod
    def from_json(cls, json_str: str) -> "Hobby":
        return cls.parse_raw(json_str)

    def to_json(self) -> str:
        return self.json()

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
        return {
            "phrase_text": str(self.phrase_text),
            "reasoning": str(self.reasoning),
            "confidence_score": int(self.confidence_score),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "Phrase":
        return cls(**data)


class ProfileCategory(BaseModel):
    category: str
    scores: dict

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
    def from_json(cls, json_str: str) -> "Strength":
        return cls.parse_raw(json_str)

    def to_json(self) -> str:
        return self.json()

    @classmethod
    def from_dict(cls, data: dict) -> "Strength":
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
    def from_json(cls, json_str: str) -> "Connection":
        return cls.parse_raw(json_str)

    def to_json(self) -> str:
        return self.json()

    @classmethod
    def from_dict(cls, data: dict) -> "Connection":
        return cls.parse_obj(data)

    def to_tuple(self) -> Tuple[str, Optional[HttpUrl], Optional[HttpUrl]]:
        return self.name, self.image_url, self.linkedin_url

    @classmethod
    def from_tuple(cls, data: Tuple[str, Optional[HttpUrl], Optional[HttpUrl]]) -> "Connection":
        return cls(name=data[0], image_url=data[1], linkedin_url=data[2])

    def to_dict(self) -> Dict[str, any]:
        return {
            "name": str(self.name),
            "image_url": str(self.image_url) if self.image_url else None,
            "linkedin_url": str(self.linkedin_url) if self.linkedin_url else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "Connection":
        return cls(**data)


class ProfileDTO(BaseModel):
    uuid: UUID
    name: str
    company: str
    position: Optional[str] = None
    summary: Optional[str] = None
    picture_url: Optional[HttpUrl] = None
    get_to_know: Optional[Dict[str, List[Phrase]]] = Field(
        {"avoid": [], "best_practices": [], "phrases_to_use": []}
    )
    connections: Optional[List[Connection]] = []
    strengths: Optional[List[Strength]] = []
    hobbies: Optional[List[UUID]] = []
    work_history_summary: Optional[str] = None

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
        profile_dict = {
            "uuid": str(self.uuid),
            "name": self.name,
            "company": self.company,
            "position": self.position,
            "summary": self.summary,
            "picture_url": self.picture_url,
            "get_to_know": {
                key: [phrase.to_dict() for phrase in phrases] for key, phrases in self.get_to_know.items()
            },
            "connections": [connection.to_dict() for connection in self.connections],
            "strengths": [strength.to_dict() for strength in self.strengths],
            "hobbies": [str(hobby) for hobby in self.hobbies],
            "work_history_summary": self.work_history_summary,
        }
        return profile_dict

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
        Optional[List[Connection]],
        Optional[List[Strength]],
        Optional[List[UUID]],
        Optional[str],
    ]:
        return (
            self.uuid,
            self.name,
            self.company,
            self.position,
            self.summary,
            self.picture_url,
            self.get_to_know,
            self.connections,
            self.strengths,
            self.hobbies,
            self.work_history_summary,
        )

    @classmethod
    def from_tuple(
        cls,
        data: Tuple[
            UUID,
            str,
            str,
            str,
            Optional[str],
            Optional[HttpUrl],
            Optional[Dict[str, List[Phrase]]],
            Optional[List[Connection]],
            Optional[List[Strength]],
            Optional[List[UUID]],
            Optional[str],
        ],
    ) -> "ProfileDTO":
        return cls(
            uuid=data[0],
            name=to_custom_title_case(data[1]),
            company=to_custom_title_case(data[2]),
            position=to_custom_title_case(data[3]),
            summary=data[4],
            picture_url=data[5],
            get_to_know=data[6],
            connections=data[7],
            strengths=data[8],
            hobbies=data[9],
            work_history_summary=data[10],
        )
