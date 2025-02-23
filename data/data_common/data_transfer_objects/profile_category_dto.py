from enum import Enum
from typing import Optional, Dict

from pydantic import BaseModel, Field, HttpUrl, field_validator

from common.utils import env_utils


class SalesCriteriaType(str, Enum):
    BUDGET = "BUDGET"
    TRUST = "TRUST"
    TECHNICAL_FIT = "TECHNICAL_FIT"
    BUSINESS_FIT = "BUSINESS_FIT"
    VALUE_PROPOSITION = "VALUE_PROPOSITION"
    INNOVATION = "INNOVATION"
    REPUTATION = "REPUTATION"
    LONG_TERM_PROFESSIONAL_ADVISOR = "LONG_TERM_PROFESSIONAL_ADVISOR"
    RESPONSIVENESS = "RESPONSIVENESS"


class ProfileCategoryExplanation(BaseModel):
    characteristics: str
    needs: str
    recommendations: str

    @staticmethod
    def from_dict(data: dict) -> "ProfileCategoryExplanation":
        return ProfileCategoryExplanation(
            characteristics=data.get("characteristics"),
            needs=data.get("needs"),
            recommendations=data.get("recommendations"),
        )

class ProfileCategoryReasoning(BaseModel):
    text: str
    param: str
    reasoning: str

    @staticmethod
    def from_dict(data: dict) -> "ProfileCategoryReasoning":
        return ProfileCategoryReasoning(
            text=data.get("text"),
            param=data.get("param"),
            reasoning=data.get("reasoning"),
        )
    
    
    def to_dict(self) -> Dict[str, str | int]:
        return {
            "text": str(self.text),
            "param": str(self.param),
            "reasoning": str(self.reasoning),
        }

class ProfileCategory(BaseModel):
    category: str
    scores: dict
    description: str
    extended_description: Optional[str] = None
    explanation: Optional[ProfileCategoryExplanation] = None
    reasoning: Optional[list[ProfileCategoryReasoning]] = None
    icon: HttpUrl | None = '/images/image9.png'
    color: str = "#000000"
    font_color: str = "#FFFFFF"

    @staticmethod
    def from_dict(data: dict) -> "ProfileCategory":
        return ProfileCategory(
            category=data["category"],
            scores=data["scores"],
            description=data["description"],
            extended_description=data.get("extended_description"),
            explanation=ProfileCategoryExplanation.from_dict(data["explanation"]) if data.get("explanation") else None,
            reasoning=[ProfileCategoryReasoning.from_dict(reasoning) for reasoning in data["reasoning"]] if data.get("reasoning") else None,
            icon=env_utils.get("BLOB_FRONTEND_PROFILE_CATEGORY_URL", '/images/image9.png') +
                 (f"{'-'.join(data["category"].lower().split(' '))}.png" if data.get("category") else ''),
            color=data.get("color", "#000000"),
            font_color=data.get("font_color", "#FFFFFF"),
        )

class SalesCriteria(BaseModel):
    criteria: SalesCriteriaType
    score: int = Field(0, ge=0, le=100)
    target_score: int = Field(100, ge=0, le=100)

    @field_validator("criteria")
    def validate_criteria(cls, value):
        if value not in SalesCriteriaType:
            raise ValueError(f"Invalid criteria: {value}")
        return value

    def to_dict(self) -> Dict[str, str | int]:
        return {
            "criteria": str(self.criteria.value).upper(),
            "score": int(self.score),
            "target_score": int(self.target_score),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "SalesCriteria":
        if data.get("criteria") and isinstance(data["criteria"], str):
            data["criteria"] = SalesCriteriaType(data["criteria"].upper())
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str) -> "SalesCriteria":
        return cls.parse_raw(json_str)

