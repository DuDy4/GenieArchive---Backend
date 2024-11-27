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


class ProfileCategory(BaseModel):
    category: str
    scores: dict
    description: str
    extended_description: Optional[str] = None
    explanation: Optional[ProfileCategoryExplanation] = None
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
            "criteria": str(self.criteria.value),
            "score": int(self.score),
            "target_score": int(self.target_score),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "SalesCriteria":
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str) -> "SalesCriteria":
        return cls.parse_raw(json_str)


class ActionItem(BaseModel):
    icon: str
    title: str
    description: str
    percentage: str
    criteria: SalesCriteriaType

    @staticmethod
    def from_dict(data: dict) -> "ActionItem":
        return ActionItem(
            icon=data["icon"],
            title=data["title"],
            description=data["description"],
            percentage=data["percentage"],
            criteria=SalesCriteriaType(data["criteria"]),
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            "icon": self.icon,
            "title": self.title,
            "description": self.description,
            "percentage": self.percentage,
            "criteria": self.criteria.value,
        }

    @classmethod
    def from_tuple(cls, data: tuple) -> "ActionItem":
        return cls(icon=data[0], title=data[1], description=data[2], percentage=data[3], criteria=SalesCriteriaType(data[4]))

    def to_tuple(self) -> tuple:
        return self.icon, self.title, self.description, self.percentage, self.criteria.value


