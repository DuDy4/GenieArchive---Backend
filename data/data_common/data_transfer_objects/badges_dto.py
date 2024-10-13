from pydantic import BaseModel, EmailStr
from uuid import UUID
from enum import Enum
from datetime import datetime
from typing import Tuple, Dict, Any

class BadgesEventTypes(str, Enum):
    VIEW_PROFILE = "VIEW_PROFILE"
    VIEW_MEETING = "VIEW_MEETING"
    LOGIN_USER = "LOGIN_USER"


class BadgeDTO(BaseModel):
    badge_id: UUID
    name: str
    description: str
    criteria: Dict[str, Any]  # Assuming criteria is a dictionary that defines the badge's requirements
    icon_url: str
    created_at: datetime
    last_updated: datetime

    def to_tuple(self) -> Tuple[UUID, str, str, Dict[str, Any], str, datetime, datetime]:
        return (
            str(self.badge_id),
            self.name,
            self.description,
            self.criteria,
            self.icon_url,
            self.created_at,
            self.last_updated,
        )

    @classmethod
    def from_tuple(cls, data: Tuple[UUID, str, str, Dict[str, Any], str, datetime, datetime]) -> "BadgeDTO":
        return cls(
            badge_id=data[0],
            name=data[1],
            description=data[2],
            criteria=data[3],
            icon_url=data[4],
            created_at=data[5],
            last_updated=data[6],
        )

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BadgeDTO":
        return cls(**data)



class UserBadgeDTO(BaseModel):
    user_badge_id: UUID
    email: EmailStr
    badge_id: UUID
    earned_at: datetime

    def to_tuple(self) -> Tuple[UUID, EmailStr, UUID, datetime]:
        return (
            str(self.user_badge_id),
            self.email,
            str(self.badge_id),
            self.earned_at,
        )

    @classmethod
    def from_tuple(cls, data: Tuple[UUID, EmailStr, UUID, datetime]) -> "UserBadgeDTO":
        return cls(
            user_badge_id=data[0],
            email=data[1],
            badge_id=data[2],
            earned_at=data[3],
        )

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserBadgeDTO":
        return cls(**data)



class UserBadgeProgressDTO(BaseModel):
    email: EmailStr
    badge_id: UUID
    progress: Dict[str, Any]  
    last_updated: datetime

    def to_tuple(self) -> Tuple[EmailStr, UUID, Dict[str, Any], datetime]:
        return (
            self.email,
            str(self.badge_id),
            self.progress,
            self.last_updated,
        )

    @classmethod
    def from_tuple(cls, data: Tuple[EmailStr, UUID, Dict[str, Any], datetime]) -> "UserBadgeProgressDTO":
        return cls(
            email=data[0],
            badge_id=data[1],
            progress=data[2],
            last_updated=data[3],
        )

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserBadgeProgressDTO":
        return cls(**data)
    
class DetailedUserBadgeProgressDTO(BaseModel):
    email: EmailStr
    badge_id: UUID
    progress: Dict[str, Any]  
    last_updated: datetime
    badge_name: str
    badge_description: str
    badge_icon_url: str
    criteria: Dict[str, Any]

    def to_tuple(self) -> Tuple[EmailStr, UUID, Dict[str, Any], datetime, str, str, str, Dict[str, Any]]:
        return (
            self.email,
            str(self.badge_id),
            self.progress,
            self.last_updated,
            self.badge_name,
            self.badge_description,
            self.badge_icon_url,
            self.criteria,
        )

    @classmethod
    def from_tuple(cls, data: Tuple[EmailStr, UUID, Dict[str, Any], datetime, str, str, str, Dict[str, Any]]) -> "DetailedUserBadgeProgressDTO":
        return cls(
            email=data[0] if data[0] else "",
            badge_id=data[1],
            progress=data[2] if data[2] else {},
            last_updated=data[3] if data[3] else datetime.now(),
            badge_name=data[4],
            badge_description=data[5],
            badge_icon_url=data[6],
            criteria=data[7],
        )

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DetailedUserBadgeProgressDTO":
        return cls(**data)
