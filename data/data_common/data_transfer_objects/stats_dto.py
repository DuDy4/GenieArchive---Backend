from common.utils.str_utils import get_uuid4
from pydantic import BaseModel, Field, EmailStr, field_validator
from uuid import UUID
from datetime import datetime
from enum import Enum
from typing import Tuple, Dict, Any

class ActionEnum(str, Enum):
    VIEW = "VIEW"
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    LOGIN = "LOGIN"
    UPLOAD = "UPLOAD"

class EntityEnum(str, Enum):
    MEETING = "MEETING"
    PROFILE = "PROFILE"
    USER = "USER"
    FILE = "FILE"
    FILE_CATEGORY = "FILE_CATEGORY"

class StatsDTO(BaseModel):
    uuid: UUID = Field(default=get_uuid4())
    action: ActionEnum
    entity: EntityEnum
    entity_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    email: EmailStr
    tenant_id: str = Field(default="")
    user_id: str = Field(default="")

    @field_validator("entity_id", "email", "tenant_id", "user_id")
    def not_empty(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value

    def to_tuple(self) -> Tuple[str, str, str, str, datetime, str, str, str]:
        return (
            str(self.uuid),
            self.action.value,
            self.entity.value,
            self.entity_id,
            self.timestamp,
            self.email,
            self.tenant_id,
            self.user_id
        )

    @classmethod
    def from_tuple(cls, data: Tuple[UUID, str, str, str, datetime, str, str, str]) -> "StatsDTO":
        return cls(
            uuid=data[0],
            action=ActionEnum(data[1]),
            entity=EntityEnum(data[2]),
            entity_id=data[3],
            timestamp=data[4],
            email=data[5],
            tenant_id=data[6],
            user_id=data[7]
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uuid": str(self.uuid),
            "action": self.action.value,
            "entity": self.entity.value,
            "entity_id": self.entity_id,
            "timestamp": self.timestamp,
            "email": self.email,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StatsDTO":
        return StatsDTO(
            uuid=UUID(data.get("uuid")),
            action=ActionEnum(data.get("action")),
            entity=EntityEnum(data.get("entity")),
            entity_id=data.get("entity_id"),
            timestamp=data.get("timestamp"),
            email=data.get("email"),
            tenant_id=data.get("tenant_id"),
            user_id=data.get("user_id")
        )

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str) -> "StatsDTO":
        return cls.parse_raw(json_str)
