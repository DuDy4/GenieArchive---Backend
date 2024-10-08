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

class EntityEnum(str, Enum):
    MEETING = "MEETING"
    PROFILE = "PROFILE"
    USER = "USER"

class StatsDTO(BaseModel):
    uuid: UUID
    action: ActionEnum
    entity: EntityEnum
    entity_id: str
    timestamp: datetime
    email: EmailStr
    tenant_id: str

    @field_validator("entity_id", "email", "tenant_id")
    def not_empty(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value

    def to_tuple(self) -> Tuple[str, str, str, str, datetime, str, str]:
        return (
            str(self.uuid),
            self.action.value,
            self.entity.value,
            self.entity_id,
            self.timestamp,
            self.email,
            self.tenant_id,
        )

    @classmethod
    def from_tuple(cls, data: Tuple[UUID, str, str, str, datetime, str, str]) -> "StatsDTO":
        return cls(
            uuid=data[0],
            action=ActionEnum(data[1]),
            entity=EntityEnum(data[2]),
            entity_id=data[3],
            timestamp=data[4],
            email=data[5],
            tenant_id=data[6],
        )

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StatsDTO":
        return cls(**data)

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str) -> "StatsDTO":
        return cls.parse_raw(json_str)
