from pydantic import BaseModel, Field, EmailStr, field_validator
from datetime import datetime, timezone
from enum import Enum
from typing import Tuple, Dict, Any, Optional

class StatusTypeEnum(str, Enum):
    EMAIL = "EMAIL"
    PROFILE = "PROFILE"
    PERSON = "PERSON"
    MEETING = "MEETING"
    COMPANY = "COMPANY"
    UNKNOWN = "UNKNOWN"

class StatusEnum(str, Enum):
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    STARTED = "STARTED"


class StatusDTO(BaseModel):
    ctx_id: str
    object_id: str
    object_type: Optional[str | StatusTypeEnum] = None
    user_id: str
    tenant_id: str
    event_topic: str
    previous_event_topic: Optional[str | None] = None
    current_event_start_time: datetime
    status: StatusEnum

    @field_validator("ctx_id", "object_id", "user_id")
    def not_empty(cls, value):
        if not str(value).strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value

    def to_tuple(self) -> Tuple[str, str, str, str, str, str, str | None, str, str]:
        return (
            self.ctx_id,
            self.object_id,
            self.object_type,
            self.user_id,
            self.tenant_id,
            self.event_topic,
            self.previous_event_topic,
            self.current_event_start_time.isoformat(),
            self.status,
        )

    @classmethod
    def from_tuple(cls, data: Tuple[str, str, str, str,str, str, str | None, str, str]) -> "StatusDTO":
        return cls(
            ctx_id=data[0],
            object_id=data[1],
            object_type=StatusTypeEnum(data[2]) or data[2],
            user_id=data[3],
            tenant_id=data[4],
            event_topic=data[5],
            previous_event_topic=data[6],
            current_event_start_time=datetime.fromisoformat(data[7]) if isinstance(data[7], str) else data[7],
            status=StatusEnum(data[8])
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ctx_id": self.ctx_id,
            "object_id": str(self.object_id),
            "object_type": self.object_type,
            "user_id": self.user_id,
            "tenant_id": self.tenant_id,
            "event_topic": self.event_topic,
            "previous_event_topic": self.previous_event_topic,
            "current_event_start_time": self.current_event_start_time.isoformat(),
            "status": self.status
        }

    @classmethod
    def from_dict(cls, data) -> "StatusDTO":
        return StatusDTO(
            ctx_id=data["ctx_id"],
            object_id=data["object_id"],
            object_type=StatusTypeEnum(data.get("object_type")) or data.get("object_type"),
            user_id=data["user_id"],
            tenant_id=data["tenant_id"],
            event_topic=data["event_topic"],
            previous_event_topic=data["previous_event_topic"],
            current_event_start_time=datetime.fromisoformat(data["current_event_start_time"]),
            status=StatusEnum(data["status"])
        )

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str) -> "StatusDTO":
        return cls.parse_raw(json_str)
