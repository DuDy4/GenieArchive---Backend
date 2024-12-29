from common.utils.str_utils import get_uuid4
from pydantic import BaseModel, Field, EmailStr, field_validator
from uuid import UUID
from datetime import datetime, timezone
from enum import Enum
from typing import Tuple, Dict, Any, Optional


class StatusEnum(str, Enum):
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    STARTED = "STARTED"


class StatusDTO(BaseModel):
    ctx_id: str
    object_uuid: UUID
    tenant_id: str
    event_topic: str
    previous_event_topic: Optional[str | None] = None
    current_event_start_time: datetime
    status: StatusEnum

    @field_validator("ctx_id", "object_uuid", "tenant_id")
    def not_empty(cls, value):
        if not str(value).strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value

    def to_tuple(self) -> Tuple[str, str, str, str, str | None, str, str]:
        return (
            self.ctx_id,
            str(self.object_uuid),
            self.tenant_id,
            self.event_topic,
            self.previous_event_topic,
            self.current_event_start_time.isoformat(),
            self.status,
        )

    @classmethod
    def from_tuple(cls, data: Tuple[str, str, str, str, str | None, str, str]) -> "StatusDTO":
        return cls(
            ctx_id=data[0],
            object_uuid=UUID(data[1]),
            tenant_id=data[2],
            event_topic=data[3],
            previous_event_topic=data[4],
            current_event_start_time=datetime.fromisoformat(data[5]) if isinstance(data[5], str) else data[5],
            status=StatusEnum(data[6])
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ctx_id": self.ctx_id,
            "object_uuid": str(self.object_uuid),
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
            object_uuid=UUID(data["object_uuid"]),
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
