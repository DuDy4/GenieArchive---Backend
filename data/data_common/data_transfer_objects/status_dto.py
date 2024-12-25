from common.utils.str_utils import get_uuid4
from pydantic import BaseModel, Field, EmailStr, field_validator
from uuid import UUID
from datetime import datetime, timezone
from enum import Enum
from typing import Tuple, Dict, Any

class StatusEnum(str, Enum):
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class StatusDTO(BaseModel):
    person_uuid: UUID
    tenant_id: str
    current_event: str
    current_event_start_time: datetime
    status: StatusEnum

    @field_validator("person_uuid", "tenant_id")
    def not_empty(cls, value):
        if not str(value).strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value

    def to_tuple(self) -> Tuple[str, str, str, str, str]:
        return (
            str(self.person_uuid),
            self.tenant_id,
            self.current_event,
            self.current_event_start_time.isoformat(),
            self.status,
        )

    @classmethod
    def from_tuple(cls, data: Tuple[str, str, str, str, str]) -> "StatusDTO":
        return cls(
            person_uuid=UUID(data[0]),
            tenant_id=data[1],
            current_event=data[2],
            current_event_start_time=data[3].astimezone(timezone.utc) if isinstance(data[3], datetime) else datetime.fromisoformat(data[3]).astimezone(timezone.utc),
            status=StatusEnum(data[4]),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "person_uuid": str(self.person_uuid),
            "tenant_id": self.tenant_id,
            "current_event": self.current_event,
            "current_event_start_time": str(self.current_event_start_time),
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, data) -> "StatusDTO":
        return StatusDTO(
            person_uuid=UUID(data.get("person_uuid")),
            tenant_id=data.get("tenant_id"),
            current_event=data.get("current_event"),
            current_event_start_time=datetime.fromisoformat(data.get("current_event_start_time")),
            status=StatusEnum(data.get("status"))
        )

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str) -> "StatusDTO":
        return cls.parse_raw(json_str)
