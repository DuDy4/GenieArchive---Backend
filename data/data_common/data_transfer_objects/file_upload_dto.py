from pydantic import BaseModel, Field, EmailStr, field_validator
from uuid import UUID
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional
import hashlib

from common.utils.str_utils import get_uuid4
from enum import Enum


class FileStatusEnum(str, Enum):
    UPLOADED = "UPLOADED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class FileCategoryEnum(str, Enum):
    WHITEPAPER = "WHITEPAPER"
    CASE_STUDY = "CASE_STUDY"
    COMPETITOR_ANALYSIS = "COMPETITOR_ANALYSIS"
    FAQ = "FAQ"
    OTHER = "OTHER"

    @classmethod
    def get_all_categories(cls) -> List[str]:
        return [category.value for category in cls]


class FileUploadDTO(BaseModel):
    uuid: UUID
    file_name: str
    file_hash: Optional[str] = None  # Optional file hash field
    upload_timestamp: datetime  # Readable timestamp
    upload_time_epoch: int  # Epoch time in seconds
    email: EmailStr
    tenant_id: str
    user_id: str
    status: FileStatusEnum
    categories: List[FileCategoryEnum] = []

    @field_validator("file_name", "email", "tenant_id", "user_id")
    def not_empty(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value

    def to_tuple(self) -> Tuple[str, str, Optional[str], datetime, int, str, str, str, str, List[str]]:
        return (
            str(self.uuid),
            self.file_name,
            self.file_hash,
            self.upload_timestamp,
            self.upload_time_epoch,
            self.email,
            self.tenant_id,
            self.user_id,
            str(self.status),
            self.categories,
        )

    @classmethod
    def from_tuple(
        cls, data: Tuple[UUID, str, Optional[str], datetime, int, str, str, str, List[str]]
    ) -> "FileUploadDTO":
        return cls(
            uuid=data[0],
            file_name=data[1],
            file_hash=data[2],
            upload_timestamp=data[3],
            upload_time_epoch=data[4],
            email=data[5],
            tenant_id=data[6],
            user_id=data[7],
            status=FileStatusEnum(data[8]),
            categories=data[9] if data[9] else [],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uuid": str(self.uuid),
            "file_name": self.file_name,
            "file_hash": self.file_hash,
            "upload_timestamp": datetime.isoformat(self.upload_timestamp),
            "upload_time_epoch": self.upload_time_epoch,
            "email": self.email,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "status": self.status,
            "categories": self.categories if self.categories else [],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FileUploadDTO":
        return FileUploadDTO(
            uuid=UUID(data.get("uuid")),
            file_name=data.get("file_name"),
            file_hash=data.get("file_hash"),
            upload_timestamp=datetime.fromisoformat(data.get("upload_timestamp")),
            upload_time_epoch=data.get("upload_time_epoch"),
            email=data.get("email"),
            tenant_id=data.get("tenant_id"),
            user_id=data.get("user_id"),
            status=FileStatusEnum(data.get("status")),
            categories=data.get("categories") if data.get("categories") else [],
        )

    @staticmethod
    def from_file(
        file_name: str,
        file_content: Optional[str],
        email: EmailStr,
        tenant_id: str,
        user_id: str,
        upload_time=datetime.now(),
    ) -> "FileUploadDTO":
        file_hash = FileUploadDTO.calculate_hash(file_content) if file_content else None
        upload_timestamp = upload_time or datetime.now()
        return FileUploadDTO(
            uuid=UUID(get_uuid4()),
            file_name=file_name,
            file_hash=file_hash,
            upload_timestamp=upload_timestamp,
            upload_time_epoch=int(upload_timestamp.timestamp()),
            email=email,
            tenant_id=tenant_id,
            user_id=user_id,
            status=FileStatusEnum.UPLOADED,
            categories=[],
        )

    def update_file_content(self, file_content: str) -> None:
        self.file_hash = FileUploadDTO.calculate_hash(file_content)

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str) -> "FileUploadDTO":
        return cls.parse_raw(json_str)

    @staticmethod
    def calculate_hash(file_content: str) -> str:
        """
        Calculate the SHA-256 hash of the given file content.

        :param file_content: The content of the file as a string
        :return: The SHA-256 hash of the file content
        """
        # Convert the string to bytes and compute the hash
        return hashlib.sha256(file_content.encode("utf-8")).hexdigest()
