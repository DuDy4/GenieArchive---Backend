from pydantic import BaseModel, Field, EmailStr, field_validator
from uuid import UUID
from datetime import datetime
from typing import Tuple, Dict, Any
import hashlib


class FileUploadDTO(BaseModel):
    uuid: UUID
    file_name: str
    file_hash: str
    upload_time: datetime
    email: EmailStr
    tenant_id: str

    @field_validator("file_name", "email", "tenant_id")
    def not_empty(cls, value):
        if not value.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return value

    def to_tuple(self) -> Tuple[str, str, str, datetime, str, str]:
        return (
            str(self.uuid),
            self.file_name,
            self.file_hash,
            self.upload_time,
            self.email,
            self.tenant_id,
        )

    @classmethod
    def from_tuple(cls, data: Tuple[UUID, str, str, datetime, str, str]) -> "FileUploadDTO":
        return cls(
            uuid=data[0],
            file_name=data[1],
            file_hash=data[2],
            upload_time=data[3],
            email=data[4],
            tenant_id=data[5],
        )

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FileUploadDTO":
        return cls(**data)

    @staticmethod
    def from_file(
        file_name: str, file_content: bytes, email: EmailStr, tenant_id: str, upload_time=datetime.now()
    ) -> "FileUploadDTO":
        file_hash = FileUploadDTO.calculate_hash(file_content)
        return FileUploadDTO(
            uuid=UUID(int=0),
            file_name=file_name,
            file_hash=file_hash,
            upload_time=upload_time or datetime.now(),
            email=email,
            tenant_id=tenant_id,
        )

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
