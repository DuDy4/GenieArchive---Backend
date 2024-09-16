import json
from typing import List, Dict, Optional, Union, Tuple, Any

from pydantic import HttpUrl, field_validator, BaseModel
from datetime import date

from data.data_common.utils.str_utils import (
    to_custom_title_case,
    get_uuid4,
)
from common.genie_logger import GenieLogger

logger = GenieLogger()


class TenantDTO:
    def __init__(
        self,
        uuid: str,
        name: str,
        tenant_id: str,
        email: str,
        user_id: str
    ):
        self.uuid = uuid
        self.name = name
        self.tenant_id = tenant_id
        self.email = email
        self.user_id = user_id

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "name": to_custom_title_case(self.name),
            "tenant_id": self.tenant_id,
            "email": self.email,
            "user_id": self.user_id
        }

    @staticmethod
    def from_dict(data: dict):
        return TenantDTO(
            uuid=data.get("uuid", ""),
            name=data.get("name", ""),
            tenant_id=data.get("tenant_id", ""),
            email=data.get("email", ""),
            user_id=data.get("user_id", ""),
        )


    def to_tuple(self) -> tuple:
        return (
            self.uuid,
            self.name,
            self.tenant_id,
            self.email,
            self.user_id,
        )

    @staticmethod
    def from_tuple(row: tuple) -> "TenantDTO":
        return TenantDTO(
            uuid=row[0],
            tenant_id=row[1],
            name=row[2],
            email=row[3],
            user_id=row[4],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return TenantDTO.from_dict(data)

    def __str__(self):
        return (
            f"TenantDTO(uuid={self.uuid},\n name={self.name},\n tenant_id={self.tenant_id},\n email={self.email},\n "
            f"user_id={self.user_id}"
        )
