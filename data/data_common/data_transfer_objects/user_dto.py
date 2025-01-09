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


class UserDTO:
    def __init__(
            self,
            uuid: str,
            user_id: str,
            name: str,
            email: str,
            tenant_id: str,
    ):
        self.uuid = uuid
        self.user_id = user_id
        self.name = name
        self.email = email
        self.tenant_id = tenant_id

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "user_id": self.user_id,
            "name": to_custom_title_case(self.name),
            "email": self.email,
            "tenant_id": self.tenant_id,
        }

    @staticmethod
    def from_dict(data: dict):
        return UserDTO(
            uuid=data.get("uuid", "") or get_uuid4(),
            user_id=data.get("user_id", ""),
            name=data.get("name", ""),
            email=data.get("email", ""),
            tenant_id=data.get("tenant_id", ""),
        )

    def to_tuple(self) -> tuple:
        return (
            self.uuid,
            self.user_id,
            self.name,
            self.email,
            self.tenant_id,
        )

    @staticmethod
    def from_tuple(data: tuple):
        return UserDTO(
            uuid=data[0],
            user_id=data[1],
            name=data[2],
            email=data[3],
            tenant_id=data[4],
        )