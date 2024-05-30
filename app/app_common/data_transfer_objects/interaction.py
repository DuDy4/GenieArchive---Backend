from dataclasses import dataclass, field
from typing import List
import json


@dataclass
class InteractionDTO:
    uuid: str
    userUuid: str
    userEmail: str
    interaction_source: str
    interaction_type: str
    company: str
    recipient_uuid: str
    recipient_email: str
    recipient_company: str
    content: str
    timestamp: int

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "userUuid": self.userUuid,
            "userEmail": self.userEmail,
            "interaction_source": self.interaction_source,
            "interaction_type": self.interaction_type,
            "company": self.company,
            "recipient_uuid": self.recipient_uuid,
            "recipient_email": self.recipient_email,
            "content": self.content,
            "timestamp": self.timestamp,
            "recipient_company": self.recipient_company,
        }

    @staticmethod
    def from_dict(data: dict):
        return InteractionDTO(
            uuid=data.get("uuid", ""),
            userUuid=data.get("userUuid", ""),
            userEmail=data.get("userEmail", ""),
            interaction_source=data.get("interaction_source", ""),
            interaction_type=data.get("interaction_type", ""),
            company=data.get("company", ""),
            recipient_uuid=data.get("recipient_uuid", ""),
            recipient_email=data.get("recipient_email", ""),
            content=data.get("content", ""),
            timestamp=data.get("timestamp", 0),
            recipient_company=data.get("recipient_company", ""),
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return InteractionDTO.from_dict(data)

    def to_tuple(self):
        return (
            self.uuid,
            self.userUuid,
            self.userEmail,
            self.interaction_source,
            self.interaction_type,
            self.company,
            self.recipient_uuid,
            self.recipient_email,
            self.recipient_company,
            self.content,
            self.timestamp,
        )
