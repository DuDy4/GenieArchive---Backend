from dataclasses import dataclass, field
from typing import List
import json


@dataclass
class PersonDTO:
    uuid: str
    name: str
    company: str
    email: str
    position: str
    timezone: str
    challenges: List[str] = field(default_factory=list)
    strengths: List[str] = field(default_factory=list)

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "name": self.name,
            "company": self.company,
            "email": self.email,
            "position": self.position,
            "timezone": self.timezone,
            "challenges": self.challenges,
            "strengths": self.strengths,
        }

    @staticmethod
    def from_dict(data: dict):
        return PersonDTO(
            uuid=data.get("uuid", ""),
            name=data.get("name", ""),
            company=data.get("company", ""),
            email=data.get("email", ""),
            position=data.get("position", ""),
            timezone=data.get("timezone", ""),
            challenges=data.get("challenges", []),
            strengths=data.get("strengths", []),
        )

    def to_tuple(self) -> tuple[str, str, str, str, str, str, List[str], List[str]]:
        return (
            self.uuid,
            self.name,
            self.company,
            self.email,
            self.position,
            self.timezone,
            self.challenges,
            self.strengths,
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return PersonDTO.from_dict(data)

    @staticmethod
    def from_sf_contact(contact: dict):
        return PersonDTO(
            uuid=contact["Id"],
            name=f"{contact.get('FirstName')} {contact.get('LastName')}",
            company=f"{contact.get('AccountName') or contact.get('Account', {}).get('Name', '') if contact.get('Account') else ''}",
            email=contact["Email"],
            position=f"{contact.get('Title') or ''}",
            timezone="",
        )
