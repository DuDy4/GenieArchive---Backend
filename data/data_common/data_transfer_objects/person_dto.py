from dataclasses import dataclass, field
import json
from common.utils.str_utils import get_uuid4


@dataclass
class PersonDTO:
    uuid: str
    name: str
    company: str
    email: str
    linkedin: str
    position: str
    timezone: str

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "name": self.name,
            "company": self.company,
            "email": self.email,
            "linkedin": self.linkedin,
            "position": self.position,
            "timezone": self.timezone,
        }

    @staticmethod
    def from_dict(data: dict):
        return PersonDTO(
            uuid=data.get("uuid", get_uuid4()),
            name=data.get("name", ""),
            company=data.get("company", ""),
            email=data.get("email", ""),
            linkedin=data.get("linkedin", ""),
            position=data.get("position", ""),
            timezone=data.get("timezone", ""),
        )

    def to_tuple(self) -> tuple[str, str, str, str, str, str, str]:
        return (
            self.uuid,
            self.name,
            self.company,
            self.email,
            self.linkedin,
            self.position,
            self.timezone,
        )

    @staticmethod
    def from_tuple(data: tuple[str, str, str, str, str, str, str]):
        return PersonDTO(
            uuid=data[0],
            name=data[1],
            company=data[2],
            email=data[3],
            linkedin=data[4],
            position=data[5],
            timezone=data[6],
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
            uuid=get_uuid4(),
            name=f"{contact.get('FirstName')} {contact.get('LastName')}",
            company=f"{contact.get('AccountName') or (contact.get('Account', {}).get('Name', '') if contact.get('Account') else '')}",
            email=contact.get("Email") or "",
            linkedin=contact.get("LinkedInUrl__c") or "",
            position=contact.get("Title") or "",
            timezone="",
        )
