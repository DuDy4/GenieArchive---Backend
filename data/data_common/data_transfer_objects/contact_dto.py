from dataclasses import dataclass, field
import json
from common.utils.str_utils import get_uuid4


@dataclass
class ContactDTO:
    uuid: str
    tenant_id: str
    salesforce_id: str
    name: str
    company: str
    email: str
    linkedin: str
    position: str
    timezone: str

    def to_dict(self):
        return {
            "uuid": self.uuid,
            "tenant_id": self.tenant_id,
            "salesforce_id": self.salesforce_id,
            "name": self.name,
            "company": self.company,
            "email": self.email,
            "linkedin": self.linkedin,
            "position": self.position,
            "timezone": self.timezone,
        }

    @staticmethod
    def from_dict(data: dict):
        return ContactDTO(
            uuid=data.get("uuid", get_uuid4()),
            tenant_id=data.get("tenant_id", ""),
            salesforce_id=data.get("salesforce_id", ""),
            name=data.get("name", ""),
            company=data.get("company", ""),
            email=data.get("email", ""),
            linkedin=data.get("linkedin", ""),
            position=data.get("position", ""),
            timezone=data.get("timezone", ""),
        )

    def to_tuple(self) -> tuple[str, str, str, str, str, str, str, str, str]:
        return (
            self.uuid,
            self.tenant_id,
            self.salesforce_id,
            self.name,
            self.company,
            self.email,
            self.linkedin,
            self.position,
            self.timezone,
        )

    def from_tuple(data: tuple[str, str, str, str, str, str, str, str, str]):
        return ContactDTO(
            uuid=data[0],
            tenant_id=data[1],
            salesforce_id=data[2],
            name=data[3],
            company=data[4],
            email=data[5],
            linkedin=data[6],
            position=data[7],
            timezone=data[8],
        )

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return ContactDTO.from_dict(data)

    @staticmethod
    def from_sf_contact(contact: dict, tenant_id: str):
        return ContactDTO(
            uuid=get_uuid4(),
            tenant_id=tenant_id,
            salesforce_id=contact.get("Id"),
            name=f"{contact.get('FirstName')} {contact.get('LastName')}",
            company=f"{contact.get('AccountName') or (contact.get('Account', {}).get('Name', '') if contact.get('Account') else '')}",
            email=contact.get("Email") or "",
            linkedin=contact.get("LinkedInUrl__c") or "",
            position=contact.get("Title") or "",
            timezone="",
        )
