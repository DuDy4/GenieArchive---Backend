from typing import Dict, Tuple
from pydantic import BaseModel



class ContactDTO(BaseModel):
    id: str
    name: str
    email: str
    owner_email: str
    salesforce_user_id: str

    def to_tuple(self) -> Tuple:
        return (
            self.id,
            self.name,
            self.email,
            self.owner_email,
            self.salesforce_user_id
        )

    @classmethod
    def from_tuple(cls, data: Tuple) -> 'ContactDTO':
        return cls(
            id=data[0],
            name=data[1],
            email=data[2],
            owner_email=data[3],
            salesforce_user_id=data[4]
        )

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'ContactDTO':
        return cls(
            id=data['id'],
            name=data['name'],
            email=data['email'],
            owner_email=data['owner_email'],
            salesforce_user_id=data.get('salesforce_user_id', None)
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            'id': self.id,
            'name': self.name,
            'email': self.email,
            'owner_email': self.owner_email,
            'salesforce_user_id': self.salesforce_user_id
        }
