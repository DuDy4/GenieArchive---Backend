from data.data_common.data_transfer_objects.profile_dto import SalesCriteria
from pydantic import BaseModel, EmailStr
from uuid import UUID
from enum import Enum
from datetime import datetime
from typing import Tuple, Dict, Any


class DealStatus(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    CLOSED = "closed"
    SUCCESSFUL = "successful"

    @staticmethod
    def from_str(label: str) -> 'DealStatus':
        label = label.lower()
        if label in ("active", "inactive", "closed", "successful"):
            return DealStatus[label.upper()]
        else:
            raise ValueError(f"Invalid classification: {label}")


class DealDTO(BaseModel):
    uuid: UUID
    name: str
    description: str
    criterias: list[SalesCriteria]  
    tenant_id: str
    company_id: str
    status: DealStatus = "active"

    def to_tuple(self) -> Tuple:
        return (
            str(self.uuid),
            self.name,
            self.description,
            self.criterias,
            self.tenant_id,
            self.company_id,
            self.status.value
        )
    
    @classmethod
    def from_tuple(cls, data: Tuple) -> 'DealDTO':
        return cls(
            uuid=data[0],
            name=data[1],
            description=data[2],
            criterias=data[3],
            tenant_id=data[4],
            company_id=data[5],
            status=DealStatus.from_str(data[7])
        )
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DealDTO':
        return cls(
            uuid=data['uuid'],
            name=data['name'],
            description=data['description'],
            criterias=data['criterias'],
            tenant_id=data['tenant_id'],
            company_id=data['company_id'],
            status=DealStatus.from_str(data.get('status', 'active'))
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'uuid': self.uuid,
            'name': self.name,
            'description': self.description,
            'criterias': self.criterias,
            'tenant_id': self.tenant_id,
            'company_id': self.company_id,
            'status': self.status.value
        }


