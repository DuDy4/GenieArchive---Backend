from pydantic import BaseModel, EmailStr
from uuid import UUID
from enum import Enum
from datetime import datetime
from typing import Tuple, Dict, Any

class DealCriteriaType(str, Enum):
    BUDGET = "BUDGET"
    TRUST = "TRUST"
    TECHNICAL_FIT = "TECHNICAL_FIT"
    BUSINESS_FIT = "BUSINESS_FIT"
    VALUE_PROPOSITION = "VALUE_PROPOSITION"
    INNOVATION = "INNOVATION"
    REPUTATION = "REPUTATION"
    LONG_TERM_PROFESSIONAL_ADVISOR = "LONG_TERM_PROFESSIONAL_ADVISOR"
    RESPONSIVENESS = "RESPONSIVENESS"

class DealCriteriaDTO(BaseModel):
    type: DealCriteriaType
    score: int = 0
    progress_score: int = 0


class DealDTO(BaseModel):
    deal_id: UUID
    name: str
    description: str
    criterias: list[DealCriteriaDTO]  
    tenant_id: str
    compani_id: str
    created_at: datetime
    last_updated: datetime

    def to_tuple(self) -> Tuple:
        return (
            str(self.deal_id),
            self.name,
            self.description,
            self.criterias,
            self.tenant_id,
            self.company_id,
            self.created_at,
            self.last_updated
        )
    
    @classmethod
    def from_tuple(cls, data: Tuple) -> 'DealDTO':
        return cls(
            deal_id=data[0],
            name=data[1],
            description=data[2],
            criterias=data[3],
            tenant_id=data[4],
            company_id=data[5],
            created_at=data[6],
            last_updated=data[7]
        )
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DealDTO':
        return cls(
            deal_id=data['deal_id'],
            name=data['name'],
            description=data['description'],
            criterias=data['criterias'],
            tenant_id=data['tenant_id'],
            company_id=data['company_id'],
            created_at=data['created_at'],
            last_updated=data['last_updated']
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'deal_id': self.deal_id,
            'name': self.name,
            'description': self.description,
            'criterias': self.criterias,
            'tenant_id': self.tenant_id,
            'company_id': self.company_id,
            'created_at': self.created_at,
            'last_updated': self.last_updated
        }


