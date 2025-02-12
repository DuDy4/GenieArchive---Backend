from typing import Dict, Tuple, Optional
from pydantic import BaseModel



class SalesforceCredsDTO(BaseModel):
    salesforce_user_id: str
    salesforce_tenant_id: str
    instance_url: str
    access_token: str
    refresh_token: str
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None

    def to_tuple(self) -> Tuple:
        return (
            self.salesforce_user_id,
            self.salesforce_tenant_id,
            self.instance_url,
            self.access_token,
            self.refresh_token,
            self.user_id,
            self.tenant_id
        )

    @classmethod
    def from_tuple(cls, data: Tuple) -> 'SalesforceCredsDTO':
        return cls(
            salesforce_user_id=data[0],
            salesforce_tenant_id=data[1],
            instance_url=data[2],
            access_token=data[3],
            refresh_token=data[4],
            user_id=data[5],
            tenant_id=data[6]
        )

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> 'SalesforceCredsDTO':
        return cls(
            salesforce_user_id=data['salesforce_user_id'],
            salesforce_tenant_id=data['salesforce_tenant_id'],
            instance_url=data['instance_url'],
            access_token=data['access_token'],
            refresh_token=data['refresh_token'],
            user_id=data.get('user_id', None),
            tenant_id=data.get('tenant_id', None)
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            'salesforce_user_id': self.salesforce_user_id,
            'salesforce_tenant_id': self.salesforce_tenant_id,
            'instance_url': self.instance_url,
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'user_id': self.user_id,
            'tenant_id': self.tenant_id
        }

