from enum import Enum
from typing import Optional, Dict

from data.data_common.data_transfer_objects.profile_category_dto import SalesCriteriaType
from pydantic import BaseModel, Field

class SalesActionItemStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"


class SalesActionItemCategory(str, Enum):
    GENERIC = "GENERIC"
    SEND_FILE = "SEND_FILE"



class SalesActionItem(BaseModel):
    criteria: SalesCriteriaType
    action_item: str
    detailed_action_item: Optional[str] = None
    status: SalesActionItemStatus = SalesActionItemStatus.PENDING
    score: int = Field(0, ge=0, le=100)
    category: SalesActionItemCategory = SalesActionItemCategory.GENERIC

    def to_dict(self) -> Dict[str, str | int]:
        return {
            "criteria": str(self.criteria.value),
            "action_item": str(self.action_item),
            "detailed_action_item": str(self.detailed_action_item) if self.detailed_action_item else "",
            "status": self.status.value,
            "score": int(self.score),
            "category": self.category.value,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, any]) -> "SalesActionItem":
        return SalesActionItem(
            criteria=SalesCriteriaType(data["criteria"]),
            action_item=data["action_item"],
            detailed_action_item=data["detailed_action_item"],
            status=SalesActionItemStatus(data["status"]),
            score=data["score"],
            category=SalesActionItemCategory(data["category"]) if "category" in data else SalesActionItemCategory.GENERIC,
        )

