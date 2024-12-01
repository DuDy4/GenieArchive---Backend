import asyncio
import sys
import os
from typing import List

from data.data_common.events.genie_event import GenieEvent
from data.data_common.events.genie_event_batch_manager import EventHubBatchManager
from data.data_common.events.topics import Topic

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from common.genie_logger import GenieLogger

from data.data_common.dependencies.dependencies import (
    ownerships_repository,
    profiles_repository,
    tenant_profiles_repository,
    persons_repository
)

from data.internal_services.sales_action_items_service import SalesActionItemsService

logger = GenieLogger()

ownerships_repository = ownerships_repository()
profiles_repository = profiles_repository()
tenant_profiles_repository = tenant_profiles_repository()
persons_repository = persons_repository()

sales_service = SalesActionItemsService()

class Ownership:
    def __init__(self, tenant_id, profile_uuid):
        self.tenant_id = tenant_id
        self.profile_uuid = profile_uuid

def get_all_uuids_and_tenants_id_without_action_items(forced_refresh=False):
    # Should get [{tenant_id: str, profile_uuid: str}]
    all_uuids_and_tenants_id = tenant_profiles_repository.get_all_uuids_and_tenants_id_without_action_items(forced_refresh)
    # logger.info(f"Got all uuids and tenants_id without action items: {all_uuids_and_tenants_id}")
    return all_uuids_and_tenants_id

# find all profiles uuid -> tenant_id without action items
# we need:
# ownership repo
# profile repo
# tenant_profile repo


def create_new_base_profile_events(uuid: str, tenant_id: str):
    profile = profiles_repository.get_profile_data(uuid)
    profile_dict = profile.to_dict()
    personal_data = {
        "strengths": profile_dict.get("strengths"),
        "get_to_know": profile_dict.get("get_to_know"),
    }
    person = persons_repository.get_person(uuid)
    email_address = person.email

    data_to_send = {
        "profile": personal_data,
        "email": email_address,
        "tenant_id": tenant_id,
        "person": person.to_dict(),
        "force_refresh": True
    }

    event = GenieEvent(
        topic=Topic.NEW_BASE_PROFILE,
        data=data_to_send
    )
    return event


# create action items for each profile [:10]
def get_sales_criteria_for_profile(profile_uuid, tenant_id):
    sales_criteria = tenant_profiles_repository.get_sales_criteria(profile_uuid, tenant_id)
    if not sales_criteria:
        logger.warning(f"No sales criteria found for profile {profile_uuid} in tenant {tenant_id}")
        profile = profiles_repository.get_profile_data(profile_uuid)
        sales_criteria = profile.sales_criteria
        if not sales_criteria:
            logger.warning(f"No sales criteria found for profile {profile_uuid}")
            return None
        tenant_profiles_repository.update_sales_criteria(profile_uuid, tenant_id, sales_criteria)
    return sales_criteria


def create_general_action_items(ownership_relation: List[Ownership]):
    res = []
    for ownership in ownership_relation:
        try:
            sales_criteria = get_sales_criteria_for_profile(ownership.profile_uuid, ownership.tenant_id)
            logger.info(f"Sales criteria for {ownership.profile_uuid} in tenant {ownership.tenant_id}: {sales_criteria}")
            action_items = sales_service.get_action_items(sales_criteria)
            tenant_profiles_repository.update_sales_action_items(ownership.profile_uuid, ownership.tenant_id, action_items)
            res.append({
                "tenant_id": ownership.tenant_id,
                "profile_uuid": ownership.profile_uuid,
                "action_items": action_items
            })
        except Exception as e:
            logger.error(f"Error getting action items for {ownership.profile_uuid}: {e}")
    return res


def create_specific_action_items(ownership_relation: List[Ownership]):
    event_batch = EventHubBatchManager()
    events = [event_batch.queue_event(create_new_base_profile_events(ownership.profile_uuid, ownership.tenant_id)) for ownership in ownership_relation]
    logger.info(f"Events to send {len(events)}: {events}")
    if events:
        asyncio.run(event_batch.send_batch())
        return True
    return False


def sync_action_items(num_sync = 5, forced_refresh=False):
    uuids_and_tenant_ids = get_all_uuids_and_tenants_id_without_action_items(forced_refresh)
    ownerships_relation = [Ownership(object.get("tenant_id"), object.get("profile_uuid")) for object in uuids_and_tenant_ids]
    logger.info(f"Ownership relation: {len(ownerships_relation)}")
    ans = create_specific_action_items(ownerships_relation[:num_sync])
    logger.info(f"Action items created: {ans}")
    return ans


# if __name__ == "__main__":
#     uuids_and_tenant_ids = get_all_uuids_and_tenants_id_without_action_items(True)
#     ownerships_relation = [Ownership(object.get("tenant_id"), object.get("profile_uuid")) for object in uuids_and_tenant_ids]
#     logger.info(f"Ownership relation: {len(ownerships_relation)}")
    # for ownership in ownerships_relation[:10]:
    #     logger.info(f"Creating action items for {ownership.profile_uuid} in tenant {ownership.tenant_id}")
    # ans = create_specific_action_items(ownerships_relation)
    # logger.info(f"Action items created: {ans}")
