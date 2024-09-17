from data.data_common.repositories.tenants_repository import TenantsRepository
from data.data_common.repositories.ownerships_repository import OwnershipsRepository
from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.dependencies.dependencies import (
    tenants_repository,
    meetings_repository,
    ownerships_repository,
)
from common.genie_logger import GenieLogger

logger = GenieLogger()


class TenantService:
    tenants_repository: TenantsRepository = tenants_repository()
    ownerships_repository: OwnershipsRepository = ownerships_repository()
    meetings_repository: MeetingsRepository = meetings_repository()

    @staticmethod
    def changed_old_tenant_to_new_tenant(
        new_tenant_id: str, old_tenant_id: str, user_id: str, user_name: str
    ) -> bool:

        if not TenantService.ownerships_repository.update_tenant_id(new_tenant_id, old_tenant_id):
            return False
        logger.info(f"OWNERSHIPS - Updated tenant_id from {old_tenant_id} to {new_tenant_id}")
        if not TenantService.meetings_repository.update_tenant_id(new_tenant_id, old_tenant_id):
            return False
        logger.info(f"MEETINGS - Updated tenant_id from {old_tenant_id} to {new_tenant_id}")
        if not TenantService.tenants_repository.update_tenant_id(
            old_tenant_id=old_tenant_id, new_tenant_id=new_tenant_id, user_id=user_id, user_name=user_name
        ):
            return False
        logger.info(f"TENANTS - Updated tenant_id from {old_tenant_id} to {new_tenant_id}")
        return True
