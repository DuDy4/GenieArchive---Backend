from data.data_common.repositories.tenants_repository import TenantsRepository
from data.data_common.repositories.ownerships_repository import OwnershipsRepository
from data.data_common.repositories.meetings_repository import MeetingsRepository
from data.data_common.dependencies.dependencies import (get_db_connection, tenants_repository, meetings_repository,
                                                        ownerships_repository)
from common.genie_logger import GenieLogger

logger = GenieLogger()


class TenantService:
    def __init__(self, tenants_repository: TenantsRepository):
        self.tenants_repository = tenants_repository


    def changed_old_tenant_to_new_tenant(new_tenant: str, old_tenant: str):
        if not ownerships_repository.update_tenant_id(new_tenant, old_tenant):
            return False
        logger.info(f"OWNERSHIPS - Updated tenant_id from {old_tenant} to {new_tenant}")
        if not meetings_repository.update_tenant_id(new_tenant, old_tenant):
            return False
        logger.info(f"MEETINGS - Updated tenant_id from {old_tenant} to {new_tenant}")
        if not tenants_repository.update_tenant_id(new_tenant, old_tenant):
            return False
        logger.info(f"TENANTS - Updated tenant_id from {old_tenant} to {new_tenant}")
        return True