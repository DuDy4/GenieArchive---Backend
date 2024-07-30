import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.repositories.tenants_repository import TenantsRepository
from data.data_common.dependencies.dependencies import (
    get_db_connection,
)


def test_tenants():
    conn = get_db_connection()
    tenants_repository = TenantsRepository(conn=conn)
    tenant = {
        "uuid": "65b5afe8",
        "tenantId": "TestOwner",
        "name": "Dan Shevel",
        "email": "dan.shevel@genie.ai",
    }
    if not tenants_repository.exists(tenant.get("tenantId"), tenant.get("name")):
        tenants_repository.insert(tenant)
    assert tenants_repository.exists(tenant.get("tenantId"), tenant.get("name"))
    print("Tenants test passed")


test_tenants()
