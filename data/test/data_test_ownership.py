import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.repositories.ownerships_repository import OwnershipsRepository
from data.data_common.dependencies.dependencies import (
    ownerships_repository,
    get_db_connection,
)


def test_ownership():
    conn = get_db_connection()
    ownerships_repository = OwnershipsRepository(conn=conn)
    uuid = "ThisIsATest"
    tenant_id = "TestOwner"
    if not ownerships_repository.exists(uuid, tenant_id):
        ownerships_repository.insert(uuid, tenant_id)
    assert ownerships_repository.exists(uuid, tenant_id)
    print("Ownership test passed")


test_ownership()
