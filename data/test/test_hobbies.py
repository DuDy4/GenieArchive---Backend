from loguru import logger
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.data_common.repositories.hobbies_repository import HobbiesRepository
from data.data_common.dependencies.dependencies import (
    hobbies_repository,
    get_db_connection,
)

hobby1 = {
    "uuid": "a79d4460-8d40-4f30-88a1-b7d0eb3ff2ed",
    "hobby_name": "Hiking",
    "icon_url": "https://img.icons8.com/color/48/trekking.png",
}

hobby2 = {
    "uuid": "3ea1cfbc-d2cd-43a8-841a-818e9ea779c5",
    "hobby_name": "Painting",
    "icon_url": "https://img.icons8.com/fluency/48/easel.png",
}


def test_hobbies():
    conn = get_db_connection()
    hobbies_repository = HobbiesRepository(conn=conn)
    if not hobbies_repository.exists(hobby1["uuid"]):
        hobbies_repository.insert(hobby1)
    if not hobbies_repository.exists(hobby2["uuid"]):
        hobbies_repository.insert(hobby2)
    assert hobbies_repository.exists(hobby1["uuid"])
    assert hobbies_repository.exists(hobby2["uuid"])
    logger.info("Hobbies test passed")


test_hobbies()
