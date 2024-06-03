import uuid


def get_uuid4():
    new_uuid = uuid.uuid4()
    return str(new_uuid)
