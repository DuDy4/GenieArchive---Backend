import json


def extract_object_uuid(data) -> str:
    data = json.loads(data)
    if isinstance(data, str):
        data = json.loads(data)
    if data.get("object_uuid"):
        return data.get("object_uuid")
    if data.get("person_uuid"):
        return data.get("person_uuid")
    if data.get("person_id"):
        return data.get("person_id")
    if data.get("profile_uuid"):
        return data.get("profile_uuid")
    if data.get("uuid"):
        return data.get("uuid")
    if data.get("person"):
        return data.get("person").get("uuid")
    if data.get("profile"):
        return data.get("profile").get("uuid")
    if data.get("meeting_uuid"):
        return data.get("meeting_uuid")
    if data.get("meeting"):
        return data.get("meeting").get("uuid")
    if data.get("company_uuid"):
        return data.get("company_uuid")
    if data.get("company"):
        return data.get("company").get("uuid")