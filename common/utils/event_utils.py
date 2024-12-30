import json


def extract_object_id(data) -> (str, str):
    data = json.loads(data)
    if isinstance(data, str):
        data = json.loads(data)
    if data.get("object_id"):
        return data.get("object_id"), "UNKNOWN"
    if data.get("person_uuid"):
        return data.get("person_uuid"), "PERSON"
    if data.get("person_id"):
        return data.get("person_id"), "PERSON"
    if data.get("profile_uuid"):
        return data.get("profile_uuid"), "PROFILE"
    if data.get("uuid"):
        return data.get("uuid"), "PERSON"
    if data.get("person"):
        return data.get("person").get("uuid"), "PERSON"
    if data.get("profile"):
        return data.get("profile").get("uuid"), "PROFILE"
    if data.get("meeting_uuid"):
        return data.get("meeting_uuid"), "MEETING"
    if data.get("meeting"):
        return data.get("meeting").get("uuid"), "MEETING"
    if data.get("company_uuid"):
        return data.get("company_uuid"), "COMPANY"
    if data.get("company"):
        return data.get("company").get("uuid"), "COMPANY"
    if data.get("email"):
        return data.get("email"), "EMAIL"
    return "", "UNKNOWN"