import json


def json_to_python(json_str):
    try:
        python_obj = json.loads(json_str)
        return python_obj
    except json.JSONDecodeError:
        return None


def remove_empty_keys(json_str):
    try:
        python_obj = json.loads(json_str)
        if isinstance(python_obj, dict):
            python_obj = {
                k: v for k, v in python_obj.items() if v is not None and v != ""
            }
        return python_obj
    except json.JSONDecodeError:
        return None
