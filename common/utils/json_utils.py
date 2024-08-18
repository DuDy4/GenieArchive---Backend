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
    

def clean_json(json_str):
    """
    Clean JSON string by replacing single quotes with double single quotes
    """
    json_data = json.loads(json_str)
    for item in json_data:
        item['title'] = item['title'].replace("'", "''")
        item['summary'] = item['summary'].replace("'", "''")

    clean_json_string = json.dumps(json_data)
    return clean_json_string

