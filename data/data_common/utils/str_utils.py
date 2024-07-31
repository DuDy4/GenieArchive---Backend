import uuid
import inflection

SMALL_WORDS = {
    "and",
    "or",
    "the",
    "a",
    "an",
    "but",
    "nor",
    "for",
    "so",
    "yet",
    "at",
    "by",
    "from",
    "of",
    "on",
    "to",
    "with",
    "is",
    "if",
}


def to_custom_title_case(value):
    if isinstance(value, str):
        words = value.split()
        title_cased = [words[0].capitalize()]
        for word in words[1:-1]:
            if word in SMALL_WORDS:
                title_cased.append(word)
            else:
                title_cased.append(word.capitalize())
        if len(words) > 1:
            title_cased.append(words[-1].capitalize())
        return " ".join(title_cased)
    if isinstance(value, list):
        return [to_custom_title_case(item) for item in value]
    if isinstance(value, dict):
        return {k: to_custom_title_case(v) for k, v in value.items()}
    return value


def titleize_values(data):
    if isinstance(data, list):
        return [titleize_values(item) for item in data]
    if isinstance(data, dict):
        return {
            k: to_custom_title_case(v) if isinstance(v, str) else titleize_values(v)
            for k, v in data.items()
        }
    return data


def get_uuid4():
    new_uuid = uuid.uuid4()
    return str(new_uuid)
