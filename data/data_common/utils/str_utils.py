import re
import uuid
from common.genie_logger import logger

ALLOWED_EXTENSIONS = {"pdf", "doc", "docx", "ppt", "pptx"} 
MAX_FILE_NAME_LENGTH = 64
ALLOWED_CHARS_PATTERN = r"^[a-zA-Z0-9\-_ ()]"  

SMALL_WORDS = {
    "and",
    "or",
    "as",
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

ABBREVIATIONS = {
    "ceo",
    "cfo",
    "coo",
    "cto",
    "cio",
    "cmo",
    "cpo",
    "chro",
    "cdo",
    "cso",
    "vp",
    "svp",
    "evp",
    "hr",
    "it",
    "pr",
    "r&d",
    "ux",
    "ui",
    "pm",
    "qa",
    "cob",
    "cro",
    "saas",
    "paas",
    "iaas",
    "api",
    "kpi",
    "okr",
    "roi",
    "mvp",
    "b2b",
    "b2c",
    "crm",
    "erp",
    "bi",
    "cagr",
    "ebitda",
    "gaap",
    "ipo",
    "rfp",
    "rfq",
    "rfid",
    "sla",
    "tco",
    "byod",
    "bom",
    "poc",
    "gtm",
    "cro",
}

def remove_non_alphanumeric_strings(strings):
    return [s for s in strings if any(c.isalnum() for c in s)]

def get_uuid4():
    new_uuid = uuid.uuid4()
    return str(new_uuid)


def titleize_name(name: str) -> str:
    if not name:
        return name
    words = name.split(" ")
    if not words:
        return name
    title_cased = []
    for i, word in enumerate(words):
        if "." in word:
            title_cased.append(word.upper())
        else:
            title_cased.append(word.capitalize())
    return " ".join(title_cased)


def to_custom_title_case(value) -> str:
    if isinstance(value, str):
        words = value.split()
        if len(words) == 0:
            return value
        # if len(words) == 1:
        #     if words[0].lower() in SMALL_WORDS:
        #         return words[0].lower()
        #     if len(words[0]) < 4 and words[0] not in SMALL_WORDS:
        #         logger.debug(f"Word: {words[0]}")
        #         return words[0].upper()
        #     return words[0].capitalize()
        # title_cased = [words[0].capitalize()]
        title_cased = []
        for word in words:
            if word.lower() in SMALL_WORDS:
                title_cased.append(word.lower())
            elif len(word) < 4 and word.lower() not in SMALL_WORDS:
                title_cased.append(word.upper())
            else:
                title_cased.append(word.capitalize())
        return " ".join(title_cased)
    if isinstance(value, list):
        return [to_custom_title_case(item) for item in value]
    if isinstance(value, dict):
        return {k: to_custom_title_case(v) for k, v in value.items()}
    return value


def titleize_sentence(sentence):
    if not isinstance(sentence, str) or not sentence:
        return sentence

    words = sentence.split()
    if not words:
        return sentence

    title_cased = []

    # Capitalize the first word of the sentence
    for i, word in enumerate(words):
        if i == 0:
            title_cased.append(word.capitalize())
        elif word.lower() in ABBREVIATIONS:
            title_cased.append(word.upper())
        else:
            title_cased.append(word.lower())

    return " ".join(title_cased)


def titleize_values(data):
    if isinstance(data, list):
        return [titleize_values(item) for item in data]
    if isinstance(data, dict):
        return {
            k: titleize_values(v) if isinstance(v, (dict, list)) else titleize_sentence(v)
            for k, v in data.items()
        }
    return titleize_sentence(data)

def upload_file_name_validation(file_name: str) -> bool:
    if "." not in file_name:
        return False
    
    base_name, extension = file_name.rsplit(".", 1)
    
    # Check if the extension is allowed
    if extension.lower() not in ALLOWED_EXTENSIONS:
        return False
    
    # Check if the base name length is within the allowed limit
    if len(base_name) > MAX_FILE_NAME_LENGTH:
        return False
    
    # Check if the base name contains only allowed characters
    if not re.match(ALLOWED_CHARS_PATTERN + r"{" + str(len(base_name)) + r"}$", base_name):
        return False
    
    return True