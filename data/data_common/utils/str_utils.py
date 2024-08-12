import uuid

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


def get_uuid4():
    new_uuid = uuid.uuid4()
    return str(new_uuid)


def to_custom_title_case(value) -> str:
    if isinstance(value, str):
        words = value.split()
        if len(words) == 0:
            return value
        if len(words) == 1:
            if words[0].lower() in SMALL_WORDS:
                return words[0].lower()
            if words[0].lower() in ABBREVIATIONS:
                return words[0].upper()
            return words[0].capitalize()
        title_cased = [words[0].capitalize()]
        for word in words[1:-1]:
            if word.lower() in SMALL_WORDS:
                title_cased.append(word.lower())
            elif word.lower() in ABBREVIATIONS:
                title_cased.append(word.upper())
            else:
                title_cased.append(word.capitalize())
        if len(words) > 1:
            last_word = words[-1]
            if last_word.lower() in ABBREVIATIONS:
                title_cased.append(last_word.upper())
            else:
                title_cased.append(last_word.capitalize())
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


# # Test the functions
# test_string = "the ceo and cto of the company"
# print(to_custom_title_case(test_string))  # Output: "The CEO and CTO of the Company"
# print(titleize_values(test_string))  # Output: "The CEO and cto of the company"
