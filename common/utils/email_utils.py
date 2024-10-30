import asyncio
import requests
import re

from typing import List
from pydantic import BaseModel
from common.genie_logger import GenieLogger
from common.utils import env_utils

logger = GenieLogger()

additional_domains = env_utils.get("ADDITIONAL_EMAIL_DOMAINS", "").split(",")


async def fetch_public_domains():
    response = requests.get(
        "https://gist.githubusercontent.com/ammarshah/f5c2624d767f91a7cbdc4e54db8dd0bf/raw/660fd949eba09c0b86574d9d3aa0f2137161fc7c/all_email_provider_domains.txt"
    )

    domain_list = response.text.split("\n")
    domain_dict = {}
    for domain in domain_list:
        domain_dict[domain] = True

    return domain_dict


PUBLIC_DOMAIN = asyncio.run(fetch_public_domains())
PUBLIC_DOMAIN["group.calendar.google.com"] = True


def extract_email_from_url(url: str) -> str:
    pattern = r"/([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)/"
    match = re.search(pattern, url)

    if match:
        return match.group(1)
    else:
        return None


def filter_email_objects(participants_emails) -> List:
    """
    Filter emails of:
    1. is the organizer.
    2. has the same domain as the organizer.
    3. has a public domain.
    """
    final_list = []

    host_email_list = [email.get("email") for email in participants_emails if email.get("self")]
    host_email = host_email_list[0] if host_email_list else None
    if not host_email:
        return final_list
    host_domain = host_email.split("@")[1]
    logger.info(f"Host email: {host_email}")
    for email in participants_emails:
        email_domain = email.get("email").split("@")[1]
        if email_domain == host_domain:
            continue
        elif email_domain in PUBLIC_DOMAIN:
            continue
        if "assistant." in email.get("email"):
            continue
        else:
            final_list.append(email)
    logger.info(f"Final list: {final_list}")
    return final_list


def filter_emails(host_email: str, participants_emails: List):
    """
    Filter emails of:
    1. is the organizer.
    2. has the same domain as the organizer.
    3. has a public domain.
    """
    final_list = []
    if not host_email:
        return final_list
    host_domain = host_email.split("@")[1]
    logger.info(f"Host email: {host_email}")
    for email_object in participants_emails:
        if isinstance(email_object, BaseModel):
            email = email_object.email_address
        elif isinstance(email_object, dict):
            email = email_object.get("email")
        else:
            email = email_object
        email_domain = email.split("@")[1]
        if email_domain == host_domain:
            continue
        elif email_domain in PUBLIC_DOMAIN:
            continue
        else:
            final_list.append(email)
    logger.info(f"Final list: {final_list}")
    return final_list


def is_genie_admin(email: str):
    return email and email.lower().endswith("@genieai.ai")


def get_domain(email: str):
    return email.split("@")[1] if email else None
