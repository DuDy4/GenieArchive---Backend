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
FAKE_LINKEDIN_EMAIL_PREFIX = "linkedin-email-"
FAKE_LINKEDIN_EMAIL_SUFFIX = "@stam.com"

def create_fake_linkedin_email(linkedin_url: str) -> str:
        linkedin_id = linkedin_url.split("in/")[1].replace("/", "")
        return f"{FAKE_LINKEDIN_EMAIL_PREFIX}{linkedin_id}{FAKE_LINKEDIN_EMAIL_SUFFIX}"

def get_fake_email_linkedin_url(email: str) -> str:
    if FAKE_LINKEDIN_EMAIL_PREFIX in email:
        linkedin_id = email.split(FAKE_LINKEDIN_EMAIL_PREFIX)[1].split(FAKE_LINKEDIN_EMAIL_SUFFIX)[0]
        if linkedin_id:
            return "linkedin.com/in/" + linkedin_id
    return None


def extract_email_from_url(url: str) -> str:
    pattern = r"/([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)/"
    match = re.search(pattern, url)

    if match:
        return match.group(1)
    else:
        return None


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
        if "noreply" in email or "no-reply" in email:
            continue
        if "assistant." in email_domain:
            continue
        else:
            final_list.append(email)
    logger.info(f"Final list: {final_list}")
    return final_list

def filter_emails_with_additional_domains(host_email: str, participants_emails: List, additional_domains: List):
    filtered_emails = filter_emails(host_email, participants_emails)
    for domain in additional_domains:
        if isinstance(domain, list):
            domain = domain[0]
        host_email = f"host@{domain}"
        logger.info(f"Filtering with host email: {host_email}")

        # Filter emails for the current domain
        additional_filtered_participants_emails = filter_emails(
            host_email=host_email, participants_emails=participants_emails
        )
        logger.info(f"Additional filtered participants emails: {additional_filtered_participants_emails}")

        # Strict intersection of filtered participants
        filtered_emails = list(set(filtered_emails).intersection(set(additional_filtered_participants_emails)))
        logger.info(f"Filtered participants emails after intersection: {filtered_emails}")
    return filtered_emails

def is_genie_admin(email: str):
    return email and (email.lower().endswith("@genieai.ai") or email.lower().endswith("genietest6@gmail.com"))


def get_domain(email: str):
    return email.split("@")[1] if email else None
