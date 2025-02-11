import uuid
import re
from common.genie_logger import GenieLogger

logger = GenieLogger()


def get_uuid4():
    new_uuid = uuid.uuid4()
    return str(new_uuid)

def fix_linkedin_url(linkedin_url: str) -> str:
    """
    Converts a full LinkedIn URL to a shortened URL.

    Args:
        linkedin_url (str): The full LinkedIn URL.

    Returns:
        str: The shortened URL.
    """

    if not linkedin_url:
        logger.error(f"Trying to fix Linkedin URL, but it is None or empty: {linkedin_url}")
        return ""

    linkedin_url = linkedin_url.replace("http://www.linkedin.com/in/", "linkedin.com/in/")
    linkedin_url = linkedin_url.replace("https://www.linkedin.com/in/", "linkedin.com/in/")
    linkedin_url = linkedin_url.replace("http://linkedin.com/in/", "linkedin.com/in/")
    linkedin_url = linkedin_url.replace("https://linkedin.com/in/", "linkedin.com/in/")

    if linkedin_url and linkedin_url[-1] == "/":
        linkedin_url = linkedin_url[:-1:]
    return linkedin_url

def get_email_suffix(email: str) -> str:
    """
    Safely extracts the domain suffix from an email address.
    
    Args:
        email (str): The email address to extract the suffix from.
    
    Returns:
        str: The email suffix (domain) if valid, or an empty string if invalid.
    """
    # Regular expression for validating an email address
    email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    
    # Check if the email is valid
    if not re.match(email_regex, email):
        return ""
    
    # Split the email and get the suffix
    try:
        return email.split('@')[1].strip().lower()
    except (IndexError, AttributeError):
        return ""
