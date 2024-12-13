import uuid
import re


def get_uuid4():
    new_uuid = uuid.uuid4()
    return str(new_uuid)


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
