import asyncio
import os
import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

api_key = os.environ.get("HUNTER_API_KEY")


def get_domain_from_email(email: str) -> str:
    return email.split("@")[1]


async def get_domain_info(email_address: str):
    """
    Get domain information from Hunter API
    Args:
        email_address (str): The email address to get domain information for.
    Returns:
        dict: The domain information.
    """
    domain = get_domain_from_email(email_address)

    logger.info(f"Domain: {domain}, API Key: {api_key}")

    response = requests.get(
        f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={api_key}"
    )

    logger.info(f"Response: {response}")
    data = response.json()
    logger.info(f"Data: {data}")

    return data


asyncio.run(get_domain_info("announcements@figma.com"))
