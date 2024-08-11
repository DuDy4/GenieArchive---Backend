import logging

import requests
from loguru import logger
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

BASE_URL = os.getenv("APOLLO_BASE_URL")
API_KEY = os.getenv("APOLLO_API_KEY")


class ApolloClient:
    def __init__(self):
        # Get API key from environment variables
        self.api_key = API_KEY
        if not self.api_key:
            raise ValueError("API key is missing. Please set it in the .env file.")

        self.base_url = BASE_URL
        self.headers = {"Cache-Control": "no-cache", "Content-Type": "application/json", "X-Api-Key": self.api_key}

    def enrich_contact(self, emails):
        """
        Enrich contact information by sending emails to the Apollo API.

        :param emails: A list of email addresses to enrich.
        :return: The API response as a dictionary.
        """
        url = f"{self.base_url}/people/bulk_match"
        details = [{"email": email} for email in emails]
        data = {"reveal_personal_emails": True, "reveal_phone_number": True, "webhook_url": "https://your_webhook_site", "details": details}

        try:
            response = requests.post(url, headers=self.headers, json=data)
            response.raise_for_status()  # Raise an error for bad responses
            result = response.json()
            if result.get("matches"):
                matches = result.get("matches")
                logger.info(f"Got {len(matches)} matches")
                if isinstance(matches, list):
                    return matches[0]
                logger.warning("Matches came back in other form than a list")
            logger.error("Failed to get Apollo personal data")
            return result

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
        except Exception as err:
            logger.error(f"Other error occurred: {err}")

        return None
