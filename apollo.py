import logging

import requests
from loguru import logger
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()


class ApolloClient:
    def __init__(self):
        # Get API key from environment variables
        self.api_key = os.getenv('APOLLO_API_TOKEN')
        if not self.api_key:
            raise ValueError("API key is missing. Please set it in the .env file.")

        self.base_url = "https://api.apollo.io/v1"
        self.headers = {
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/json',
            'X-Api-Key': self.api_key
        }

    def enrich_contact(self, emails):
        """
        Enrich contact information by sending emails to the Apollo API.

        :param emails: A list of email addresses to enrich.
        :return: The API response as a dictionary.
        """
        url = f"{self.base_url}/people/bulk_match"
        details = [{"email": email} for email in emails]
        data = {
            "reveal_personal_emails": True,
            "reveal_phone_number": True,
            "webhook_url": "https://your_webhook_site",
            "details": details
        }

        try:
            response = requests.post(url, headers=self.headers, json=data)
            response.raise_for_status()  # Raise an error for bad responses
            result = response.json()
            logger.info(f"Response received: {result}")
            return result

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
        except Exception as err:
            logger.error(f"Other error occurred: {err}")

        return None


# Example usage:
if __name__ == "__main__":
    apollo_client = ApolloClient()

    # Example emails to be sent
    emails = ["adi@genieai.ai"]

    response = apollo_client.enrich_contact(emails)

    if response:
        logger.info(f"API Response: {response}")
    else:
        logger.warning("No data found.")
