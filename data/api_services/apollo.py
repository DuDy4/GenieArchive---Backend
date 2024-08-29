import requests
from dotenv import load_dotenv
from common.genie_logger import GenieLogger
from data.data_common.data_transfer_objects.person_dto import PersonDTO

logger = GenieLogger()
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
        self.headers = {
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "X-Api-Key": self.api_key,
        }

    def enrich_person(self, person: PersonDTO):
        """
        Enrich person information by sending emails to the Apollo API.

        :param email: The email of the person to fetch data for.
        :return: The API response as a dictionary.
        """
        url = f"{self.base_url}/people/bulk_match"

        details = {"email": person.email}
        if person.linkedin:
            details["linkedin_url"] = person.linkedin
        data = {
            "reveal_personal_emails": True,
            "reveal_phone_number": False,
            "details": [details],
        }

        try:
            logger.debug(f"Sending request to Apollo: {data}")
            response = requests.post(url, headers=self.headers, json=data)
            response.raise_for_status()  # Raise an error for bad responses
            result = response.json()
            result_status = result.get("status")
            if result_status == "success":
                logger.info(f"Got response from Apollo: {result_status}")
            else:
                logger.error(f"Failed to get Apollo personal data: {result}")
            if result.get("matches"):
                matches = result.get("matches")
                logger.info(f"Got {len(matches)} matches: {str(matches)[:100]}")
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

    async def enrich_company(self, domain):
        """
        Get company data by domain using Apollo API.

        :param domain: The domain of the company to fetch data for.
        :return: The API response as a dictionary.
        """
        url = f"{self.base_url}/organizations/enrich?domain={domain}"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()  # Raise an error for bad responses
            result = response.json()
            logger.info(f"Got company data for domain {domain}: {result}")
            return result
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
        except Exception as err:
            logger.error(f"Other error occurred: {err}")
        return None
