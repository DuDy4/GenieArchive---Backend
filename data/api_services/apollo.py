import requests
from dotenv import load_dotenv
from common.genie_logger import GenieLogger
from data.data_common.data_transfer_objects.person_dto import PersonDTO
import os
import time

logger = GenieLogger()

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

    def _handle_rate_limit(self, response):
        """
        Handles the rate-limiting logic by checking the `Retry-After` header.
        If the header is present, waits for the specified time. If not, uses
        exponential backoff.
        """
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 1))  # Fallback to 1 second if not present
            logger.warning(f"Rate limited. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            return True
        return False

    def _send_request_with_retries(self, url, data, max_retries=5):
        """
        Sends a POST request with retry logic in case of 429 Too Many Requests.
        """
        retries = 0
        backoff_time = 1  # Start with a 1-second delay

        while retries < max_retries:
            response = requests.post(url, headers=self.headers, json=data)
            if response.status_code == 429:
                if self._handle_rate_limit(response):
                    retries += 1
                    backoff_time *= 2  # Exponentially increase backoff time
                continue
            else:
                return response
        logger.error(f"Max retries reached for URL: {url}")
        return None

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
            "reveal_personal_emails": "true",
            "reveal_phone_number": "false",
            "details": [details],
        }

        try:
            response = self._send_request_with_retries(url, data)
            if response is None:
                return None  # Max retries reached

            response.raise_for_status()  # Raise an error for bad responses
            result = response.json()
            result_status = result.get("status")
            if result_status == "success":
                logger.info(f"Got response from Apollo: {result_status}")
            else:
                logger.error(f"Failed to get Apollo personal data ({data}): {result}")
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
            logger.error(f"HTTP error occurred for domain({domain}): {http_err}")
        except Exception as err:
            logger.error(f"Other error occurred: {err}")
        return None
