import requests
import os
from loguru import logger
from dotenv import load_dotenv

load_dotenv()
class LinkedInProfileFetcher:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://fresh-linkedin-profile-data.p.rapidapi.com/get-profile-posts"
        self.headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": "fresh-linkedin-profile-data.p.rapidapi.com"
        }

    def fetch_latest_posts(self, linkedin_url, num_posts=3):  # Default value of 3 posts
        logger.info(f"Fetching the latest {num_posts} posts from: {linkedin_url}")
        querystring = {"linkedin_url": linkedin_url, "type": "posts"}

        try:
            response = requests.get(self.base_url, headers=self.headers, params=querystring)
            response.raise_for_status()
            data = response.json()
            # Extract the latest `num_posts` posts
            latest_posts = data['data'][:num_posts]
            logger.success(f"Successfully fetched {len(latest_posts)} posts from {linkedin_url}")
            return latest_posts
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
        except Exception as err:
            logger.error(f"An error occurred: {err}")
        return []


# Usage example:
if __name__ == "__main__":
    # Fetch the API key from environment variables
    api_key = os.getenv("RAPID_API_KEY")  # Ensure the environment variable is set as 'RAPIDAPI_KEY'

    if not api_key:
        logger.error("API key not found in environment variables")
    else:
        linkedin_url = "https://www.linkedin.com/in/asaf-savich/"
        fetcher = LinkedInProfileFetcher(api_key)

        # Fetching the default 3 posts
        latest_posts = fetcher.fetch_latest_posts(linkedin_url)  # Defaults to 3 posts
        for post in latest_posts:
            logger.info(post)


